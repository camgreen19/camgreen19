WITH max_changed_at AS (
    SELECT m."Patron ID"
        , m."Wager ID"
        , MAX("Settled Timestamp Toronto") AS "Settled Timestamp Toronto"
    FROM ca_finance_reports.used_free_wagers m
    GROUP BY 1,2
)

--MAX_WALLET_TRANSACTIONS
, max_row_wallet_transactions AS (
    SELECT DISTINCT id
                  , MAX(_changed_at) AS _changed_at
    FROM casino_db_history.wallet_transactions
    WHERE _changed_at < reports_end_time_utc_ca_on() + INTERVAL '2 MINUTES'
    GROUP BY 1
)
, wallet_transactions_temp AS (
    SELECT DISTINCT m.id,
                    m._changed_at,
                    MAX(updated_at) updated_at
    FROM casino_db_history.wallet_transactions  b
    INNER JOIN max_row_wallet_transactions m ON m.id = b.id AND m._changed_at = b._changed_at
    GROUP BY 1, 2
)
, max_wallet_transactions AS (
SELECT b.*
FROM casino_db_history.wallet_transactions  AS b
INNER JOIN wallet_transactions_temp AS m
ON b.id = m.id
AND b._changed_at = m._changed_at
AND b.updated_at = m.updated_at)

--MAX_WALLET_TRANSACTION_JACKPOTS
, max_row_wallet_transaction_jackpots AS (
    SELECT DISTINCT wallet_transaction_id
                  , jackpot_id
                  , MAX(_changed_at) AS _changed_at
    FROM casino_db_history.wallet_transaction_jackpots
    WHERE _changed_at < reports_end_time_utc_ca_on() + INTERVAL '2 MINUTES'
    GROUP BY 1,2
)
, max_wallet_transaction_jackpots_temp AS (
    SELECT DISTINCT m.wallet_transaction_id,
                    m.jackpot_id,
                    m._changed_at,
                    MAX(updated_at) updated_at
    FROM casino_db_history.wallet_transaction_jackpots  b
    INNER JOIN max_row_wallet_transaction_jackpots m ON m.wallet_transaction_id = b.wallet_transaction_id AND m.jackpot_id = b.jackpot_id AND m._changed_at = b._changed_at
    GROUP BY 1, 2, 3
)

, max_wallet_transaction_jackpots AS (
SELECT b.*
FROM casino_db_history.wallet_transaction_jackpots  AS b
INNER JOIN max_wallet_transaction_jackpots_temp AS m
ON m.wallet_transaction_id = b.wallet_transaction_id AND m.jackpot_id = b.jackpot_id
AND b._changed_at = m._changed_at
AND b.updated_at = m.updated_at
)

--MAX_JACKPOTS
, max_row_jackpots AS (
    SELECT DISTINCT id
                  , MAX(_changed_at) AS _changed_at
    FROM casino_db_history.jackpots
    WHERE _changed_at < reports_end_time_utc_ca_on()
    GROUP BY 1
)
, max_jackpots_temp AS (
    SELECT DISTINCT m.id,
                    m._changed_at,
                    MAX(updated_at) updated_at
    FROM casino_db_history.jackpots b
    INNER JOIN max_row_jackpots m ON m.id = b.id AND m._changed_at = b._changed_at
    GROUP BY 1, 2
)

, max_jackpots AS (
SELECT b.*
FROM casino_db_history.jackpots  AS b
INNER JOIN max_jackpots_temp AS m
ON b.id = m.id
AND b._changed_at = m._changed_at
AND b.updated_at = m.updated_at)

, JACKPOTS_TEST AS
    (SELECT DISTINCT j._changed_at
, j.wallet_transaction_id
--, t.identity_user_id AS patron_id
, j.type
, t.game_provider
, t.game_provider_game_round_id
, t.game_provider_transaction_id
, t.parent_wallet_transaction_id
, t.remote_wallet_transaction_id -- patron_transaction_id in edgebook
, t.game_session_id
, jp.id AS jackpot_id
, jp.display_name
, t.free_round_award_id
, (j.amount_units) + (j.amount_nanos*10^(-9)) AS amount_precision
, CASE WHEN j.type = 'win' THEN t.amount/100.00 ELSE 0.0 END AS wallet_amount -- includes jackpot win and regular payout
, j.inserted_at
, j.updated_at
, CASE WHEN (MAX(wallet_amount) - SUM(amount_precision) < 0.01 AND MAX(wallet_amount) - SUM(amount_precision) > -0.01)
    THEN 0.00
    ELSE MAX(wallet_amount) - SUM(amount_precision)
    END AS casino_payout_normal
, MAX(wallet_amount) - SUM(amount_precision) AS casino_payout_precise
FROM max_wallet_transactions t
JOIN max_wallet_transaction_jackpots j
    ON j.wallet_transaction_id = t.id
JOIN ca_on_reports.max_jackpots jp
    ON jp.id = j.jackpot_id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)

, free_rounds_payouts AS (
    SELECT w."Patron ID" AS patron_id
        , w."Wager ID" AS wager_id
        , w."Promo Free Wager ID" AS promo_free_wager_id
        , "Payout Amount" AS amount_dollars
        , w."Settled Timestamp Toronto" AS transaction_timestamp
    FROM ca_finance_reports.used_free_wagers w
    JOIN max_changed_at c
        ON w."Patron ID" = c."Patron ID"
        AND w."Wager ID" = c."Wager ID"
        AND w."Settled Timestamp Toronto" = c."Settled Timestamp Toronto"
    JOIN ca_finance_reports.max_identity_users u
        ON u.patron_id = w."Patron ID"
        AND is_tester = false
    WHERE "Outcome" != 'rollback'
    AND "Product" = 'Casino'
    AND "Payout Amount" != 0
    AND (
        w."Settled Timestamp Toronto" >= reports_start_time_et_ca_finance()
        AND w."Settled Timestamp Toronto" < reports_end_time_et_ca_finance()
    )
)

, game_metadata AS (
    SELECT f.*
        , REPLACE(json_extract_path_text(metadata , 'game_name'), '_R3', '') AS game_name
        , json_extract_path_text(metadata , 'game_category') AS game_category
        , json_extract_path_text(metadata , 'game_provider') AS game_provider
    FROM ca_finance_reports.max_casino_wagers_all c
    JOIN free_rounds_payouts f
        ON c.patron_transaction_id::VARCHAR = f.wager_id
        AND c.promo_engine_free_round_award_id::VARCHAR = f.promo_free_wager_id
)

-- WAGERS AND WINNINGS

, dr_fu_cash_casino_rollback AS (
    SELECT DISTINCT ledger_transaction_id
        , patron_transaction_id
        , a_patron_id AS patron_id
        , a_type AS type
        , debit_cents
        , t_changed_at AS updated_at
        , p_id AS remote_wallet_transaction_id
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS a
    INNER JOIN ca_finance_reports.max_identity_users_in_state u
        ON a.p_patron_id=u.patron_id
    WHERE p_type in ('casino_rollback')
    AND a_type='casino_house_wins'
    AND gaming_state='CA-ON'
    AND debit_cents>0
    AND u.is_tester IS FALSE
)

, cr_cl_cash_casino_rollback AS (
    SELECT DISTINCT ledger_transaction_id
        , patron_transaction_id
        , a_patron_id AS patron_id
        , a_type AS type
        , credit_cents
        , t_changed_at AS updated_at
        , p_id AS remote_wallet_transaction_id
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS a
    INNER JOIN ca_finance_reports.max_identity_users_in_state u
        ON u.patron_id=a.p_patron_id
    WHERE p_type in ('casino_rollback')
    AND a_type in ('customer_liability')
    AND gaming_state = 'CA-ON'
    AND credit_cents>0
    AND u.is_tester IS FALSE
)

, casino_rollback AS (
    SELECT DATE_TRUNC('day',d.updated_at AT time zone 'UTC' AT time zone 'America/Toronto') AS transaction_timestamp
        , g.name AS game_name
        , g.game_provider AS game_provider
        , g.category AS game_category
        , SUM(debit_cents/100.00) AS total_rollback
    FROM dr_fu_cash_casino_rollback d
    INNER JOIN cr_cl_cash_casino_rollback c
        ON c.ledger_transaction_id = d.ledger_transaction_id
        AND debit_cents = credit_cents
    JOIN ca_finance_reports.max_rollback_payout_info w
        ON w.patron_transaction_id = d.patron_transaction_id
    JOIN ca_finance_reports.max_games g
        ON g.id::VARCHAR = w.game_id
    AND d.updated_at >= reports_start_time_utc_ca_finance()
    AND d.updated_at < reports_end_time_utc_ca_finance()
    GROUP BY 1,2,3,4
)

-- CASINO WAGERS
--modify original to be at transaction level to join to jackpot contributions
, cash_rounds_wagered_pre AS (
    SELECT DISTINCT SUM(w.amount_cents/100.00) AS amount
        , pt._changed_at AS transaction_timestamp
        , w.patron_transaction_id
        , json_extract_path_text(metadata , 'game_name') AS game_name
        , json_extract_path_text(metadata , 'game_provider') AS game_provider
        , json_extract_path_text(metadata , 'game_category') AS game_category
    FROM ca_finance_reports.max_casino_wagers_all AS w
    JOIN ca_finance_reports.max_patron_transactions_all pt
        ON pt.id=w.patron_transaction_id
    INNER JOIN ca_finance_reports.max_identity_users_in_state u
        ON u.patron_id=pt.patron_id
    WHERE w.promo_engine_free_round_award_id IS NULL
    AND amount_cents !=0
    AND u.is_tester IS FALSE
    AND w.gaming_state = 'CA-ON'
    AND pt._changed_at >= reports_start_time_utc_ca_finance()
    AND pt._changed_at < reports_end_time_utc_ca_finance()
    GROUP BY 2,3,4,5,6
)

--cash wagers
, cash_rounds_wagered AS (
    SELECT
        SUM(amount) AS amount
        , DATE_TRUNC('day', transaction_timestamp AT time zone 'utc' AT time zone 'America/Toronto') AS transaction_timestamp
        , game_name
        , game_provider
        , game_category
    FROM cash_rounds_wagered_pre
    GROUP BY 2,3,4,5
)

--jackpot contributions to be added into winnings - join to cash_rounds_wagered_pre to get game metadata and round by day/game
, jackpot_contributions AS (
    SELECT
        SUM(j.amount_precision) AS transfer_from_casino
        ,  w.transaction_timestamp AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , w.game_name
        , w.game_provider
        , w.game_category
    FROM cash_rounds_wagered_pre w
    JOIN JACKPOTS_TEST j
        ON w.patron_transaction_id::VARCHAR = j.remote_wallet_transaction_id
        AND j.type = 'contribution'
    GROUP BY 2,3,4,5
)

-- CASINO CASH PAYOUT
--modify original to be at transaction level to join to jackpot payouts
, transfer_from_casino_pre AS (
     SELECT pt._changed_at AS transaction_timestamp
        , g.patron_transaction_id
        , m.name AS game_name
        , m.game_provider
        , m.category AS game_category
        , SUM(g.amount_cents/100.00) AS transfer_from_casino
    FROM ca_finance_reports.max_wager_and_game_info g
    JOIN ca_finance_reports.max_patron_transactions_all pt
        ON g.patron_transaction_id = pt.id
    JOIN ca_finance_reports.max_casino_payouts_all p
        ON g.patron_transaction_id::VARCHAR = p.patron_transaction_id::VARCHAR
    JOIN ca_finance_reports.max_games m
        ON m.id::VARCHAR = g.game_id
    JOIN ca_finance_reports.max_identity_users u
        ON g.patron_id = u.patron_id
    WHERE g.promo_engine_free_round_award_id IS NULL
    AND p.gaming_state = 'CA-ON'
    AND u.is_tester = false
    AND (
        pt._changed_at >= reports_start_time_utc_ca_finance()
        AND pt._changed_at < reports_end_time_utc_ca_finance()
    )
    GROUP BY 1,2,3,4,5
)

--cash payouts
, transfer_from_casino AS (
    SELECT
        DATE_TRUNC('day', transaction_timestamp AT time zone 'utc' AT time zone 'America/Toronto') AS transaction_timestamp
        , game_name
        , game_provider
        , game_category
        , SUM(transfer_from_casino) AS transfer_from_casino
    FROM transfer_from_casino_pre
    GROUP BY 1,2,3,4
)

-- jackpot payout for cash round to be backed out from winnings
, jackpot_payout_cash AS (
    SELECT
         t.transaction_timestamp AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , t.game_name
        , t.game_provider
        , t.game_category
        , SUM(j.amount_precision) AS transfer_from_casino
    FROM transfer_from_casino_pre t
    JOIN JACKPOTS_TEST j
        ON t.patron_transaction_id::VARCHAR = j.remote_wallet_transaction_id
        AND j.free_round_award_id IS NULL
       WHERE j.type = 'win'
       GROUP BY 1,2,3,4

)

, jackpot_net_adjustments AS (
SELECT DATE_TRUNC('day', transaction_timestamp) AS gaming_day
, game_name
, game_provider
, game_category
, SUM(transfer_from_casino) AS transfer_from_casino
FROM jackpot_contributions
GROUP BY 1,2,3,4

UNION ALL

SELECT DATE_TRUNC('day', transaction_timestamp) AS gaming_day
, game_name
, game_provider
, game_category
, SUM(transfer_from_casino)*(-1) AS transfer_from_casino --subtract jackpot win
FROM jackpot_payout_cash
GROUP BY 1,2,3,4

)

, dataset AS (
    SELECT DATE_TRUNC('day', transaction_timestamp) AS gaming_day
        , 'Casino' AS product_code
        , game_provider
        , game_category
        , game_name
        , 0.00 AS wagers
        , 0.00 AS winnings
        , SUM(amount_dollars) AS free_bet_winnings
    FROM game_metadata
    GROUP BY 1,3,4,5

    UNION ALL
    -- cash rounds wagered
    SELECT transaction_timestamp AS gaming_day
        , 'Casino' AS product_code
        , game_provider
        , game_category
        , game_name
        , SUM(amount) AS wagers
        , 0.00 AS winnings
        , 0.00 AS free_bet_winnings
    FROM cash_rounds_wagered
    GROUP BY 1,3,4,5

    UNION ALL
    -- casino rollbacks
    SELECT transaction_timestamp AS gaming_day
        , 'Casino' AS product_code
        , game_provider
        , game_category
        , game_name
        , SUM(total_rollback) * -1.0 AS wagers
        , 0.00 AS winnings
        , 0.00 AS free_bet_winnings
    FROM casino_rollback
    GROUP BY 1,3,4,5

    UNION ALL
    -- transfers from casino
    SELECT transaction_timestamp AS gaming_day
        , 'Casino' AS product_code
        , game_provider
        , game_category
        , game_name
        , 0.00 AS wagers
        , SUM(transfer_from_casino) AS winnings
        , 0.00 AS free_bet_winnings
    FROM transfer_from_casino
    GROUP BY 1,3,4,5

     UNION ALL
    --add jackpot contributions/jackpot payout accounting to winnings
    SELECT gaming_day
        , 'Casino' AS product_code
        , game_provider
        , game_category
        , game_name
        , 0.00 AS wagers
        , SUM(transfer_from_casino)::NUMERIC(38,2) AS winnings
        , 0.00 AS free_bet_winnings
    FROM jackpot_net_adjustments
    GROUP BY 1,3,4,5
)

, summed_dataset AS (
    SELECT gaming_day
        , product_code
        , game_provider
        , game_category
        , REPLACE(game_name,'_R3', '') AS game_name
        , SUM(wagers) AS wagers
        , SUM(winnings) AS winnings
        , SUM(free_bet_winnings) AS free_bet_winnings
    FROM dataset
    GROUP BY 1,2,3,4,5
)

, igo_generated_id AS (
    SELECT 'OP100001'::VARCHAR AS igo_operator_id
        , DATE_TRUNC('day',reports_start_time_et_ca_finance()) AS period_start -- start timestamp
        , DATE_TRUNC('day',reports_end_time_et_ca_finance()) AS period_end -- end timestamp
        , 'S100001A' AS gaming_site_id
        , '1' as file_version
)

, igo_gen_id AS (
    SELECT *
        , 'GGR_' || g.igo_operator_id || '_' || g.transaction_id || '_' || to_char(g.period_start
        , 'YYYYMMDD')::VARCHAR || '_' || to_char(g.period_end, 'YYYYMMDD')::VARCHAR || '_' || g.file_version AS ggr_file_name
        , g.transaction_id || '_' || g.file_version || '_' || g.igo_operator_id || '_' || g.gaming_site_id || '_' || to_char(g.period_start, 'YYYYMMDD')::VARCHAR || '_' || g.product_code AS record_id
    FROM (
        SELECT 'Casino' AS product_code
            , period_end + INTERVAL '1 day' AS transaction_date
            , DATE_TRUNC('day', reports_start_time_et_ca_finance()) AS gaming_day
            , igo_operator_id
            , period_start
            , period_end
            , igo_operator_id || to_char(period_end - INTERVAL '1 day', 'YYYYMMDD') :: VARCHAR as transaction_id
            , gaming_site_id
            , file_version
        FROM
        igo_generated_id
    ) g
)

, final AS (
    SELECT 'E'||i.transaction_id AS transaction_id
        , TO_CHAR(i.transaction_date, 'YYYYMMDD') AS transaction_date
        , i.igo_operator_id
        , i.gaming_site_id
        , TO_CHAR(i.period_start, 'YYYYMMDD') AS period_start
        , TO_CHAR(i.period_end - INTERVAL '1 day', 'YYYYMMDD') AS period_end
        , TO_CHAR(i.gaming_day, 'YYYYMMDD') AS gaming_day
        , i.product_code
        , d.game_provider
        , d.game_category
        , d.game_name
        , d.wagers
        , d.winnings
        , d.free_bet_winnings
        , CASE
            WHEN d.free_bet_winnings = 0
            THEN 0.00
            ELSE ((d.wagers- d.winnings) * 0.10)
        END AS ed_cap
        , CASE
            WHEN free_bet_winnings >= ((d.wagers - d.winnings) * 0.10)
            THEN ((d.wagers - d.winnings) * 0.10)
            ELSE free_bet_winnings
        END AS eligible_deductions
        , i.file_version
    FROM summed_dataset d
    RIGHT JOIN igo_gen_id i
        ON i.product_code = d.product_code
        AND i.gaming_day = DATE_TRUNC('day',d.gaming_day)
)

SELECT *
FROM final
