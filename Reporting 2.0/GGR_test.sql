-- iGO GENERATED METADATA
WITH igo_generated_id AS (
    SELECT 'OP100001'::VARCHAR AS igo_operator_id
        , DATE_TRUNC('day',reports_start_time_et_ca_on()) AS period_start -- start timestamp
        , DATE_TRUNC('day',reports_start_time_et_ca_on()) AS period_end -- end timestamp
        , 'S100001A'::VARCHAR AS gaming_site_id
        , '1' as file_version
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

, igo_gen_id AS (
    SELECT *
        , 'GGR_' || g.igo_operator_id || '_' || g.transaction_id || '_' || to_char(g.period_start, 'YYYYMMDD')::VARCHAR || '_' || to_char(g.period_end, 'YYYYMMDD')::VARCHAR || '_' || g.file_version AS ggr_file_name
        , g.transaction_id || '_' || g.file_version || '_' || g.igo_operator_id || '_' || g.gaming_site_id || '_' || to_char(g.period_start, 'YYYYMMDD')::VARCHAR || '_' || g.product_code AS record_id
    FROM (
        SELECT 'Betting' AS product_code
            , period_end + INTERVAL '1 day' AS transaction_date
            , DATE_TRUNC('day',reports_start_time_et_ca_on()) AS gaming_day
            , igo_operator_id
            , period_start
            , period_end
            , igo_operator_id || to_char(period_end, 'YYMMDD')::VARCHAR as transaction_id
            , gaming_site_id
            , file_version
        FROM igo_generated_id
        UNION ALL
        SELECT 'Casino' AS product_code
            , period_end + INTERVAL '1 day' AS transaction_date
            , DATE_TRUNC('day',reports_start_time_et_ca_on()) AS gaming_day
            , igo_operator_id
            , period_start
            , period_end
            , igo_operator_id || to_char(period_end, 'YYMMDD')::VARCHAR as transaction_id
            , gaming_site_id
            , file_version
        FROM igo_generated_id
    ) g
)

, free_bets_wagered AS(
SELECT DISTINCT
        b.patron_id
        , b.placed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , b.id AS bet_id
        , SUM(b.bet_amount_cents)/100.0 AS free_bets_wagered
    FROM ca_on_reports.max_vegas_bets AS b
    INNER JOIN ca_on_reports.max_identity_users_in_state u ON u.patron_id=b.patron_id
    WHERE b.free_bet_id IS NOT NULL
    AND u.is_tester IS FALSE
    GROUP BY 1,2,3)

, free_bets AS (
SELECT DISTINCT
        COALESCE(p_vegas_bet_id,p_bet_id) AS bet_id
    FROM ca_edgebook_snapshot.max_accounting_tables_all a
    INNER JOIN ca_on_reports.max_identity_users_in_state u ON u.patron_id=a.a_patron_id
    WHERE p_type = 'sportsbook_free_bet'
    AND gaming_state='CA-ON'
    AND u.is_tester IS FALSE)

-- Resettled sport wagers
, resettled_sport_wagers_accounting AS (
    SELECT t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , a_patron_id AS patron_id
        , COALESCE(p_vegas_bet_id, p_bet_id) AS bet_id
        , coalesce(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) as free_bet_id
        , s.state
        , s.previous_state
        , a_type AS type
        , new_timestamp
        , new_previous_timestamp
        , COALESCE(CASE
            -- void patron transactions
            WHEN s.state='voided' AND s.previous_state='open' THEN 0.0
            WHEN s.state='voided' AND s.previous_state='loss' THEN 0.0
            WHEN s.state='voided' AND s.previous_state='win' AND coalesce(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NULL AND a_type IN ('sportsbook_unsettled_bets','sportsbook_house_wins') THEN SUM(credit_cents)/100.0
            WHEN s.state='voided' AND s.previous_state='push' AND coalesce(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NULL AND a_type IN ('sportsbook_unsettled_bets','sportsbook_house_wins') THEN SUM(credit_cents)/100.0
            -- cancelled patron transactions
            WHEN s.state='refund' AND s.previous_state='open' THEN 0.0
            WHEN s.state='refund' AND s.previous_state='loss' AND coalesce(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NULL AND a_type IN ('customer_liability') THEN -1.0*SUM(credit_cents)/100.0
            WHEN s.state='refund' AND s.previous_state='win' AND coalesce(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NULL AND a_type IN ('sportsbook_house_wins') THEN SUM(credit_cents)/100.0
            WHEN s.state='refund' AND s.previous_state='push' AND coalesce(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NULL AND a_type IN ('sportsbook_house_wins') THEN SUM(credit_cents)/100.0

            -- interim cancelled patron transactions
            WHEN DATE_TRUNC('day',new_timestamp) = DATE_TRUNC('day',new_previous_timestamp) AND s.state='loss' AND s.previous_state='refund' AND coalesce(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NULL AND a_type IN ('customer_liability') THEN SUM(debit_cents)/100.0
            WHEN DATE_TRUNC('day',new_timestamp) = DATE_TRUNC('day',new_previous_timestamp) AND s.state='win' AND s.previous_state='refund' AND coalesce(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NULL AND a_type IN ('sportsbook_unsettled_bets','sportsbook_house_wins') THEN -1.0*((SUM(debit_cents)/100.0) - (SUM(credit_cents)/100.0))
            WHEN DATE_TRUNC('day',new_timestamp) = DATE_TRUNC('day',new_previous_timestamp) AND s.state='push' AND s.previous_state='refund' AND coalesce(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NULL AND a_type IN ('sportsbook_unsettled_bets','sportsbook_house_wins') THEN -1.0*SUM(debit_cents)/100.0
            WHEN DATE_TRUNC('day',new_timestamp) != DATE_TRUNC('day',new_previous_timestamp) AND s.state NOT IN ('refund','voided') AND s.previous_state='refund' AND a_type='customer_liability'THEN (SUM(debit_cents)/100.0) - (SUM(credit_cents)/100.0)

            -- regrade and ungrade patron transactions
            WHEN s.state='open' AND s.previous_state IN ('voided') THEN 0.0
            WHEN s.state='open' AND s.previous_state IN ('refund') AND coalesce(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NULL AND a_type IN ('customer_liability') THEN SUM(debit_cents)/100.0
            -- ignore impact to customer_liability in specific cases handled by summing only credits
            WHEN s.state='voided' AND s.previous_state='win' AND coalesce(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NULL THEN 0.0
            WHEN s.state='voided' AND s.previous_state='push' AND coalesce(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NULL THEN 0.0
            WHEN s.state='refund' AND s.previous_state='win' AND coalesce(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NULL THEN 0.0
            WHEN s.state='refund' AND s.previous_state='push' AND coalesce(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NULL THEN 0.0
            WHEN s.state='win' AND s.previous_state='refund' AND coalesce(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NULL THEN 0.0
            WHEN s.state='push' AND s.previous_state='refund' AND coalesce(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NULL THEN 0.0
            -- Free Bets ELSE (catch-all)
            WHEN a_type='customer_liability' AND s.state IN ('refund','voided') AND coalesce(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NOT NULL THEN (SUM(debit_cents)/100.0) - (SUM(credit_cents)/100.0)
            WHEN a_type='customer_liability' AND s.state IN ('win', 'loss') AND s.previous_state NOT IN ('open') AND coalesce(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NOT NULL THEN (SUM(debit_cents)/100.0) - (SUM(credit_cents)/100.0)
            -- Ticket DATA-8788
            WHEN a_type='customer_liability' AND  s.state IN ('win') AND s.previous_state IN ('open') AND coalesce(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NOT NULL
                        THEN (SUM(debit_cents)/100.0) - (SUM(credit_cents)/100.0)
            WHEN a_type='customer_liability' AND s.state IN ('open') AND s.previous_state IN ('win') AND coalesce(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NOT NULL
                        THEN (SUM(debit_cents)/100.0) - (SUM(credit_cents)/100.0)
            -- Cash Bets ELSE (catch-all)
            WHEN a_type='customer_liability' AND s.state NOT IN ('refund','voided') AND coalesce(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NULL THEN (SUM(debit_cents)/100.0) - (SUM(credit_cents)/100.0)
                         END,0.0) AS resettled_sport_wagers
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS p
    INNER JOIN ca_on_reports.max_bet_states AS s
        ON p.bet_history_id = s.bet_history_id
    INNER JOIN ca_on_reports.max_identity_users_in_state u
        ON u.patron_id=p.a_patron_id
    WHERE (p.p_type='sportsbook_bet_resettlement' OR
        p.p_type='sportsbook_bet_ungrade' OR
        p.p_type='sportsbook_bet_void')
        AND t_changed_at >= reports_start_time_utc_ca_on()
        AND t_changed_at < reports_end_time_utc_ca_on()
        AND p.gaming_state='CA-ON'
        AND u.is_tester IS FALSE
    GROUP BY 1,2,3,4,5,6,7,8,9)

-- Deprecated due to Vegas
-- , cancelled_bets AS (
--     SELECT DISTINCT DATE_TRUNC('day',closed_at AT time zone 'utc' AT time zone 'America/Toronto') AS gaming_date
--         , SUM(COALESCE(bet_amount_cents,0.0))/100.0 AS cancelled_amount
--     FROM ca_on_reports.max_vegas_bets AS b
--     INNER JOIN ca_on_reports.max_identity_users_in_state u ON b.patron_id=u.patron_id
--     WHERE b.status = 'cancelled'
--     AND b.free_bet_id IS NULL
--     AND u.is_tester IS FALSE
--     GROUP BY 1
-- )

-- CASH VOIDED
, voided_bets AS (
    SELECT DISTINCT DATE_TRUNC('day',closed_at AT time zone 'utc' AT time zone 'America/Toronto') AS gaming_date
        , SUM(COALESCE(bet_amount_cents,0.0))/100.0 AS voided_amount
    FROM ca_on_reports.max_vegas_bets AS b
    INNER JOIN ca_on_reports.max_identity_users_in_state u ON b.patron_id=u.patron_id
    WHERE b.status = 'voided'
    AND b.free_bet_id IS NULL
    AND u.is_tester IS FALSE
    GROUP BY 1
)

-- CASH BETS WAGERED
, total_bets_wagered_dr AS (
    SELECT DISTINCT a.ledger_transaction_id
        ,a.a_patron_id AS patron_id
        ,a.a_type
        ,COALESCE(a.debit_cents,0.0) AS debit_cents
        ,t_changed_at AS updated_at
    FROM ca_edgebook_snapshot.max_accounting_tables_all a
    LEFT JOIN free_bets fb
        ON COALESCE(a.p_vegas_bet_id,a.p_bet_id) = fb.bet_id
    INNER JOIN ca_on_reports.max_identity_users_in_state u
        ON u.patron_id=a.p_patron_id
    WHERE t_type IN ('sportsbook_bet')
    AND a_type = 'customer_liability'
    AND gaming_state='CA-ON'
    AND fb.bet_id IS NULL
    AND debit_cents > 0
    AND u.is_tester IS FALSE
)

, total_bets_wagered_cr AS (
    SELECT DISTINCT a.ledger_transaction_id
        ,a.a_patron_id AS patron_id
        ,a.a_type
        ,COALESCE(a.credit_cents,0.0) AS credit_cents
        ,t_changed_at AS updated_at
    FROM ca_edgebook_snapshot.max_accounting_tables_all a
    LEFT JOIN free_bets fb
        ON COALESCE(a.p_vegas_bet_id,a.p_bet_id) = fb.bet_id
    INNER JOIN ca_on_reports.max_identity_users_in_state u
        ON u.patron_id=a.p_patron_id
    WHERE t_type IN ('sportsbook_bet')
    AND a.gaming_state = 'CA-ON'
    AND a_type = 'sportsbook_unsettled_bets'
    AND fb.bet_id IS NULL
    AND credit_cents > 0
    AND u.is_tester IS FALSE
)

, transfer_from_sports AS (
    -- cash payouts
    SELECT DISTINCT
        t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , a_patron_id AS patron_id
        , COALESCE(p_vegas_bet_id,p_bet_id) AS bet_id
        , (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) AS transfer_from_sports
    FROM ca_edgebook_snapshot.max_accounting_tables_all a
    LEFT JOIN free_bets_wagered fb
        ON COALESCE(a.p_vegas_bet_id,a.p_bet_id)=fb.bet_id
    INNER JOIN ca_on_reports.max_identity_users_in_state u
        ON u.patron_id=a.p_patron_id
    WHERE p_type IN ('sportsbook_cash_out_bet','sportsbook_payout','sportsbook_bet_lost')
    AND a_type='customer_liability'
    AND a.gaming_state = 'CA-ON'
    AND fb.bet_id IS NULL -- Winnings does not include those from payouts from freebets
    AND u.is_tester IS FALSE
    GROUP BY 1,2,3

    UNION ALL

    SELECT DISTINCT
        t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , a_patron_id AS patron_id
        , COALESCE(p_vegas_bet_id, p_bet_id) AS bet_id
        , COALESCE(
            CASE WHEN COALESCE(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NULL AND s.state='refund'
                THEN (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0)
                WHEN COALESCE(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NOT NULL AND s.state='win'
                THEN (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0)
            END, 0.0) AS transfer_from_sports
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS p
    INNER JOIN ca_on_reports.max_bet_states AS s
    ON p.bet_history_id = s.bet_history_id
    INNER JOIN ca_on_reports.max_identity_users_in_state u
        ON u.patron_id=p.p_patron_id
    WHERE (p.p_type='sportsbook_bet_resettlement')
        AND a_type IN ('customer_liability')
        AND s.state IN ('refund','win')
        AND previous_ordinal = '1'
        AND s.previous_state='open'
    AND t_changed_at >= reports_start_time_utc_ca_on()
        AND t_changed_at < reports_end_time_utc_ca_on()
        AND p.gaming_state='CA-ON'
        AND u.is_tester IS FALSE
    GROUP BY 1,2,3,COALESCE(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR), s.state

     UNION ALL

    SELECT DISTINCT
        t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , a_patron_id AS patron_id
        , COALESCE(p_vegas_bet_id, p_bet_id) AS bet_id
        , COALESCE((SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0)
                , 0.0)
            AS transfer_from_sports
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS p
    INNER JOIN ca_on_reports.max_bet_states AS s
    ON p.bet_history_id = s.bet_history_id
    INNER JOIN ca_on_reports.max_identity_users_in_state u
        ON u.patron_id = p.a_patron_id
        AND u.is_tester IS FALSE
    WHERE (p.p_type='sportsbook_bet_resettlement')
        AND a_type IN ('customer_liability')
        AND COALESCE(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NULL
        AND s.state='refund'
        AND s.previous_state='open'
        AND previous_ordinal > '1'
    AND t_changed_at >= reports_start_time_utc_ca_on()
        AND t_changed_at < reports_end_time_utc_ca_on()
        AND p.gaming_state='CA-ON'
    GROUP BY 1,2,3
)

-- CASINO

, dr_fu_cash_casino_rollback AS (
    SELECT DISTINCT ledger_transaction_id
        , a_patron_id AS patron_id
        , a_type AS type
        , debit_cents
        , t_changed_at AS updated_at
        , p_id AS remote_wallet_transaction_id
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS a
    INNER JOIN ca_on_reports.max_identity_users_in_state u
        ON a.p_patron_id=u.patron_id
    WHERE p_type in ('casino_rollback')
    AND a_type='casino_house_wins'
    AND gaming_state='CA-ON'
    AND debit_cents>0
    AND u.is_tester IS FALSE
)

, cr_cl_cash_casino_rollback AS (
    SELECT DISTINCT ledger_transaction_id
        , a_patron_id AS patron_id
        , a_type AS type
        , credit_cents
        , t_changed_at AS updated_at
        , p_id AS remote_wallet_transaction_id
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS a
    INNER JOIN ca_on_reports.max_identity_users_in_state u
        ON u.patron_id=a.p_patron_id
    WHERE p_type in ('casino_rollback')
    AND a_type in ('customer_liability')
    AND gaming_state = 'CA-ON'
    AND credit_cents>0
    AND u.is_tester IS FALSE
)

, casino_rollback AS (
    SELECT d.updated_at AT time zone 'UTC' AT time zone 'America/Toronto' AS transaction_timestamp
        , SUM(debit_cents/100.00) * -1.0 AS total_rollback
    FROM dr_fu_cash_casino_rollback d
    INNER JOIN cr_cl_cash_casino_rollback c
        ON c.ledger_transaction_id = d.ledger_transaction_id
        AND debit_cents = credit_cents
    GROUP BY 1
)


-- DATA-8992
-- -- CASINO WAGERS
-- , cash_rounds_wagered AS (
--      SELECT DISTINCT
--         pt.patron_id,
--         w.amount_cents,
--         w._changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp,
--         w.patron_transaction_id
--     FROM ca_on_reports.max_casino_wagers AS w
--     LEFT JOIN ca_on_reports.max_patron_transactions pt
--         ON pt.id=w.patron_transaction_id
--     INNER JOIN ca_on_reports.max_identity_users_in_state u
--         ON u.patron_id=pt.patron_id
--     WHERE w.promo_engine_free_round_award_id IS NULL
--     AND amount_cents !=0
--     AND u.is_tester IS FALSE
-- )

, dr_fu_cash_casino AS (
    SELECT DISTINCT ledger_transaction_id
        , a_patron_id AS patron_id
        , a_type AS type
        , debit_cents
        , t_changed_at AS updated_at
        , p_id AS patron_transaction_id
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS a
    WHERE p_type in ('casino_wager') -- excludes casino_free_wager
        AND a_type='customer_liability'
        AND debit_cents>0
        AND t_changed_at >= reports_start_time_utc_ca_on()
        AND t_changed_at < reports_end_time_utc_ca_on()
        AND a.gaming_state = 'CA-ON'
)
, cr_cl_cash_casino AS (
    SELECT DISTINCT ledger_transaction_id
        , a_patron_id AS patron_id
        , a_type AS type
        , credit_cents
        , t_changed_at AS updated_at
        , p_id AS patron_transaction_id
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS a
    WHERE p_type in ('casino_wager')
        AND a_type in ('casino_house_wins')
        AND credit_cents>0
        AND t_changed_at >= reports_start_time_utc_ca_on()
        AND t_changed_at < reports_end_time_utc_ca_on()
        AND a.gaming_state = 'CA-ON'
)
, transfer_to_casino AS (
    -- CASH BETS
    SELECT DISTINCT
        d.updated_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , d.patron_id
        , d.patron_transaction_id
        , SUM(debit_cents)/100.0 AS transfer_to_casino
    FROM dr_fu_cash_casino AS d
    INNER JOIN cr_cl_cash_casino AS c
        ON d.ledger_transaction_id=c.ledger_transaction_id
        AND debit_cents=credit_cents
    JOIN ca_on_reports.max_identity_users u
        ON u.patron_id = d.patron_id AND u.is_tester IS False
    GROUP BY 1,2,3
)

-- CASINO CASH PAYOUT
, transfer_from_casino AS (
    SELECT DISTINCT
        a.t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , a.a_patron_id
        , p_id AS patron_transaction_id
        , SUM(credit_cents) - SUM(debit_cents) AS transfer_from_casino
    FROM ca_edgebook_snapshot.max_accounting_tables_all a
    LEFT JOIN ca_on_reports.max_casino_payouts p
        ON p.patron_transaction_id=a.patron_transaction_id
    INNER JOIN ca_on_reports.max_identity_users_in_state u
        ON u.patron_id=a.p_patron_id
    WHERE p_type IN ('casino_payout')
        AND a_type='customer_liability'
        AND gaming_state='CA-ON'
        AND p.promo_engine_free_round_award_id IS NULL
        AND u.is_tester IS FALSE
        AND t_changed_at >= reports_start_time_utc_ca_on()
        AND t_changed_at < reports_end_time_utc_ca_on()
    GROUP BY 1,2,3
)

, jackpot_contributions_pre AS  (
    SELECT
        w.transaction_timestamp
        , wg.game_name
        , (CASE WHEN g.game_provider_game_category='live' THEN 'Live Dealer'
                                WHEN g.game_provider_game_category='roulette' THEN 'Table'
                                WHEN g.game_provider_game_category='S' THEN 'Slots'
                                WHEN g.game_provider_game_category='V' THEN 'VideoPoker'
                              ELSE UPPER(LEFT(g.game_provider_game_category, 1))|| LOWER(SUBSTRING(g.game_provider_game_category, 2, LENGTH(g.game_provider_game_category))) END) AS product_segment
        , SUM(amount_precision) AS transfer_from_casino
    FROM transfer_to_casino w
    JOIN JACKPOTS_TEST j
        ON w.patron_transaction_id::VARCHAR = j.remote_wallet_transaction_id
        AND j.type = 'contribution'
    LEFT JOIN ca_on_reports.max_wager_and_game_info wg
        ON w.patron_transaction_id = wg.patron_transaction_id
    LEFT JOIN ca_on_reports.max_games g
    ON g.id::varchar = wg.game_id
    GROUP BY 1,2,3
)

-- jackpot payout for CASH
, jackpot_payout_pre AS  (
    SELECT t.transaction_timestamp
        , t.a_patron_id
        , wg.game_name
        , (CASE WHEN g.game_provider_game_category='live' THEN 'Live Dealer'
                                WHEN g.game_provider_game_category='roulette' THEN 'Table'
                                WHEN g.game_provider_game_category='S' THEN 'Slots'
                                WHEN g.game_provider_game_category='V' THEN 'VideoPoker'
                              ELSE UPPER(LEFT(g.game_provider_game_category, 1))|| LOWER(SUBSTRING(g.game_provider_game_category, 2, LENGTH(g.game_provider_game_category))) END) AS product_segment
        , SUM(amount_precision) AS transfer_from_casino
    FROM transfer_from_casino t
    JOIN JACKPOTS_TEST j
        ON t.patron_transaction_id::VARCHAR = j.remote_wallet_transaction_id
        AND j.free_round_award_id IS NULL
    LEFT JOIN ca_on_reports.max_wager_and_game_info wg
        ON j.remote_wallet_transaction_id = wg.patron_transaction_id::VARCHAR
    LEFT JOIN ca_on_reports.max_games g
    ON g.id::varchar = wg.game_id
    WHERE j.type = 'win'
    GROUP BY 1,2,3,4
)

, jackpot_net_adjustments_pre AS  (
       SELECT DATE_TRUNC('day', transaction_timestamp) AS gaming_day
        , game_name
        , product_segment
        , transfer_from_casino AS transfer_from_casino
    FROM jackpot_contributions_pre

    UNION ALL

    SELECT DATE_TRUNC('day', transaction_timestamp) AS gaming_day
        , game_name
        , product_segment
        , -1.0*transfer_from_casino AS transfer_from_casino
    FROM jackpot_payout_pre
)

, jackpot_net_adjustments AS (
    SELECT gaming_day
    , game_name
    , SUM(transfer_from_casino)::NUMERIC(38,2) AS transfer_from_casino
    FROM jackpot_net_adjustments_pre
    GROUP BY 1,2
)

, dataset AS (
    SELECT DATE_TRUNC('day',transaction_timestamp) AS gaming_day
        , 'Betting' AS product_code
        , 0.00 AS wagers
        , SUM(resettled_sport_wagers) * -1.00 AS winnings
        , 0.00 AS free_bet_winnings
    FROM resettled_sport_wagers_accounting
    WHERE free_bet_id IS NULL
    GROUP BY 1
    UNION ALL
    -- Deprecated due to Vegas
    -- SELECT gaming_date AS gaming_day
    --     , 'Betting' AS product_code
    --     , cancelled_amount * -1.00 AS wagers
    --     , 0.00 AS winnings
    --     , 0.00 AS free_bet_winnings
    -- FROM cancelled_bets
    -- UNION ALL
    SELECT gaming_date AS gaming_day
        , 'Betting' AS product_code
        , voided_amount * -1.00 AS wagers
        , 0.00 AS winnings
        , 0.00 AS free_bet_winnings
    FROM voided_bets
    UNION ALL
    SELECT DATE_TRUNC('day',d.updated_at AT time zone 'utc' AT time zone 'America/Toronto') AS gaming_day
        , 'Betting' AS product_code
        , SUM(COALESCE(debit_cents,0.00)/100.00) AS wagers
        , 0.00 AS winnings
        , 0.00 AS free_bet_winnings
    FROM total_bets_wagered_dr d
    INNER JOIN total_bets_wagered_cr c
        ON d.ledger_transaction_id = c.ledger_transaction_id
        AND debit_cents = credit_cents
    GROUP BY 1
    UNION ALL
    SELECT DATE_TRUNC('day',transaction_timestamp) AS gaming_day
        , 'Betting' AS product_code
        , 0.00 AS wagers
        , transfer_from_sports AS winnings
        , 0.00 AS free_bet_winnings
    FROM transfer_from_sports
    UNION ALL
    SELECT DATE_TRUNC('day',transaction_timestamp) AS gaming_day
        , 'Casino' AS product_code
        , 0.00 AS wagers
        , SUM(transfer_from_casino/100.00) AS winnings
        , 0.00 AS free_bet_winnings
    FROM transfer_from_casino
    GROUP BY 1
    UNION ALL
    SELECT DATE_TRUNC('day',transaction_timestamp) AS gaming_day
        , 'Casino' AS product_code
        , SUM(transfer_to_casino) AS wagers
        , 0.00 AS winnings
        , 0.00 AS free_bet_winnings
    FROM transfer_to_casino
    GROUP BY 1
    UNION ALL
    SELECT gaming_day
        , 'Casino' AS product_code
        , 0.00 AS wagers
        , SUM(transfer_from_casino) AS winnings
        , 0.00 AS free_bet_winnings
    FROM jackpot_net_adjustments
    GROUP BY 1
    UNION ALL
    SELECT DATE_TRUNC('day',transaction_timestamp) AS gaming_day
        , 'Casino' AS product_code
        , total_rollback AS wagers
        , 0.00 AS winnings
        , 0.00 AS free_bet_winnings
    FROM casino_rollback
)

, final AS (
    SELECT i.transaction_id
    , to_char(i.transaction_date, 'YYYYMMDD') AS transaction_date
    , i.igo_operator_id
    , i.gaming_site_id
    , to_char(i.period_start, 'YYYYMMDD') AS period_start
    , to_char(i.period_end, 'YYYYMMDD') AS period_end
    , to_char(i.gaming_day, 'YYYYMMDD') AS gaming_day
    , i.product_code
    , NULL AS ggr
    , SUM(COALESCE(wagers,0.00)) AS wagers
    , SUM(COALESCE(winnings,0.00)) AS winnings
    , 0.00  AS eligible_deductions
    , i.file_version
    FROM dataset d
    RIGHT JOIN igo_gen_id i
        ON d.product_code = i.product_code
        AND d.gaming_day = i.gaming_day
    GROUP BY 1,2,3,4,5,6,7,8,13
)

SELECT transaction_id AS "TransactionID"
    , transaction_date AS "TransactionDate"
    , igo_operator_id AS "iGOOperatorID"
    , gaming_site_id AS "GamingSiteID"
    , period_start AS "PeriodStart"
    , period_end AS "PeriodEnd"
    , gaming_day AS "GamingDay"
    , product_code AS "ProductCode"
    , wagers - winnings - eligible_deductions AS "GGR"
    , wagers AS "Wagers"
    , winnings AS "Winnings"
    , eligible_deductions AS "EligibleDeductions"
    , file_version AS "FileVersion"
FROM final
