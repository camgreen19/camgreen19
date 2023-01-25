WITH free_bets_wagered AS(
    SELECT DISTINCT
        b.patron_id
        , b.placed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , b.id AS bet_id
        , SUM(b.bet_amount_cents)/100.0 AS free_bets_wagered
    FROM ca_on_reports.max_vegas_bets AS b
    WHERE b.free_bet_id::VARCHAR IS NOT NULL
        AND b.placed_at >= reports_start_time_utc_ca_on()
        AND b.placed_at < reports_end_time_utc_ca_on()
    GROUP BY 1,2,3
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

, free_bets AS (
    -- all free bets that were placed / closed with in the period
    SELECT DISTINCT b.id as bet_id
    FROM ca_on_reports.max_vegas_bets AS b
    JOIN ca_edgebook_snapshot.max_accounting_tables_all a  ON b.id = COALESCE(a.p_vegas_bet_id,a.p_bet_id)
    WHERE b.free_bet_id IS NOT NULL
    AND a.t_changed_at >= reports_start_time_utc_ca_on()
    AND  a.t_changed_at < reports_end_time_utc_ca_on()
    AND a.gaming_state = 'CA-ON'
)

, free_rounds_wagered AS (
    SELECT DISTINCT
        pt._changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , w.patron_transaction_id::text
        , pt.patron_id
        , SUM(w.amount_cents)/100.0 AS free_rounds_wagered
    FROM ca_on_reports.max_casino_wagers AS w
    JOIN ca_on_reports.max_patron_transactions pt ON pt.id=w.patron_transaction_id
    WHERE w.promo_engine_free_round_award_id IS NOT NULL
        AND pt._changed_at >= reports_start_time_utc_ca_on()
        AND pt._changed_at < reports_end_time_utc_ca_on()
    GROUP BY 1,2,3
)

-- , free_rounds_wagered_rollback AS (
--         SELECT DISTINCT
--         t._changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
--         , remote_wallet_transaction_id AS patron_transaction_id
--         , identity_user_id AS patron_id
--         , SUM(amount)/100.0 AS free_rounds_wagered_rollback
--     FROM ca_on_reports.max_wallet_transactions t
--     WHERE rolled_back=TRUE AND free_round_award_id IS NOT NULL
--     GROUP BY 1,2,3
-- )

, free_rounds_wagered_rollback AS (
    SELECT DISTINCT
           pt._changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
         , r.patron_transaction_id AS patron_transaction_id
         , wg.patron_id AS patron_id
         , SUM(wg.amount_cents)/100.0 AS free_rounds_wagered_rollback
    FROM ca_on_reports.max_casino_wager_rollbacks r
    JOIN ca_on_reports.max_patron_transactions pt ON r.patron_transaction_id = pt.id
    LEFT JOIN ca_on_reports.max_wager_and_game_info wg ON r.patron_transaction_id=wg.patron_transaction_id
    WHERE wg.type IN ('rollback') AND wg.promo_engine_free_round_award_id IS NOT NULL
        AND pt._changed_at >= reports_start_time_utc_ca_on()
        AND pt._changed_at < reports_end_time_utc_ca_on()
    GROUP BY 1,2,3
)

-- , free_bets_payouts AS (
--     SELECT DISTINCT
--         b.patron_id
--         , b.closed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
--         , b.id AS bet_id
--         , SUM(b.payout_amount_cents)/100.0 AS free_bets_payouts
--     FROM ca_on_reports.max_bets AS b
--     WHERE COALESCE(b.promo_engine_free_bet_id::VARCHAR,b.free_bet_id::VARCHAR) IS NOT NULL
--     AND status not in ('open')
--     GROUP BY 1,2,3
--     -- double check status condition in other reports and also make sure theres no overlap with resettled freebet logic
-- )
, free_bets_payouts AS (
        -- cash payouts
    SELECT DISTINCT a_patron_id AS patron_id
            , t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
            , COALESCE(p_vegas_bet_id, p_bet_id) AS bet_id
            , (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) AS free_bets_payouts
    FROM ca_edgebook_snapshot.max_accounting_tables_all a
    LEFT JOIN free_bets fb ON COALESCE(a.p_vegas_bet_id, a.p_bet_id)=fb.bet_id
    WHERE p_type IN ('sportsbook_cash_out_bet','sportsbook_payout','sportsbook_bet_lost')
        AND a_type='customer_liability'
        AND fb.bet_id IS NOT NULL -- Winnings only from freebets
        AND t_changed_at >= reports_start_time_utc_ca_on()
        AND t_changed_at < reports_end_time_utc_ca_on()
        AND a.gaming_state = 'CA-ON'
    GROUP BY 1,2,3
)

, free_rounds_payouts AS (
    SELECT DISTINCT
        pt.patron_id
        , pt._changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , p.patron_transaction_id
        , SUM(p.amount_cents)/100.0 AS free_rounds_payouts
    FROM ca_on_reports.max_casino_payouts AS p
    JOIN ca_on_reports.max_patron_transactions pt ON pt.id=p.patron_transaction_id
    WHERE p.promo_engine_free_round_award_id IS NOT NULL
      AND amount_cents !=0
      AND pt._changed_at >= reports_start_time_utc_ca_on()
      AND pt._changed_at < reports_end_time_utc_ca_on()
    GROUP BY 1,2,3
)

-- jackpot payout for free round
, jackpot_payout_free AS (
    SELECT DISTINCT f.transaction_timestamp
        , f.patron_id
        , f.patron_transaction_id
        , t.amount_precision AS free_rounds_payouts
    FROM free_rounds_payouts f
    JOIN JACKPOTS_TEST t
        ON f.patron_transaction_id::VARCHAR = t.remote_wallet_transaction_id
        AND t.free_round_award_id IS NOT NULL
    WHERE t.type = 'win'
)

, dr_fu_cash_sportsbook AS (
    SELECT DISTINCT ledger_transaction_id
        , a_patron_id AS patron_id
        , a_type AS type
        , debit_cents
        , t_changed_at AS updated_at
        , COALESCE(p_vegas_bet_id, p_bet_id) AS bet_id
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS a
    LEFT JOIN free_bets AS fb
        ON COALESCE(a.p_vegas_bet_id, a.p_bet_id) = fb.bet_id
    WHERE p_type in ('sportsbook_bet')
        AND a_type='customer_liability'
        AND fb.bet_id IS NULL -- only cash wagers
        AND debit_cents>0
        AND t_changed_at >= reports_start_time_utc_ca_on()
        AND t_changed_at < reports_end_time_utc_ca_on()
        AND a.gaming_state = 'CA-ON'
)

, cr_cl_cash_sportsbook AS (
    SELECT DISTINCT ledger_transaction_id
        , a_patron_id AS patron_id
        , a_type AS type
        , credit_cents
        , t_changed_at AS updated_at
        , COALESCE(p_vegas_bet_id, p_bet_id) AS bet_id
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS a
    LEFT JOIN free_bets AS fb
        ON COALESCE(a.p_vegas_bet_id, a.p_bet_id) = fb.bet_id
    WHERE p_type in ('sportsbook_bet')
        AND a_type in ('sportsbook_unsettled_bets')
        AND fb.bet_id IS NULL -- only cash wagers
        AND credit_cents>0
        AND t_changed_at >= reports_start_time_utc_ca_on()
        AND t_changed_at < reports_end_time_utc_ca_on()
        AND a.gaming_state = 'CA-ON'
)

, transfer_to_sports AS (
    -- CASH BETS
    SELECT DISTINCT
        d.updated_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , d.patron_id
        , d.bet_id
        , SUM(debit_cents)/100.0 AS transfer_to_sports
    FROM dr_fu_cash_sportsbook AS d
    INNER JOIN cr_cl_cash_sportsbook AS c
        ON d.ledger_transaction_id=c.ledger_transaction_id
        AND debit_cents=credit_cents
    GROUP BY 1,2,3
)

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
    GROUP BY 1,2,3
)

, jackpot_contributions AS (
    SELECT
        transaction_timestamp
        , w.patron_id
        , w.patron_transaction_id
        , SUM(amount_precision) AS transfer_from_casino
    FROM transfer_to_casino w
    JOIN JACKPOTS_TEST j
        ON w.patron_transaction_id::VARCHAR = j.remote_wallet_transaction_id
        AND j.type = 'contribution'
    GROUP BY 1,2,3
)

-- , transfer_to_casino_rollback AS  (
--     SELECT DISTINCT
--             t._changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
--             , identity_user_id AS patron_id
--             , remote_wallet_transaction_id AS patron_transaction_id
--             , SUM(amount)/100 AS transfer_to_casino_rollback
--     FROM ca_on_reports.max_wallet_transactions t
--     WHERE rolled_back=TRUE AND free_round_award_id IS NULL
--     GROUP BY 1,2,3
-- )

, transfer_to_casino_rollback AS (
    SELECT DISTINCT
           pt._changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
         , wg.patron_id AS patron_id
         , r.patron_transaction_id AS patron_transaction_id
         , SUM(wg.amount_cents)/100.0 AS transfer_to_casino_rollback
    FROM ca_on_reports.max_casino_wager_rollbacks r
    JOIN ca_on_reports.max_patron_transactions pt ON r.patron_transaction_id = pt.id
    LEFT JOIN ca_on_reports.max_wager_and_game_info wg ON r.patron_transaction_id=wg.patron_transaction_id
    WHERE wg.type IN ('rollback') AND wg.promo_engine_free_round_award_id IS NULL
        AND pt._changed_at >= reports_start_time_utc_ca_on()
        AND pt._changed_at < reports_end_time_utc_ca_on()
    GROUP BY 1,2,3
)


, transfer_from_sports AS (
    -- cash + free bet cashouts / payouts
    SELECT DISTINCT
        t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , a_patron_id AS patron_id
        , COALESCE(p_vegas_bet_id, p_bet_id) AS bet_id
        , (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) AS transfer_from_sports
    FROM ca_edgebook_snapshot.max_accounting_tables_all a
    LEFT JOIN free_bets fb ON COALESCE(a.p_vegas_bet_id, a.p_bet_id) = fb.bet_id
    WHERE p_type IN ('sportsbook_cash_out_bet','sportsbook_payout','sportsbook_bet_lost')
        AND a_type='customer_liability'
        AND fb.bet_id IS NULL -- Winnings does not include those from payouts from freebets
        AND t_changed_at >= reports_start_time_utc_ca_on()
        AND t_changed_at < reports_end_time_utc_ca_on()
        AND a.gaming_state = 'CA-ON'
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
    WHERE (p.p_type='sportsbook_bet_resettlement')
        AND a_type IN ('customer_liability')
        AND s.state IN ('refund','win')
        AND previous_ordinal = '1'
        AND s.previous_state='open'
        AND t_changed_at >= reports_start_time_utc_ca_on()
        AND t_changed_at < reports_end_time_utc_ca_on()
        AND p.gaming_state='CA-ON'
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


, transfer_from_casino AS (
    -- cash + free bet cashouts / payouts
    SELECT DISTINCT
        t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , a_patron_id AS patron_id
        , p_id AS patron_transaction_id
        , (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) AS transfer_from_casino
    FROM ca_edgebook_snapshot.max_accounting_tables_all a
    LEFT JOIN ca_on_reports.max_casino_payouts p ON p.patron_transaction_id=a.patron_transaction_id
    WHERE p_type IN ('casino_payout')
         AND a_type='customer_liability'
         AND p.promo_engine_free_round_award_id IS NULL
         AND t_changed_at >= reports_start_time_utc_ca_on()
         AND t_changed_at < reports_end_time_utc_ca_on()
         AND a.gaming_state = 'CA-ON'
    GROUP BY 1,2,3
)

-- jackpot payout for cash round
, jackpot_payout_cash AS (
    SELECT DISTINCT t.transaction_timestamp
        , t.patron_id
        , t.patron_transaction_id
        , j.amount_precision AS transfer_from_casino
    FROM transfer_from_casino t
    JOIN JACKPOTS_TEST j
        ON t.patron_transaction_id::VARCHAR = j.remote_wallet_transaction_id
        AND j.free_round_award_id IS NULL
    WHERE j.type = 'win'
)

, pending_sport_wagers AS (
    SELECT DISTINCT
        reports_start_time_et_ca_on()::TIMESTAMP AS transaction_timestamp
        , patron_id
        , id AS bet_id
        , SUM(bet_amount_cents)/100.0 AS pending_sport_wagers
    FROM ca_on_reports.max_vegas_bets
    WHERE status='open'
        AND placed_at < reports_end_time_utc_ca_on()
        AND free_bet_id::VARCHAR IS NULL
    GROUP BY 1,2,3
)

, pending_sport_wagers_freebets AS (
    SELECT DISTINCT
        reports_start_time_et_ca_on()::TIMESTAMP AS transaction_timestamp
        , patron_id
        , id AS bet_id
        , SUM(bet_amount_cents)/100.0 AS pending_sport_wagers_freebets
    FROM ca_on_reports.max_vegas_bets
    WHERE status='open'
        AND placed_at < reports_end_time_utc_ca_on()
        AND free_bet_id::VARCHAR IS NOT NULL
    GROUP BY 1,2,3
)
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

 --positive resettled_sport_wagers = player cash balance increases
 -- DATA-8784 Moved resettled_sport_wagers_accounting to separate model
, resettled_sport_wagers AS (
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , bet_id
        , SUM(resettled_sport_wagers) AS resettled_sport_wagers
    FROM resettled_sport_wagers_accounting
    WHERE free_bet_id IS NULL
    GROUP BY 1,2,3
)


, resettled_sport_wagers_freebets AS (
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , bet_id
        , SUM(resettled_sport_wagers) AS resettled_sport_wagers_freebets
    FROM resettled_sport_wagers_accounting
    WHERE free_bet_id IS NOT NULL
    GROUP BY 1,2,3
)

-- Deprecated due to Vegas
-- , cancelled_bets AS (
--     SELECT DISTINCT
--         patron_id
--         , b.id AS bet_id
--         , closed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
--         , SUM(bet_amount_cents)/100.0 AS cancelled_bets
--     FROM ca_on_reports.max_vegas_bets AS b
--     WHERE b.status = 'cancelled'
--         AND b.free_bet_id::VARCHAR IS NULL
--         AND closed_at >= reports_start_time_utc_ca_on()
--         AND closed_at < reports_end_time_utc_ca_on()
--     GROUP BY 1,2,3
-- )

-- Deprecated due to Vegas
-- , cancelled_freebets AS (
--     SELECT DISTINCT
--         patron_id
--         , b.id AS bet_id
--         , closed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
--         , SUM(bet_amount_cents)/100.0 AS cancelled_freebets
--     FROM ca_on_reports.max_vegas_bets AS b
--     WHERE b.status = 'cancelled'
--         AND b.free_bet_id::VARCHAR IS NOT NULL
--         AND closed_at >= reports_start_time_utc_ca_on()
--         AND closed_at < reports_end_time_utc_ca_on()
--     GROUP BY 1,2,3
-- )

, voided_bets AS (
    SELECT DISTINCT
        patron_id
        , b.id AS bet_id
        , closed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , SUM(bet_amount_cents)/100.0 AS voided_bets
    FROM ca_on_reports.max_vegas_bets AS b
    WHERE b.status = 'voided'
        AND b.free_bet_id::VARCHAR IS NULL
        AND closed_at >= reports_start_time_utc_ca_on()
        AND closed_at < reports_end_time_utc_ca_on()
    GROUP BY 1,2,3
)

, voided_freebets AS (
    SELECT DISTINCT
        patron_id
        , b.id AS bet_id
        , closed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , SUM(bet_amount_cents)/100.0 AS voided_freebets
    FROM ca_on_reports.max_vegas_bets AS b
    WHERE b.status = 'voided'
        AND b.free_bet_id::VARCHAR IS NOT NULL
        AND closed_at >= reports_start_time_utc_ca_on()
        AND closed_at < reports_end_time_utc_ca_on()
    GROUP BY 1,2,3
)

-- , gaming_sessions_summary AS (
--     SELECT game_id,
--            COUNT(DISTINCT game_session_id) num_game_sessions,
--            GREATEST(SUM(EXTRACT(EPOCH FROM (session_end_time - GREATEST(session_start_time, reports_start_time_et_ca_on()))))/60.0, 1)::int total_gameplay_duration
--     FROM ca_on_reports.max_gaming_sessions
--     -- sessions that start and ends during the reporting period (most cases)
--     WHERE (session_start_time >= reports_start_time_utc_ca_on() AND session_end_time < reports_end_time_utc_ca_on())
--     -- sessions that started in the previous reporting period but still have wagers after midnight
--     -- TODO when calculating session time for these, do MAX(session_start_time, reports_start_time_et_ca_on()) - session_end_time
--     OR (session_start_time < reports_start_time_utc_ca_on() AND session_end_time > reports_start_time_utc_ca_on())
--     GROUP BY game_id
-- )
, gaming_sessions_summary_helper AS (
    SELECT game_id, casino_session_id
        , MIN(transaction_timestamp_utc) AS session_start_time
        , MAX(transaction_timestamp_utc) AS session_end_time
    FROM ca_on_reports.max_wager_and_game_info wgi
    LEFT JOIN ca_on_reports.max_patron_transactions pt ON wgi.patron_transaction_id = pt.id
    INNER JOIN ca_on_reports.max_identity_users_in_state u
    ON wgi.patron_id=u.patron_id AND u.is_tester IS FALSE
    WHERE wgi.type NOT IN ('rollback')
        AND pt._changed_at < reports_end_time_utc_ca_on()
        AND pt._changed_at >= (reports_start_time_utc_ca_on() - INTERVAL '1 DAY')
    GROUP BY 1,2
)
, gaming_sessions_summary AS (
    SELECT game_id, g.name
        , COUNT(DISTINCT casino_session_id) num_game_sessions
        , GREATEST(SUM(EXTRACT(EPOCH FROM (session_end_time - GREATEST(session_start_time, reports_start_time_utc_ca_on()))))/60.0, 1)::int total_gameplay_duration
    FROM gaming_sessions_summary_helper gs
    LEFT JOIN ca_on_reports.max_games g ON gs.game_id=g.id::varchar
    -- sessions that started and ended during the same reporting day
    WHERE (session_start_time >= reports_start_time_utc_ca_on() AND session_end_time < reports_end_time_utc_ca_on())
    -- sessions that started in the previous reporting period but still have wagers after midnight
    OR (session_start_time < reports_start_time_utc_ca_on() AND session_end_time > reports_start_time_utc_ca_on())
    GROUP BY 1,2
)

, dataset AS (
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , bet_id
        , NULL AS patron_transaction_id
        , 1 AS num_wagers
        , SUM(transfer_to_sports) AS wagers_game
        , 0.0 AS unsettled_wagers
        , 0.0 AS win_game
        , 0.0 AS promo_wagers_game
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS withdrawable_win
    FROM transfer_to_sports
    WHERE transaction_timestamp >= reports_start_time_et_ca_on()
    AND transaction_timestamp < reports_end_time_et_ca_on()
    GROUP BY 1,2,3,4
        UNION ALL
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , NULL AS bet_id
        , patron_transaction_id::text
        , 1 AS num_wagers
        , SUM(transfer_to_casino) AS wagers_game
        , 0.0 AS unsettled_wagers
        , 0.0 AS win_game
        , 0.0 AS promo_wagers_game
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS withdrawable_win
    FROM transfer_to_casino
    WHERE transaction_timestamp >= reports_start_time_et_ca_on()
    AND transaction_timestamp < reports_end_time_et_ca_on()
    GROUP BY 1,2,3,4
        UNION ALL
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , NULL AS bet_id
        , patron_transaction_id::text
        , -1 AS num_wagers
        , -1.0*SUM(transfer_to_casino_rollback) AS wagers_game
        , 0.0 AS unsettled_wagers
        , 0.0 AS win_game
        , 0.0 AS promo_wagers_game
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS withdrawable_win
    FROM transfer_to_casino_rollback
    WHERE transaction_timestamp >= reports_start_time_et_ca_on()
    AND transaction_timestamp < reports_end_time_et_ca_on()
    GROUP BY 1,2,3,4
        UNION ALL
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , NULL AS bet_id
        , patron_transaction_id::text
        , 0 AS num_wagers
        , 0.0 AS wagers_game
        , 0.0 AS unsettled_wagers
        , SUM(transfer_from_casino) AS win_game
        , 0.0 AS promo_wagers_game
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS withdrawable_win
    FROM jackpot_contributions
    WHERE transaction_timestamp >= reports_start_time_et_ca_on()
        AND transaction_timestamp < reports_end_time_et_ca_on()
    GROUP BY 1,2,3,4
        UNION ALL
    -- Deprecated due to Vegas
    -- SELECT DISTINCT
    --     transaction_timestamp
    --     , patron_id
    --     , bet_id AS bet_id
    --     , NULL AS patron_transaction_id
    --     , -1 AS num_wagers
    --     , -1.0*SUM(cancelled_bets) AS wagers_game
    --     , 0.0 AS unsettled_wagers
    --     , 0.0 AS win_game
    --     , 0.0 AS promo_wagers_game
    --     , 0.0 AS promo_unsettled_wagers
    --     , 0.0 AS withdrawable_win
    -- FROM cancelled_bets
    -- WHERE transaction_timestamp >= reports_start_time_et_ca_on()
    -- AND transaction_timestamp < reports_end_time_et_ca_on()
    -- GROUP BY 1,2,3,4
    --     UNION ALL
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , bet_id AS bet_id
        , NULL AS patron_transaction_id
        , -1 AS num_wagers
        , -1.0*SUM(voided_bets) AS wagers_game
        , 0.0 AS unsettled_wagers
        , 0.0 AS win_game
        , 0.0 AS promo_wagers_game
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS withdrawable_win
    FROM voided_bets
    WHERE transaction_timestamp >= reports_start_time_et_ca_on()
    AND transaction_timestamp < reports_end_time_et_ca_on()
    GROUP BY 1,2,3,4
        UNION ALL
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , bet_id AS bet_id
        , NULL AS patron_transaction_id
        , 0 AS num_wagers
        , 0.0 AS wagers_game
        , SUM(pending_sport_wagers) AS unsettled_wagers
        , 0.0 AS win_game
        , 0.0 AS promo_wagers_game
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS withdrawable_win
    FROM pending_sport_wagers
    WHERE transaction_timestamp >= reports_start_time_et_ca_on()
    AND transaction_timestamp < reports_end_time_et_ca_on()
    GROUP BY 1,2,3,4
        UNION ALL
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , bet_id
        , NULL AS patron_transaction_id
        , 0 AS num_wagers
        , 0.0 AS wagers_game
        , 0.0 AS unsettled_wagers
        , SUM(transfer_from_sports) AS win_game
        , 0.0 AS promo_wagers_game
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS withdrawable_win
    FROM transfer_from_sports
    WHERE transaction_timestamp >= reports_start_time_et_ca_on()
    AND transaction_timestamp < reports_end_time_et_ca_on()
    GROUP BY 1,2,3,4
        UNION ALL
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , NULL AS bet_id
        , patron_transaction_id::text
        , 0 AS num_wagers
        , 0.0 AS wagers_game
        , 0.0 AS unsettled_wagers
        , SUM(transfer_from_casino) AS win_game
        , 0.0 AS promo_wagers_game
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS withdrawable_win
    FROM transfer_from_casino
    WHERE transaction_timestamp >= reports_start_time_et_ca_on()
    AND transaction_timestamp < reports_end_time_et_ca_on()
    GROUP BY 1,2,3,4
        UNION ALL
    SELECT transaction_timestamp
        , patron_id
        , NULL AS bet_id
        , patron_transaction_id::text
        , 0 AS num_wagers
        , 0.0 AS wagers_game
        , 0.0 AS unsettled_wagers
        , SUM(transfer_from_casino) * (-1) AS win_game
        , 0.0 AS promo_wagers_game
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS withdrawable_win
    FROM jackpot_payout_cash
    WHERE transaction_timestamp >= reports_start_time_et_ca_on()
        AND transaction_timestamp < reports_end_time_et_ca_on()
    GROUP BY 1,2,3,4
        UNION ALL
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , bet_id
        , NULL AS patron_transaction_id
        , 0 AS num_wagers
        , 0.0 AS wagers_game
        , 0.0 AS unsettled_wagers
        , -1.0*SUM(resettled_sport_wagers) AS win_game
        , 0.0 AS promo_wagers_game
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS withdrawable_win
    FROM resettled_sport_wagers
    WHERE transaction_timestamp >= reports_start_time_et_ca_on()
    AND transaction_timestamp < reports_end_time_et_ca_on()
    GROUP BY 1,2,3,4
        UNION ALL
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , bet_id
        , NULL AS patron_transaction_id
        , 1 AS num_wagers
        , 0.0 AS wagers_game
        , 0.0 AS unsettled_wagers
        , 0.0 AS win_game
        , SUM(free_bets_wagered) AS promo_wagers_game
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS withdrawable_win
    FROM free_bets_wagered
    WHERE transaction_timestamp >= reports_start_time_et_ca_on()
    AND transaction_timestamp < reports_end_time_et_ca_on()
    GROUP BY 1,2,3,4
        UNION ALL
    -- Deprecated due to Vegas
    -- SELECT DISTINCT
    --     transaction_timestamp
    --     , patron_id
    --     , bet_id AS bet_id
    --     , NULL AS patron_transaction_id
    --     , -1 AS num_wagers
    --     , 0.0 AS wagers_game
    --     , 0.0 AS unsettled_wagers
    --     , 0.0 AS win_game
    --     , -1.0*SUM(cancelled_freebets) AS promo_wagers_game
    --     , 0.0 AS promo_unsettled_wagers
    --     , 0.0 AS withdrawable_win
    -- FROM cancelled_freebets
    -- WHERE transaction_timestamp >= reports_start_time_et_ca_on()
    -- AND transaction_timestamp < reports_end_time_et_ca_on()
    -- GROUP BY 1,2,3,4
    --     UNION ALL
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , bet_id AS bet_id
        , NULL AS patron_transaction_id
        , -1 AS num_wagers
        , 0.0 AS wagers_game
        , 0.0 AS unsettled_wagers
        , 0.0 AS win_game
        , -1.0*SUM(voided_freebets) AS promo_wagers_game
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS withdrawable_win
    FROM voided_freebets
    WHERE transaction_timestamp >= reports_start_time_et_ca_on()
    AND transaction_timestamp < reports_end_time_et_ca_on()
    GROUP BY 1,2,3,4
        UNION ALL
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , NULL AS bet_id
        , patron_transaction_id
        , 1 AS num_wagers
        , 0.0 AS wagers_game
        , 0.0 AS unsettled_wagers
        , 0.0 AS win_game
        , SUM(free_rounds_wagered) AS promo_wagers_game
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS withdrawable_win
    FROM free_rounds_wagered
    WHERE transaction_timestamp >= reports_start_time_et_ca_on()
    AND transaction_timestamp < reports_end_time_et_ca_on()
    GROUP BY 1,2,3,4
        UNION ALL
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , NULL AS bet_id
        , patron_transaction_id::varchar
        , -1 AS num_wagers
        , 0.0 AS wagers_game
        , 0.0 AS unsettled_wagers
        , 0.0 AS win_game
        , -1.0*SUM(free_rounds_wagered_rollback) AS promo_wagers_game
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS withdrawable_win
    FROM free_rounds_wagered_rollback
    WHERE transaction_timestamp >= reports_start_time_et_ca_on()
    AND transaction_timestamp < reports_end_time_et_ca_on()
    GROUP BY 1,2,3,4
        UNION ALL
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , bet_id
        , NULL AS patron_transaction_id
        , 0 AS num_wagers
        , 0.0 AS wagers_game
        , 0.0 AS unsettled_wagers
        , 0.0 AS win_game
        , 0.0 AS promo_wagers_game
        , SUM(pending_sport_wagers_freebets) AS promo_unsettled_wagers
        , 0.0 AS withdrawable_win
    FROM pending_sport_wagers_freebets
    WHERE transaction_timestamp >= reports_start_time_et_ca_on()
    AND transaction_timestamp < reports_end_time_et_ca_on()
    GROUP BY 1,2,3,4
        UNION ALL
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , bet_id
        , NULL AS patron_transaction_id
        , 0 AS num_wagers
        , 0.0 AS wagers_game
        , 0.0 AS unsettled_wagers
        , 0.0 AS win_game
        , 0.0 AS promo_wagers_game
        , 0.0 AS promo_unsettled_wagers
        , SUM(free_bets_payouts) AS withdrawable_win
    FROM free_bets_payouts
    WHERE transaction_timestamp >= reports_start_time_et_ca_on()
    AND transaction_timestamp < reports_end_time_et_ca_on()
    GROUP BY 1,2,3,4
        UNION ALL
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , bet_id
        , NULL AS patron_transaction_id
        , 0 AS num_wagers
        , 0.0 AS wagers_game
        , 0.0 AS unsettled_wagers
        , 0.0 AS win_game
        , 0.0 AS promo_wagers_game
        , 0.0 AS promo_unsettled_wagers
        , -1.0*SUM(resettled_sport_wagers_freebets) AS withdrawable_win
    FROM resettled_sport_wagers_freebets
    WHERE transaction_timestamp >= reports_start_time_et_ca_on()
    AND transaction_timestamp < reports_end_time_et_ca_on()
    GROUP BY 1,2,3,4
        UNION ALL
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , NULL AS bet_id
        , patron_transaction_id::text
        , 0 AS num_wagers
        , 0.0 AS wagers_game
        , 0.0 AS unsettled_wagers
        , 0.0 AS win_game
        , 0.0 AS promo_wagers_game
        , 0.0 AS promo_unsettled_wagers
        , SUM(free_rounds_payouts) AS withdrawable_win
    FROM free_rounds_payouts
    WHERE transaction_timestamp >= reports_start_time_et_ca_on()
    AND transaction_timestamp < reports_end_time_et_ca_on()
    GROUP BY 1,2,3,4
        UNION ALL
    SELECT transaction_timestamp
        , patron_id
        , NULL AS bet_id
        , patron_transaction_id::text
        , 0 AS num_wagers
        , 0.0 AS wagers_game
        , 0.0 AS unsettled_wagers
        , 0.0 AS win_game -- reverted DATA-9140
        , 0.0 AS promo_wagers_game
        , 0.0 AS promo_unsettled_wagers
        , SUM(free_rounds_payouts) * (-1) AS withdrawable_win
    FROM jackpot_payout_free
    WHERE transaction_timestamp >= reports_start_time_et_ca_on()
        AND transaction_timestamp < reports_end_time_et_ca_on()
    GROUP BY 1,2,3,4
)

SELECT
to_char(reports_start_time_et_ca_on(),'YYYYMMDD')  AS "GamingDay"
            , 'S100001A' AS "GamingSiteID"
            , (CASE WHEN d.bet_id IS NULL THEN 'Casino' ELSE 'Betting' END) AS "ProductCode"
            , (CASE WHEN d.bet_id IS NULL
                    THEN CASE WHEN g.game_provider_game_category='live' THEN 'Live Dealer'
                              WHEN g.game_provider_game_category='roulette' THEN 'Table'
                              WHEN g.game_provider_game_category='S' THEN 'Slots'
                              WHEN g.game_provider_game_category='V' THEN 'VideoPoker'
                              ELSE UPPER(LEFT(g.game_provider_game_category, 1))|| LOWER(SUBSTRING(g.game_provider_game_category, 2, LENGTH(g.game_provider_game_category))) END
                    ELSE 'Sports' END) AS "ProductSegment"
            , (CASE WHEN d.bet_id IS NULL THEN gs.name ELSE bm.event_type END) AS "Game"
            , (CASE WHEN d.bet_id IS NULL THEN NULL
                    WHEN bm.num_legs = 1 THEN 'Fixed Odds Single'
                    WHEN bm.num_legs > 1 THEN 'Fixed Odds Combo' END) AS "BetTypeA"
            , (CASE WHEN d.bet_id IS NULL THEN NULL
                    ELSE (CASE WHEN bm.num_live_legs=0 THEN 'Pre Event'
                               WHEN bm.num_live_legs=num_legs THEN 'Live Action' ELSE 'Both' END) END) AS "BetTypeB"
            , (CASE WHEN COUNT(DISTINCT d.bet_id)=0 THEN MAX(gs.num_game_sessions) END) AS "#ofGameSessions"
            , (CASE WHEN COUNT(DISTINCT d.bet_id)=0 THEN MAX(gs.total_gameplay_duration) END) AS "TotalGameplayDuration"
            , COUNT(DISTINCT (CASE WHEN num_wagers>0 THEN d.patron_id END)) "#ofUniquePlayers"
            , SUM(num_wagers) "#ofWagers"
            , SUM(d.wagers_game) AS "WagersGame"
            , SUM(d.unsettled_wagers) AS "UnsettledWagers"
            , SUM(d.win_game)::NUMERIC(38,2) AS "WinGame"
            , 0.0 AS "Fees"
            , SUM(d.wagers_game)-SUM(d.win_game)::NUMERIC(38,2) AS "NonAdjustedGGR"
            , SUM(d.promo_wagers_game) AS "PromoWagersGame"
            , SUM(d.promo_unsettled_wagers) AS "PromoUnsettledWagers"
            , 0.0 AS "PromoWinGame"
            , SUM(withdrawable_win)::NUMERIC(38,2) AS "WithdrawableWin"
            FROM dataset d
    LEFT JOIN ca_on_reports.max_wager_and_game_info wg
    ON wg.patron_transaction_id::varchar=d.patron_transaction_id
    LEFT JOIN ca_on_reports.bet_and_market_info bm
    ON bm.bet_id=d.bet_id
    LEFT JOIN gaming_sessions_summary gs
    ON wg.game_id=gs.game_id::varchar
    LEFT JOIN ca_on_reports.max_games g
    ON g.id::varchar = gs.game_id
    INNER JOIN ca_on_reports.max_identity_users_in_state mu
    ON mu.patron_id=d.patron_id and mu.is_tester IS FALSE
    WHERE transaction_timestamp >= reports_start_time_et_ca_on()
      AND transaction_timestamp < reports_end_time_et_ca_on()
    GROUP BY 1,2,3,4,5,6,7
    ORDER BY 1,2,3,4,5,6,7
