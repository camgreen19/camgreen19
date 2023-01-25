-- 1. CLEAN TABLES BY JOINING MAX ROWS IN DATE RANGE
-- FREE BETS
-- DATA-8784 Moved free_bets, free_bets_wagered to separate model

-- 2. FETCH REQUIRED COLUMNS FROM CLEANED TABLES
-- TRANSFER TO SPORTS
-- cash bets
WITH free_rounds_wagered as (
    SELECT DISTINCT
        pt._changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , pt.patron_id
        , wg.game_category AS category
        , w.patron_transaction_id
        , SUM(w.amount_cents)/100.0 AS free_rounds_wagered
    FROM ca_finance_reports.max_casino_wagers_all AS w
    JOIN ca_finance_reports.max_patron_transactions_all pt ON w.patron_transaction_id = pt.id
    LEFT JOIN ca_finance_reports.max_wager_and_game_info wg ON pt.id=wg.patron_transaction_id
    WHERE w.promo_engine_free_round_award_id IS NOT NULL
    AND pt._changed_at >= reports_start_time_utc_ca_finance()
    AND pt._changed_at < reports_end_time_utc_ca_finance()
    GROUP BY 1,2,3,4
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

, free_rounds_wagered_rollback as (
    SELECT DISTINCT
           pt._changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
         , wg.patron_id AS patron_id
         , r.patron_transaction_id AS patron_transaction_id
         , wg.game_category AS category
         , SUM(wg.amount_cents)/100.0 AS free_rounds_wagered_rollback
    FROM ca_finance_reports.max_casino_wager_rollbacks_all r
    JOIN ca_finance_reports.max_patron_transactions_all pt ON r.patron_transaction_id = pt.id
    LEFT JOIN ca_finance_reports.max_wager_and_game_info wg ON r.patron_transaction_id=wg.patron_transaction_id
    WHERE wg.type IN ('rollback') AND wg.promo_engine_free_round_award_id IS NOT NULL
    AND pt._changed_at >= reports_start_time_utc_ca_finance()
    AND pt._changed_at < reports_end_time_utc_ca_finance()
    GROUP BY 1,2,3,4
)

, free_bets as (
    SELECT DISTINCT
        COALESCE(p_vegas_bet_id,p_bet_id) AS bet_id
    FROM ca_edgebook_snapshot.max_accounting_tables_all
    WHERE p_type = 'sportsbook_free_bet'
)

, free_bets_wagered as(
    SELECT DISTINCT
        b.patron_id
        , b.bet_amount_cents
        , b.placed_at
        , b.id AS bet_id
    FROM ca_finance_reports.max_vegas_bets_all AS b
    WHERE b.free_bet_id::VARCHAR IS NOT NULL
        AND b.placed_at >= reports_start_time_utc_ca_finance()
        AND b.placed_at < reports_end_time_utc_ca_finance()
    )

, resettled_sport_wagers_accounting as (
    SELECT
        t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , a_patron_id AS patron_id
        , COALESCE(p_vegas_bet_id, p_bet_id) AS bet_id
        , COALESCE(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) as free_bet_id
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
    INNER JOIN ca_finance_reports.max_bet_states AS s
      ON p.bet_history_id = s.bet_history_id
    WHERE (p.p_type='sportsbook_bet_resettlement' OR
        p.p_type='sportsbook_bet_ungrade' OR
        p.p_type='sportsbook_bet_void')
        AND t_changed_at >= reports_start_time_utc_ca_finance()
        AND t_changed_at < reports_end_time_utc_ca_finance()
    GROUP BY 1,2,3,4,5,6,7,8,9
    )

, dr_fu_cash AS  (
    SELECT DISTINCT ledger_transaction_id
        , a_patron_id AS patron_id
        , a_type AS type
        , debit_cents
        , t_changed_at AS updated_at
        , COALESCE(p_vegas_bet_id, p_bet_id) AS bet_id
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS a
    LEFT JOIN free_bets AS fb
        ON COALESCE(a.p_vegas_bet_id, a.p_bet_id) = fb.bet_id
    WHERE p_type='sportsbook_bet'
        AND a_type='customer_liability'
        AND fb.bet_id IS NULL
        AND debit_cents>0
        AND t_changed_at >= reports_start_time_utc_ca_finance()
        AND t_changed_at < reports_end_time_utc_ca_finance()
)

, cr_cl_cash AS  (
    SELECT DISTINCT ledger_transaction_id
        , a_patron_id AS patron_id
        , a_type AS type
        , credit_cents
        , t_changed_at AS updated_at
        , COALESCE(p_vegas_bet_id, p_bet_id) AS bet_id
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS a
    LEFT JOIN free_bets AS fb
        ON COALESCE(a.p_vegas_bet_id, a.p_bet_id) = fb.bet_id
    WHERE p_type='sportsbook_bet'
        AND a_type='sportsbook_unsettled_bets'
        AND fb.bet_id IS NULL
        AND credit_cents>0
        AND t_changed_at >= reports_start_time_utc_ca_finance()
        AND t_changed_at < reports_end_time_utc_ca_finance()
)

, transfer_to_sports AS  (
    -- CASH BETS
    SELECT DISTINCT
        d.updated_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , d.patron_id
        , d.bet_id
        , SUM(debit_cents)/100.0 AS transfer_to_sports
    FROM dr_fu_cash AS d
    INNER JOIN cr_cl_cash AS c
        ON d.ledger_transaction_id=c.ledger_transaction_id
        AND debit_cents=credit_cents
    GROUP BY 1,2,3
        UNION ALL
    -- FREE BETS
    SELECT DISTINCT
        fbw.placed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , patron_id
        , bet_id
        , SUM(bet_amount_cents)/100.0 AS transfer_to_sports
    FROM free_bets_wagered AS fbw
    GROUP BY 1,2,3
)

-- Deprecated due to Vegas
-- CASH + FREE BETS CANCELLED
-- , cancelled_bets AS  (
--     SELECT DISTINCT
--         patron_id
--         , b.id AS bet_id
--         , closed_at AT time zone 'utc' AT time zone 'America/Toronto' AS gaming_date
--         , SUM(bet_amount_cents)/100.0 AS cancelled_amount
--     FROM ca_finance_reports.max_bets_all AS b
--     WHERE b.status = 'cancelled'
--         AND COALESCE(b.promo_engine_free_bet_id::VARCHAR,b.free_bet_id::VARCHAR) IS NULL
--     GROUP BY 1,2,3
-- )

-- CASH + FREE BETS VOIDED
, voided_bets AS  (
    SELECT DISTINCT
        patron_id
        , b.id AS bet_id
        , closed_at AT time zone 'utc' AT time zone 'America/Toronto' AS gaming_date
        , SUM(bet_amount_cents)/100.0 AS voided_amount
    FROM ca_finance_reports.max_vegas_bets_all AS b
    WHERE b.status = 'voided'
        AND b.free_bet_id::VARCHAR IS NULL
        AND b.closed_at >= reports_start_time_utc_ca_finance()
        AND b.closed_at < reports_end_time_utc_ca_finance()
    GROUP BY 1,2,3
)

-- -- cash + free bets resettled (regrades + ungrades)
   --positive resettled_sport_wagers = player cash balance increases
, resettled_sport_wagers AS  (
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , bet_id
        , SUM(resettled_sport_wagers) AS resettled_sport_wagers
    FROM resettled_sport_wagers_accounting
    --WHERE free_bet_id IS NULL
    GROUP BY 1,2,3
)

-- TRANSFER FROM SPORTS
, transfer_from_sports AS  (
    SELECT DISTINCT
        t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , a_patron_id AS patron_id
        , COALESCE(p_vegas_bet_id, p_bet_id) AS bet_id
        , (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) AS transfer_from_sports
    FROM ca_edgebook_snapshot.max_accounting_tables_all
    WHERE p_type IN ('sportsbook_cash_out_bet','sportsbook_payout','sportsbook_bet_lost')
         AND a_type='customer_liability'
         AND t_changed_at >= reports_start_time_utc_ca_finance()
         AND t_changed_at < reports_end_time_utc_ca_finance()
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
    INNER JOIN ca_finance_reports.max_bet_states AS s --check this
    ON p.bet_history_id = s.bet_history_id
    WHERE (p.p_type='sportsbook_bet_resettlement')
        AND a_type IN ('customer_liability')
        AND s.state IN ('refund','win')
        AND previous_ordinal = '1'
        AND s.previous_state='open'
        AND t_changed_at >= reports_start_time_utc_ca_finance()
        AND t_changed_at < reports_end_time_utc_ca_finance()
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
    INNER JOIN ca_finance_reports.max_bet_states AS s
    ON p.bet_history_id = s.bet_history_id
    WHERE (p.p_type='sportsbook_bet_resettlement')
        AND a_type IN ('customer_liability')
        AND COALESCE(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NULL
        AND s.state='refund'
        AND s.previous_state='open'
        AND previous_ordinal > '1'
    AND t_changed_at >= reports_start_time_utc_ca_finance()
        AND t_changed_at < reports_end_time_utc_ca_finance()
        AND p.gaming_state='CA-ON'
    GROUP BY 1,2,3
)

--new change
, dr_fu_cash_casino AS  (
    SELECT DISTINCT ledger_transaction_id
        , a_patron_id AS patron_id
        , a_type AS type
        , debit_cents
        , t_changed_at AS updated_at
        , p_id AS patron_transaction_id
        , a.gaming_state
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS a
    WHERE p_type in ('casino_wager')
        AND a_type='customer_liability'
        AND debit_cents>0
        AND t_changed_at >= reports_start_time_utc_ca_finance()
        AND t_changed_at < reports_end_time_utc_ca_finance()
)

, cr_cl_cash_casino AS  (
    SELECT DISTINCT ledger_transaction_id
        , a_patron_id AS patron_id
        , a_type AS type
        , credit_cents
        , t_changed_at AS updated_at
        , p_id AS patron_transaction_id
        , a.gaming_state
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS a
    WHERE p_type in ('casino_wager')
        AND a_type in ('casino_house_wins')
        AND credit_cents>0
        AND t_changed_at >= reports_start_time_utc_ca_finance()
        AND t_changed_at < reports_end_time_utc_ca_finance()
)

, transfer_to_casino AS  (
    -- CASH BETS
    SELECT DISTINCT
        d.updated_at AT time zone 'utc' AT time zone 'America/Toronto' AS wager_timestamp
        , d.patron_id
        , d.patron_transaction_id
        , w.game_category AS category
        , w.game_id
        , d.gaming_state
        , SUM(debit_cents)/100.0 AS transfer_to_casino
    FROM dr_fu_cash_casino AS d
    INNER JOIN cr_cl_cash_casino AS c
        ON d.ledger_transaction_id=c.ledger_transaction_id
        AND debit_cents=credit_cents
    LEFT JOIN ca_finance_reports.max_wager_and_game_info w ON w.patron_transaction_id=d.patron_transaction_id
    GROUP BY 1,2,3,4,5,6
)

, jackpot_contributions AS  (
SELECT DISTINCT j.remote_wallet_transaction_id AS patron_transaction_id
, w.wager_timestamp
, w.patron_id
, w.gaming_state
, j.free_round_award_id AS promo_engine_free_round_award_id
, g.name AS "Game"
, SUM(amount_precision) AS jackpot_contribution
FROM transfer_to_casino w
JOIN JACKPOTS_TEST j
    ON w.patron_transaction_id::VARCHAR = j.remote_wallet_transaction_id
    AND j.type = 'contribution'
LEFT JOIN ca_finance_reports.max_games g
    ON w.game_id = g.id::VARCHAR
GROUP BY 1,2,3,4,5,6
)


-- , casino_free_wagers AS  (
--     SELECT DISTINCT a.gaming_state
--         , a.t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS wager_timestamp
--         , a.patron_transaction_id
--         , a_patron_id as patron_id
--         , p.promo_engine_free_round_award_id
--         , SUM(p.amount_cents)/100.0 AS casino_wagered
--     FROM ca_edgebook_snapshot.max_accounting_tables_all a
--     LEFT JOIN ca_finance_reports.max_casino_wagers_all p ON p.patron_transaction_id=a.patron_transaction_id
--     WHERE p_type IN ('casino_free_wager')
--          AND a_type='casino_bonus_expense'
--          AND promo_engine_free_round_award_id IS NOT NULL
--     GROUP BY 1,2,3,4,5
-- )

--new change
, transfer_to_casino_rollback AS  (
    SELECT DISTINCT
           pt._changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS wager_timestamp
         , wg.patron_id AS patron_id
         , r.patron_transaction_id AS patron_transaction_id
         , wg.game_category AS category
         , SUM(wg.amount_cents)/100.0 AS transfer_to_casino_rollback
    FROM ca_finance_reports.max_casino_wager_rollbacks_all r
    JOIN ca_finance_reports.max_patron_transactions_all pt ON r.patron_transaction_id = pt.id
    LEFT JOIN ca_finance_reports.max_wager_and_game_info wg ON r.patron_transaction_id=wg.patron_transaction_id
    WHERE wg.type IN ('rollback') AND wg.promo_engine_free_round_award_id IS NULL
      AND pt._changed_at >= reports_start_time_utc_ca_finance()
      AND pt._changed_at < reports_end_time_utc_ca_finance()
    GROUP BY 1,2,3,4
)


, casino_payouts AS  (
    SELECT DISTINCT a.gaming_state
        , t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS payout_timestamp
        , a_patron_id AS patron_id
        , p_id AS patron_transaction_id
        , p.promo_engine_free_round_award_id
        , a.parent_id
        , (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) AS casino_payout_amount
    FROM ca_edgebook_snapshot.max_accounting_tables_all a
    LEFT JOIN ca_finance_reports.max_casino_payouts_all p ON p.patron_transaction_id=a.patron_transaction_id
    WHERE p_type IN ('casino_payout')
         AND a_type IN ('customer_liability')
         AND t_changed_at >= reports_start_time_utc_ca_finance()
         AND t_changed_at < reports_end_time_utc_ca_finance()
    GROUP BY 1,2,3,4,5,6
)

-- Jackpot contributions allocated as payouts
, casino_payouts_jackpot_contributions AS  (
    SELECT DISTINCT gaming_state
        , wager_timestamp AS payout_timestamp
        , patron_id
        , patron_transaction_id
        , promo_engine_free_round_award_id
        , patron_transaction_id AS parent_id -- keep consistent column name for UNION
        , jackpot_contribution
    FROM jackpot_contributions
)

, casino_payouts_jackpot_wins AS  (
    SELECT DISTINCT gaming_state
        , payout_timestamp
        , f.patron_id
        , f.patron_transaction_id
        , f.promo_engine_free_round_award_id
        , t.remote_wallet_transaction_id AS parent_id -- keep consistent column name for UNION
        , t.amount_precision AS jackpot_win
    FROM casino_payouts f
    JOIN JACKPOTS_TEST t
        ON f.patron_transaction_id::VARCHAR = t.remote_wallet_transaction_id
    WHERE t.type = 'win'
)

, casino_dataset AS  (
    SELECT 'CA-ON' AS gaming_state
    , DATE_TRUNC('day',wager_timestamp)::DATE AS gaming_date
    , DATE_TRUNC('seconds',wager_timestamp)::TIMESTAMP AS transaction_timestamp
    , patron_transaction_id as wager_id
    , patron_transaction_id as patron_transaction_id
    , patron_id as patron_id
    , transfer_to_casino as transfer_to_casino
    , 0.0 as rollback_casino_wagers
    , 0.0 as transfer_from_casino
    , 0.0 as jackpot_contribution
    , 0.0 as jackpot_win
    FROM transfer_to_casino
    -- SELECT 'CA-ON' AS gaming_state
    -- , DATE_TRUNC('day',wager_timestamp)::DATE AS gaming_date
    -- , DATE_TRUNC('seconds',wager_timestamp)::TIMESTAMP AS transaction_timestamp
    -- , patron_transaction_id as wager_id
    -- , patron_transaction_id as patron_transaction_id
    -- , patron_id as patron_id
    -- , casino_wagered as transfer_to_casino
    -- , 0.0 as rollback_casino_wagers
    -- , 0.0 as transfer_from_casino
    -- FROM casino_cash_wagers
    UNION ALL
    --free_rounds_wagered
    SELECT 'CA-ON' AS gaming_state
    , DATE_TRUNC('day',transaction_timestamp)::DATE AS gaming_date
    , DATE_TRUNC('seconds',transaction_timestamp)::TIMESTAMP AS transaction_timestamp
    , patron_transaction_id as wager_id
    , patron_transaction_id as patron_transaction_id
    , patron_id as patron_id
    , free_rounds_wagered as transfer_to_casino
    , 0.0 as rollback_casino_wagers
    , 0.0 as transfer_from_casino
    , 0.0 as jackpot_contribution
    , 0.0 as jackpot_win
    FROM free_rounds_wagered
    -- SELECT gaming_state
    -- , DATE_TRUNC('day',wager_timestamp)::DATE AS gaming_date
    -- , DATE_TRUNC('seconds',wager_timestamp)::TIMESTAMP AS transaction_timestamp
    -- , patron_transaction_id as wager_id
    -- , patron_transaction_id as patron_transaction_id
    -- , patron_id as patron_id
    -- , casino_wagered as transfer_to_casino
    -- , 0.0 as rollback_casino_wagers
    -- , 0.0 as transfer_from_casino
    -- FROM casino_free_wagers
    UNION ALL
    SELECT 'CA-ON' AS gaming_state
    , DATE_TRUNC('day',wager_timestamp)::DATE AS gaming_date
    , DATE_TRUNC('seconds',wager_timestamp)::TIMESTAMP AS transaction_timestamp
    , patron_transaction_id as wager_id
    , patron_transaction_id as patron_transaction_id
    , patron_id as patron_id
    , 0.0 as transfer_to_casino
    , transfer_to_casino_rollback as rollback_casino_wagers
    , 0.0 as transfer_from_casino
    , 0.0 as jackpot_contribution
    , 0.0 as jackpot_win
    FROM transfer_to_casino_rollback
    UNION ALL
    SELECT 'CA-ON' AS gaming_state
    , DATE_TRUNC('day',transaction_timestamp)::DATE AS gaming_date
    , DATE_TRUNC('seconds',transaction_timestamp)::TIMESTAMP AS transaction_timestamp
    , patron_transaction_id as wager_id
    , patron_transaction_id as patron_transaction_id
    , patron_id as patron_id
    , 0.0 as transfer_to_casino
    , free_rounds_wagered_rollback as rollback_casino_wagers
    , 0.0 as transfer_from_casino
    , 0.0 as jackpot_contribution
    , 0.0 as jackpot_win
    FROM free_rounds_wagered_rollback
    UNION ALL
    -- SELECT gaming_state
    -- , DATE_TRUNC('day',rollback_timestamp)::DATE AS gaming_date
    -- , DATE_TRUNC('seconds',rollback_timestamp)::TIMESTAMP AS transaction_timestamp
    -- , parent_id as wager_id
    -- , patron_transaction_id as patron_transaction_id
    -- , patron_id as patron_id
    -- , 0.0 as transfer_to_casino
    -- , casino_rollback_amount as rollback_casino_wagers
    -- , 0.0 as transfer_from_casino
    -- FROM casino_rollbacks
    -- UNION ALL
    SELECT gaming_state
    , DATE_TRUNC('day',payout_timestamp)::DATE AS gaming_date
    , DATE_TRUNC('seconds',payout_timestamp)::TIMESTAMP AS transaction_timestamp
    , parent_id as wager_id
    , patron_transaction_id as patron_transaction_id
    , patron_id as patron_id
    , 0.0 as transfer_to_casino
    , 0.0 as rollback_casino_wagers
    , casino_payout_amount as transfer_from_casino
    , 0.0 as jackpot_contribution
    , 0.0 as jackpot_win
    FROM casino_payouts
    UNION ALL
     SELECT gaming_state
    , DATE_TRUNC('day',payout_timestamp)::DATE AS gaming_date
    , DATE_TRUNC('seconds',payout_timestamp)::TIMESTAMP AS transaction_timestamp
    , parent_id as wager_id
    , patron_transaction_id as patron_transaction_id
    , patron_id as patron_id
    , 0.0 as transfer_to_casino
    , 0.0 as rollback_casino_wagers
    , 0.0 as transfer_from_casino
    , jackpot_contribution
    , 0.0 as jackpot_win
    FROM casino_payouts_jackpot_contributions
    UNION ALL
     SELECT gaming_state
    , DATE_TRUNC('day',payout_timestamp)::DATE AS gaming_date
    , DATE_TRUNC('seconds',payout_timestamp)::TIMESTAMP AS transaction_timestamp
    , parent_id as wager_id
    , patron_transaction_id as patron_transaction_id
    , patron_id as patron_id
    , 0.0 as transfer_to_casino
    , 0.0 as rollback_casino_wagers
    , 0.0 as transfer_from_casino
    , 0.0 as jackpot_contribution
    , jackpot_win
    FROM casino_payouts_jackpot_wins
)

-- COMBINE INTO REQUIRED REPORT COLUMNS
, dataset AS  (
    -- CASH BETS
    SELECT DISTINCT
        transaction_timestamp AS gaming_date
        , bet_id
        , patron_id
        , SUM(transfer_to_sports) AS transfer_to_sports
        , 0.0 AS cancelled_sport_wagers
        , 0.0 AS voided_sport_wagers
        , 0.0 AS resettled_sport_wagers
        , 0.0 AS transfer_from_sports
    FROM transfer_to_sports
    GROUP BY 1,2,3
        UNION ALL
    -- Deprecated due to Vegas
    -- -- CANCELLED SPORTS WAGERS
    -- SELECT DISTINCT
    --     gaming_date
    --     , bet_id
    --     , patron_id
    --     , 0.0 AS transfer_to_sports
    --     , cancelled_amount AS cancelled_sport_wagers
    --     , 0.0 AS voided_sport_wagers
    --     , 0.0 AS resettled_sport_wagers
    --     , 0.0 AS transfer_from_sports
    -- FROM cancelled_bets
    --     UNION ALL
    -- VOIDED SPORTS WAGERS
    SELECT DISTINCT
        gaming_date
        , bet_id
        , patron_id
        , 0.0 AS transfer_to_sports
        , 0.0 AS cancelled_sport_wagers
        , voided_amount AS voided_sport_wagers
        , 0.0 AS resettled_sport_wagers
        , 0.0 AS transfer_from_sports
    FROM voided_bets
        UNION ALL
    -- RESETTLED SPORT WAGERS
    SELECT DISTINCT
        transaction_timestamp AS gaming_date
        , bet_id
        , patron_id
        , 0.0 AS transfer_to_sports
        , 0.0 AS cancelled_sport_wagers
        , 0.0 AS voided_sport_wagers
        , SUM(resettled_sport_wagers) AS resettled_sport_wagers
        , 0.0 AS transfer_from_sports
    FROM resettled_sport_wagers
    GROUP BY 1,2,3
        UNION ALL
    -- TRANSFER FROM SPORTS
    SELECT DISTINCT
        transaction_timestamp AS gaming_date
        , bet_id
        , patron_id
        , 0.0 AS transfer_to_sports
        , 0.0 AS cancelled_sport_wagers
        , 0.0 AS voided_sport_wagers
        , 0.0 AS resettled_sport_wagers
        , SUM(transfer_from_sports) AS transfer_from_sports
    FROM transfer_from_sports
    GROUP BY 1,2,3
)

, final_revenue AS  (
    SELECT 'CA-ON' AS gaming_state
        , DATE_TRUNC('day',gaming_date) AS gaming_date
        , 'Sports' AS product
        , DATE_TRUNC('seconds',gaming_date) AS transaction_time
        , d.bet_id AS wager_id
        , NULL AS patron_transaction_id
        , d.patron_id AS account_number
        , CASE WHEN isu.is_tester THEN 'Test' ELSE 'Real' END AS account_designation
        , bm.event_type AS wager_type
        , bm.bet_type AS wager_description
        , CASE WHEN (bm.event_description = '' OR bm.event_description IS NULL) AND LOWER(bm.bet_type) NOT IN ('parlay','parlay_plus') THEN bm.market_type ELSE bm.event_description END AS event_description          -- Parlay Plus Change
        , DATE_TRUNC('seconds',bm.event_timestamp AT time zone 'utc' AT time zone 'America/Toronto') AS event_date
        , bm.selection
        , SUM(transfer_to_sports) AS wager_placed_amount
        , -1.0*SUM(transfer_from_sports) AS wager_paid_amount
        , -1.0*SUM(voided_sport_wagers) AS void_wager_amount
        , -1.0*SUM(cancelled_sport_wagers) AS cancelled_wager_amount
        , SUM(resettled_sport_wagers) AS resettled_wager_adjustment
        , 0.00 AS rollback_wager_amount
        , 0.00 AS jackpot_contribution
        , 0.00 AS jackpot_win
    FROM dataset AS d
    LEFT JOIN ca_finance_reports.bet_and_market_info AS bm
        USING(bet_id)
    INNER JOIN ca_finance_reports.max_identity_users AS isu
        ON d.patron_id = isu.patron_id
    WHERE gaming_date >= reports_start_time_et_ca_finance()
        AND gaming_date < reports_end_time_et_ca_finance()
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
    UNION ALL
    SELECT gaming_state
    , gaming_date
    , 'Casino' AS product
    , transaction_timestamp AS transaction_time
    , wager_id::VARCHAR AS wager_id
    , wg.patron_transaction_id::TEXT AS patron_transaction_id
    , d.patron_id AS account_number
    , CASE WHEN isu.is_tester THEN 'Test' ELSE 'Real' END AS account_designation
    , CASE WHEN wg.game_category='live' THEN 'Live Dealer'
                          WHEN wg.game_category='roulette' THEN 'Table'
                          ELSE UPPER(LEFT(wg.game_category, 1))|| LOWER(SUBSTRING(wg.game_category, 2, LENGTH(wg.game_category))) END AS wager_type
    , 'Casino Wager' AS wager_description
    , wg.game_name AS event_description
    , NULL AS event_date
    , NULL AS selection
    , transfer_to_casino::NUMERIC(38,2) as wager_placed_amount
    , (-1.0*transfer_from_casino)::NUMERIC(38,2) AS wager_paid_amount
    , 0.00 AS void_wager_amount
    , 0.00 AS cancelled_wager_amount
    , 0.00 AS resettled_wager_adjustment
    , (-1.0*rollback_casino_wagers)::NUMERIC(38,2)  AS rollback_wager_amount
    , (-1.0*jackpot_contribution) AS jackpot_contribution
    , jackpot_win AS jackpot_win --deduct jackpot payout, contributions already counted as payout
FROM casino_dataset d
JOIN ca_finance_reports.max_identity_users AS isu
    ON d.patron_id = isu.patron_id
LEFT JOIN ca_finance_reports.max_wager_and_game_info wg
    ON wg.patron_transaction_id::VARCHAR = d.patron_transaction_id::VARCHAR
WHERE gaming_date >= reports_start_time_et_ca_finance()
        AND gaming_date < reports_end_time_et_ca_finance()
ORDER BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)
SELECT
    gaming_state AS "Gaming State"
    , gaming_date::DATE AS "Gaming Date"
    , product AS "Product"
    , transaction_time::TIMESTAMP AS "Transaction Time"
    , wager_id::VARCHAR AS "Wager ID"
    , patron_transaction_id AS "Patron Transaction ID"
    , account_number::VARCHAR AS "Account Number"
    , account_designation::VARCHAR AS "Account Designation"
    , wager_type::VARCHAR AS "Wager Type"
    , wager_description::VARCHAR AS "Wager Description"
    , event_description::VARCHAR AS "Event Description"
    , event_date::TIMESTAMP AS "Event Date"
    , selection::VARCHAR AS "Selection"
    , SUM(wager_placed_amount::NUMERIC(38,2)) AS "Wager Placed Amount"
    , SUM(wager_paid_amount::NUMERIC(38,2)) AS "Wager Paid Amount"
    , SUM(void_wager_amount::NUMERIC(38,2)) AS "Void Wager Amount"
    , SUM(cancelled_wager_amount::NUMERIC(38,2)) AS "Cancelled Wager Amount"
    , SUM(resettled_wager_adjustment::NUMERIC(38,2)) AS "Resettled Wager Adjustment"
    , SUM(rollback_wager_amount::NUMERIC(38,2)) AS "Rollback Wager Amount"
    , SUM(jackpot_contribution) AS "Jackpot Contributions and Reseeds"
    , SUM(jackpot_win) AS "Jackpot Win Payouts"
    , SUM(wager_placed_amount +
      wager_paid_amount +
      void_wager_amount +
      cancelled_wager_amount +
      resettled_wager_adjustment
      + rollback_wager_amount
      + jackpot_contribution
      + jackpot_win
      )
      AS "Gross Revenue"
FROM final_revenue
WHERE (wager_placed_amount!=0 OR
      wager_paid_amount!=0 OR
      void_wager_amount!=0 OR
      cancelled_wager_amount!=0 OR
      rollback_wager_amount != 0 OR
      --added IS NOT NULL instead of != 0 to include $0 resettlements
      --this makes the active_users report tie with financial reporting
      resettled_wager_adjustment IS NOT NULL)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
ORDER BY 1,2,3,4,5,6,7,8,9,10,11
