WITH free_bets_wagered AS (
    SELECT b.patron_id
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
    JOIN ca_edgebook_snapshot.max_accounting_tables_all a  ON b.id = COALESCE(a.p_vegas_bet_id, a.p_bet_id)
    WHERE b.free_bet_id::VARCHAR IS NOT NULL
    AND a.t_changed_at >= reports_start_time_utc_ca_on()
    AND  a.t_changed_at < reports_end_time_utc_ca_on()
    AND a.gaming_state='CA-ON'
)

, free_rounds_wagered AS (
    SELECT pt._changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , pt.patron_id
        , wg.game_category AS category
        , w.patron_transaction_id
        , SUM(w.amount_cents)/100.0 AS free_rounds_wagered
    FROM ca_on_reports.max_casino_wagers AS w
    JOIN ca_on_reports.max_patron_transactions pt ON pt.id=w.patron_transaction_id
    LEFT JOIN ca_on_reports.max_wager_and_game_info wg ON pt.id::varchar=wg.patron_transaction_id::varchar
    WHERE w.promo_engine_free_round_award_id IS NOT NULL
    AND pt._changed_at >= reports_start_time_utc_ca_on()
    AND pt._changed_at < reports_end_time_utc_ca_on()
    GROUP BY 1,2,3,4
)

, free_rounds_wagered_rollback AS (
    SELECT pt._changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
         , wg.patron_id AS patron_id
         , r.patron_transaction_id AS patron_transaction_id
         , wg.game_category AS category
         , SUM(wg.amount_cents)/100.0 AS free_rounds_wagered_rollback
    FROM ca_on_reports.max_casino_wager_rollbacks r
    JOIN ca_on_reports.max_patron_transactions pt ON pt.id=r.patron_transaction_id
    LEFT JOIN ca_on_reports.max_wager_and_game_info wg ON r.patron_transaction_id=wg.patron_transaction_id
    WHERE wg.type IN ('rollback') AND wg.promo_engine_free_round_award_id IS NOT NULL
    AND pt._changed_at >= reports_start_time_utc_ca_on()
    AND pt._changed_at < reports_end_time_utc_ca_on()
    GROUP BY 1,2,3,4
)

, free_bets_payouts AS (
        -- cash payouts
    SELECT t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , a_patron_id AS patron_id
        , COALESCE(p_vegas_bet_id, p_bet_id) AS bet_id
        , (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) AS free_bets_payouts
    FROM ca_edgebook_snapshot.max_accounting_tables_all a
    LEFT JOIN free_bets fb ON COALESCE(a.p_vegas_bet_id, a.p_bet_id) = fb.bet_id
    WHERE p_type IN ('sportsbook_cash_out_bet','sportsbook_payout','sportsbook_bet_lost')
        AND a_type='customer_liability'
        AND fb.bet_id IS NOT NULL -- Winnings only from freebets
        AND t_changed_at >= reports_start_time_utc_ca_on()
        AND t_changed_at < reports_end_time_utc_ca_on()
        AND a.gaming_state='CA-ON'
    GROUP BY 1,2,3
)

, free_rounds_payouts AS (
    SELECT pt.patron_id
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

-- DATA-9000
-- jackpot payout for free round
, jackpot_payout_free AS (
    SELECT DISTINCT f.transaction_timestamp
        , f.patron_id
        , f.patron_transaction_id
        , t.amount_precision AS free_round_payout
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
        AND fb.bet_id IS NULL
        AND debit_cents>0
        AND t_changed_at >= reports_start_time_utc_ca_on()
        AND t_changed_at < reports_end_time_utc_ca_on()
        AND a.gaming_state='CA-ON'
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
        AND fb.bet_id IS NULL
        AND credit_cents>0
        AND t_changed_at >= reports_start_time_utc_ca_on()
        AND t_changed_at < reports_end_time_utc_ca_on()
        AND a.gaming_state='CA-ON'
)

, transfer_to_sports AS (
    -- CASH BETS
    SELECT d.updated_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
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
    WHERE p_type in ('casino_wager')
        AND a_type='customer_liability'
        AND debit_cents>0
        AND t_changed_at >= reports_start_time_utc_ca_on()
        AND t_changed_at < reports_end_time_utc_ca_on()
        AND a.gaming_state='CA-ON'
)

, cr_cl_cash_casino AS (
    SELECT DISTINCT ledger_transaction_id
        , a_patron_id AS patron_id
        , a_type AS type
        , credit_cents
        , t_changed_at AS updated_at
        , p_id AS patron_transaction_id_id
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS a
    WHERE p_type in ('casino_wager')
        AND a_type in ('casino_house_wins')
        AND credit_cents>0
        AND t_changed_at >= reports_start_time_utc_ca_on()
        AND t_changed_at < reports_end_time_utc_ca_on()
        AND a.gaming_state='CA-ON'
)

, transfer_to_casino AS (
    -- CASH BETS
    SELECT d.updated_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , d.patron_id
        , d.patron_transaction_id AS patron_transaction_id
        , wg.game_category AS category
        , SUM(debit_cents)/100.0 AS transfer_to_casino
    FROM dr_fu_cash_casino AS d
    INNER JOIN cr_cl_cash_casino AS c
        ON d.ledger_transaction_id=c.ledger_transaction_id
        AND debit_cents=credit_cents
    LEFT JOIN ca_on_reports.max_wager_and_game_info wg
    ON wg.patron_transaction_id::varchar=d.patron_transaction_id::varchar
    GROUP BY 1,2,3,4
)

, transfer_to_casino_rollback AS (
    SELECT pt._changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
         , wg.patron_id AS patron_id
         , r.patron_transaction_id AS patron_transaction_id
         , wg.game_category AS category
         , SUM(wg.amount_cents)/100.0 AS transfer_to_casino_rollback
    FROM ca_on_reports.max_casino_wager_rollbacks r
    JOIN ca_on_reports.max_patron_transactions pt ON r.patron_transaction_id = pt.id
    LEFT JOIN ca_on_reports.max_wager_and_game_info wg ON r.patron_transaction_id=wg.patron_transaction_id
    WHERE wg.type IN ('rollback') AND wg.promo_engine_free_round_award_id IS NULL
        AND pt._changed_at >= reports_start_time_utc_ca_on()
        AND pt._changed_at < reports_end_time_utc_ca_on()
    GROUP BY 1,2,3,4
)

, transfer_from_sports AS (
    -- cash payouts
    SELECT t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
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
        AND a.gaming_state='CA-ON'
    GROUP BY 1,2,3

    UNION ALL

    SELECT t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
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

    SELECT t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
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
    -- cash payouts
    SELECT t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
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
        AND a.gaming_state='CA-ON'
    GROUP BY 1,2,3
)

, pending_sport_wagers AS (
    SELECT reports_start_time_et_ca_on()::TIMESTAMP AS transaction_timestamp
        , patron_id
        , id AS bet_id
        , SUM(bet_amount_cents)/100.0 AS pending_sport_wagers
    FROM ca_on_reports.max_vegas_bets b
    WHERE status='open'
        AND placed_at < reports_end_time_utc_ca_on()
        AND b.free_bet_id::VARCHAR IS NULL
    GROUP BY 1,2,3
)

, pending_sport_wagers_freebets AS (
    SELECT reports_start_time_et_ca_on()::TIMESTAMP AS transaction_timestamp
        , patron_id
        , id AS bet_id
        , SUM(bet_amount_cents)/100.0 AS pending_sport_wagers_freebets
    FROM ca_on_reports.max_vegas_bets b
    WHERE status='open'
        AND placed_at < reports_end_time_utc_ca_on()
        AND b.free_bet_id::VARCHAR IS NOT NULL
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

, resettled_sport_wagers AS (
    SELECT transaction_timestamp
        , patron_id
        , bet_id
        , SUM(resettled_sport_wagers) AS resettled_sport_wagers
    FROM resettled_sport_wagers_accounting
    WHERE free_bet_id IS NULL
    GROUP BY 1,2,3
)

, resettled_sport_wagers_freebets AS (
    SELECT transaction_timestamp
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
--         , SUM(bet_amount_cents)/100.0 AS cancelled_amount
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
--         , SUM(bet_amount_cents)/100.0 AS cancelled_amount
--     FROM ca_on_reports.max_vegas_bets AS b
--     WHERE b.status = 'cancelled'
--         AND b.free_bet_id::VARCHAR IS NOT NULL
--         AND closed_at >= reports_start_time_utc_ca_on()
--         AND closed_at < reports_end_time_utc_ca_on()
--     GROUP BY 1,2,3
-- )

, voided_bets AS (
    SELECT patron_id
        , b.id AS bet_id
        , closed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , SUM(bet_amount_cents)/100.0 AS voided_amount
    FROM ca_on_reports.max_vegas_bets AS b
    WHERE b.status = 'voided'
        AND b.free_bet_id::VARCHAR IS NULL
        AND closed_at >= reports_start_time_utc_ca_on()
        AND closed_at < reports_end_time_utc_ca_on()
    GROUP BY 1,2,3
)

, voided_freebets AS (
    SELECT patron_id
        , b.id AS bet_id
        , closed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , SUM(bet_amount_cents)/100.0 AS voided_amount
    FROM ca_on_reports.max_vegas_bets AS b
    WHERE b.status = 'voided'
        AND b.free_bet_id::VARCHAR IS NOT NULL
        AND closed_at >= reports_start_time_utc_ca_on()
        AND closed_at < reports_end_time_utc_ca_on()
    GROUP BY 1,2,3
)

-- OPENING CASH BALANCE
, opening_cash_balances_accounting AS (
    SELECT
        a_patron_id AS patron_id
        , (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) AS opening_cash_balance
    FROM ca_edgebook_snapshot.max_accounting_tables_all
    WHERE a_type='customer_liability' AND t_changed_at < reports_start_time_utc_ca_on ()
    AND gaming_state='CA-ON'
    GROUP BY 1
)

, opening_cash_balances AS (
SELECT
    u.patron_id
    , COALESCE(b.opening_cash_balance,0.0) AS opening_cash_balance
FROM ca_on_reports.min_identity_users_in_state AS u
LEFT JOIN opening_cash_balances_accounting AS b
    ON u.patron_id = b.patron_id)

-- DATA-8784 opening cash balance CTE moved to separate model
-- , opening_cash_balances_accounting AS (
--     SELECT
--         a_patron_id AS patron_id
--         , (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) AS opening_cash_balance
--     FROM ca_edgebook_snapshot.max_accounting_tables_all
--     WHERE a_type='customer_liability' AND t_changed_at < reports_start_time_utc_ca_on ()
--     AND gaming_state='CA-ON'
--     GROUP BY 1
-- )

-- , opening_cash_balances AS (
--     SELECT
--         u.patron_id
--         , COALESCE(b.opening_cash_balance,0.0) AS opening_cash_balance
--     FROM ca_on_reports.min_identity_users_in_state AS u
--     LEFT JOIN opening_cash_balances_accounting AS b
--         ON u.patron_id = b.patron_id
-- )

, deposits AS (
    -- deposit_amount
    SELECT t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , a_patron_id AS patron_id
        , (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) AS patron_cash_deposits
    FROM ca_edgebook_snapshot.max_accounting_tables_all
    WHERE p_type='deposit'
        AND a_type='customer_liability'
        AND t_changed_at >= reports_start_time_utc_ca_on()
        AND t_changed_at < reports_end_time_utc_ca_on()
        AND gaming_state='CA-ON'
    GROUP BY 1,2
        UNION ALL
    -- wire transfer deposits
    SELECT t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , a_patron_id AS patron_id
        , (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) AS patron_cash_deposits
    FROM ca_edgebook_snapshot.max_accounting_tables_all
    WHERE (p_type='amendment' OR p_type='amendment_rg')
         AND adjustment_reason = '9998'
         AND adjustment_type = '"cash_deposit"'
         AND a_type='customer_liability'
         AND t_changed_at >= reports_start_time_utc_ca_on()
         AND t_changed_at < reports_end_time_utc_ca_on()
         AND gaming_state = 'CA-ON'
    GROUP BY 1,2
        UNION ALL
    -- REGION TRANSFER IN
    SELECT
        t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , a_patron_id
        , (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) AS patron_cash_deposits
    FROM ca_edgebook_snapshot.max_accounting_tables_all
    WHERE p_type='region_transfer_in'
        AND a_type='customer_liability'
        AND t_changed_at >= reports_start_time_utc_ca_on()
        AND t_changed_at < reports_end_time_utc_ca_on()
        AND gaming_state='CA-ON'
    GROUP BY 1,2
)

-- WITHDRAWALS
, dr_withdrawal_initiated AS (
    SELECT DISTINCT ledger_transaction_id
        , patron_transaction_id
        , a_patron_id AS patron_id
        , a_type
        , debit_cents
        , t_changed_at AS updated_at
    FROM ca_edgebook_snapshot.max_accounting_tables_all
    WHERE a_type='customer_liability'
        AND t_type='withdrawal'
        AND debit_cents>0
        AND t_changed_at >= reports_start_time_utc_ca_on()
        AND t_changed_at < reports_end_time_utc_ca_on()
        AND gaming_state='CA-ON'
)

, cr_withdrawal_initiated AS (
    SELECT DISTINCT ledger_transaction_id
        , patron_transaction_id
        , a_patron_id AS patron_id
        , a_type
        , credit_cents
        , t_changed_at AS updated_at
    FROM ca_edgebook_snapshot.max_accounting_tables_all
    WHERE a_type='clearing_withdrawals'
        AND t_type='withdrawal'
        AND credit_cents>0
        AND t_changed_at >= reports_start_time_utc_ca_on()
        AND t_changed_at < reports_end_time_utc_ca_on()
        AND gaming_state='CA-ON'
)

, withdrawals AS (
    -- withdrawals initiated
    SELECT
        d.updated_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , d.patron_id
        , SUM(debit_cents)/100.0 AS patron_withdrawals
    FROM dr_withdrawal_initiated AS d
    INNER JOIN cr_withdrawal_initiated AS c
        ON d.ledger_transaction_id=c.ledger_transaction_id
        AND debit_cents=credit_cents
    GROUP BY 1,2
        UNION ALL
    -- wire transfer withdrawals
    SELECT t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , a_patron_id AS patron_id
        , (SUM(debit_cents)/100.0) - (SUM(credit_cents)/100.0) AS patron_withdrawals
    FROM ca_edgebook_snapshot.max_accounting_tables_all
    WHERE (p_type='amendment' OR p_type='amendment_rg')
         AND adjustment_reason = '9998'
         AND adjustment_type = '"cash_withdrawal"'
         AND a_type='customer_liability'
         AND t_changed_at >= reports_start_time_utc_ca_on()
         AND t_changed_at < reports_end_time_utc_ca_on()
         AND gaming_state='CA-ON'
    GROUP BY 1,2
        UNION ALL
    -- REGION TRANSFER OUT
    SELECT
        t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , a_patron_id AS patron_id
        , (SUM(debit_cents)/100.0) - (SUM(credit_cents)/100.0) AS patron_withdrawals
    FROM ca_edgebook_snapshot.max_accounting_tables_all
    WHERE p_type='region_transfer_out'
        AND a_type='customer_liability'
        AND t_changed_at >= reports_start_time_utc_ca_on()
        AND t_changed_at < reports_end_time_utc_ca_on()
        AND gaming_state='CA-ON'
    GROUP BY 1,2
)

-- CANCELLED WITHDRAWALS
, dr_withdrawal_cancelled AS (
  SELECT DISTINCT ledger_transaction_id
    , patron_transaction_id
    , a_patron_id AS patron_id
    , a_type
    , debit_cents
    , t_changed_at AS updated_at
FROM ca_edgebook_snapshot.max_accounting_tables_all
WHERE a_type='clearing_withdrawals'
     AND (p_type='withdrawal_cancellation' OR p_type='withdrawal_decline')
     AND debit_cents>0
     AND t_changed_at >= reports_start_time_utc_ca_on()
     AND t_changed_at < reports_end_time_utc_ca_on()
     AND gaming_state='CA-ON'
)

, cr_withdrawal_cancelled AS (
  SELECT DISTINCT ledger_transaction_id
    , patron_transaction_id
    , a_patron_id AS patron_id
    , a_type
    , credit_cents
    , t_changed_at AS updated_at
FROM ca_edgebook_snapshot.max_accounting_tables_all
WHERE a_type='customer_liability'
    AND (p_type='withdrawal_cancellation' OR p_type='withdrawal_decline')
    AND credit_cents>0
    AND t_changed_at >= reports_start_time_utc_ca_on()
    AND t_changed_at < reports_end_time_utc_ca_on()
    AND gaming_state='CA-ON'
)

, cancelled_withdrawals AS (
    SELECT
        d.updated_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , d.patron_id
        , SUM(debit_cents)/100.0 AS patron_cancelled_withdrawals
    FROM dr_withdrawal_cancelled AS d
    INNER JOIN cr_withdrawal_cancelled AS c
        ON d.ledger_transaction_id=c.ledger_transaction_id
        AND debit_cents=credit_cents
    GROUP BY 1,2
)

, cashable_adjustments AS (
    -- transaction_adjustments
    SELECT a_patron_id AS patron_id
        , DATE_TRUNC('day',t_changed_at AT time zone 'utc' AT time zone 'America/Toronto') AS transaction_timestamp
        , (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) AS adjustments
    FROM ca_edgebook_snapshot.max_accounting_tables_all
    WHERE (p_type='amendment' OR p_type='amendment_rg')
         AND (adjustment_reason != '9998' OR adjustment_reason IS NULL)
         AND a_type='customer_liability'
         AND t_changed_at >= reports_start_time_utc_ca_on()
         AND t_changed_at < reports_end_time_utc_ca_on()
         AND gaming_state='CA-ON'
    GROUP BY 1,2
)

-- CLOSING CASH BALANCE
, closing_cash_balances_accounting AS (
    SELECT
        a_patron_id AS patron_id
        , (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) AS closing_cash_balance
    FROM ca_edgebook_snapshot.max_accounting_tables_all
    WHERE a_type='customer_liability'
    AND gaming_state='CA-ON'
    GROUP BY 1
)

, closing_cash_balances AS (
SELECT
    u.patron_id
    , COALESCE(b.closing_cash_balance,0.0) AS closing_cash_balance
FROM ca_on_reports.max_identity_users_in_state AS u
LEFT JOIN closing_cash_balances_accounting AS b
    ON u.patron_id = b.patron_id
    )
-- DATA-8784 closing cash balance CTE moved to separate model
-- CLOSING CASH BALANCE
-- , closing_cash_balances_accounting AS (
--     SELECT
--         a_patron_id AS patron_id
--         , (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) AS closing_cash_balance
--     FROM ca_edgebook_snapshot.max_accounting_tables_all
--     WHERE a_type='customer_liability'
--     AND gaming_state='CA-ON'
--     GROUP BY 1
-- )

-- , closing_cash_balances AS (
--     SELECT
--         u.patron_id
--         , COALESCE(b.closing_cash_balance,0.0) AS closing_cash_balance
--     FROM ca_on_reports.max_identity_users_in_state AS u
--     LEFT JOIN closing_cash_balances_accounting AS b
--         ON u.patron_id = b.patron_id
-- )

-- DATA-8784 bonus_released_edgebook_all CTE moved to separate model
-- , bonus_released_edgebook_all AS (
--     -- BONUSES CONVERTED / REVOKED (NET)
--     SELECT t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS gaming_date
--         , a_patron_id AS patron_id
--         , (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) AS bonus_released_all
--     FROM ca_edgebook_snapshot.max_accounting_tables_all
--     WHERE p_type IN ('bonus_offer_payout','bonus_offer_revoked')
--         AND a_type='customer_liability'
--     GROUP BY 1,2
-- )

 -- BONUSES CONVERTED / REVOKED (NET)
, bonus_released_edgebook_all AS (
    SELECT t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS gaming_date
        , a_patron_id AS patron_id
        , (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) AS bonus_released_all
    FROM ca_edgebook_snapshot.max_accounting_tables_all
    WHERE p_type IN ('bonus_offer_payout','bonus_offer_revoked', 'cash_deposit')
        AND a_type='customer_liability'
    GROUP BY 1,2)

 -- bonus_balance
, opening_bonus_balances_accounting AS (
    SELECT
        ab.user_id as patron_id
        , SUM(transaction_amount)/100.0 AS opening_bonus_cash_balance
    FROM ca_promotions_snapshot.min_d_bonus_transactions AS ab
    WHERE ab.transaction_timestamp_utc < reports_start_time_utc_ca_on()
      AND transaction_type NOT IN('Bonus Playthrough','bonus playthrough cancellation')
    GROUP BY 1
)

-- DATA-9129
, expired_bonus_programs AS (
    SELECT user_id,
            bc.id as bonus_award_cash_id,
            bc.amount_cents as total_amount,
            bc.expires_at
    FROM ca_promotions_snapshot.max_user_award_bonus_cash bc
    WHERE expires_at < reports_end_time_utc_ca_on()
)
-- DATA-9129
-- Associate bonus playthroughs past midnight with expired programs
, playthroughs_expiry_day_before AS (
     SELECT DISTINCT e.user_id,
        e.bonus_award_cash_id,
        t_changed_at as tx_time,
        COALESCE(SUM(bct.amount_cents),0.0) trans_amount
    FROM expired_bonus_programs e
    JOIN ca_promotions_snapshot.max_user_bonus_cash_transactions bct ON e.bonus_award_cash_id = bct.user_award_bonus_cash_id
    WHERE transaction_type='unlock' and t_changed_at >= expires_at
    AND expires_at < reports_start_time_utc_ca_on()
    AND t_changed_at >= reports_start_time_utc_ca_on()
    AND t_changed_at < reports_end_time_utc_ca_on()
    GROUP BY 1,2,3
)

-- PromoStartBalance
, opening_bonus_balances_in_state AS (
    -- sum of all non-playthrough bonus txs from patrons
    -- who were registered at the start of reporting period
    SELECT DISTINCT u.patron_id
         , COALESCE(opening_bonus_cash_balance,0.0) AS opening_bonus_cash_balance
    FROM ca_on_reports.min_identity_users_in_state AS u
    LEFT JOIN opening_bonus_balances_accounting AS b
        ON u.patron_id = b.patron_id

    UNION ALL

    -- sum of all playthrough bonus txs from patrons
    -- who were registered at the start of reporting period
    SELECT b.patron_id
         , -1.0*SUM(bonus_released_all) AS opening_bonus_cash_balance
    FROM bonus_released_edgebook_all AS b
    INNER JOIN ca_on_reports.min_identity_users_in_state u
    ON b.patron_id = u.patron_id
    WHERE gaming_date < reports_start_time_et_ca_on()
    GROUP BY 1

    UNION ALL

    -- DATA-9129
    -- handle edge case where bonus playthrough is recorded
    -- in Edgebook after the bonus program expiry timestamp
    SELECT pl.user_id AS patron_id
        , SUM(trans_amount/100.0) AS opening_bonus_cash_balance
    FROM playthroughs_expiry_day_before pl
    INNER JOIN ca_on_reports.min_identity_users_in_state u
    ON pl.user_id = u.patron_id
    GROUP BY 1
)

, closing_bonus_balances_accounting AS (
    SELECT user_id as patron_id
        , SUM(transaction_amount)/100.0 AS closing_bonus_cash_balance
    FROM ca_promotions_snapshot.max_bonus_transactions AS ab
    WHERE ab.transaction_timestamp_utc < reports_end_time_utc_ca_on()
    AND transaction_type NOT IN('Bonus Playthrough','bonus playthrough cancellation')
    GROUP BY 1
)

--EndPromoBalance
, closing_bonus_balances AS (
    SELECT DISTINCT u.patron_id
        , COALESCE(closing_bonus_cash_balance,0.0) AS closing_bonus_cash_balance
    FROM ca_on_reports.max_identity_users_in_state AS u
    LEFT JOIN closing_bonus_balances_accounting AS b
        ON u.patron_id = b.patron_id

  UNION ALL

    SELECT u.patron_id
      , -1.0*SUM(bonus_released_all) AS closing_bonus_cash_balance
    FROM bonus_released_edgebook_all b
    INNER JOIN ca_on_reports.max_identity_users_in_state u
    ON b.patron_id = u.patron_id
    WHERE gaming_date < reports_end_time_et_ca_on()
    GROUP BY 1
)

, new_region_users AS (
    SELECT DISTINCT customer_id
                  , patron_id
                  , inserted_at
                  , DATE_TRUNC('day',inserted_at AT time zone 'utc' AT time zone 'America/Toronto') AS inserted_at_et
    FROM ca_on_reports.max_identity_users_in_state
    WHERE inserted_at >= reports_start_time_utc_ca_on()
        AND inserted_at < reports_end_time_utc_ca_on()
)

-- PromoAdjustments
, bonus_adjustments AS (
    -- for patrons who were registered at the start of the reporting period
    -- non-playthrough bonus txs that happened within reporting period
     SELECT user_id AS patron_id
                  , COALESCE(SUM(transaction_amount)/100.0, 0.0) AS bonus_adjustments
    FROM ca_on_reports.min_identity_users_in_state u
    LEFT JOIN ca_promotions_snapshot.max_bonus_transactions AS ab
    ON ab.user_id=u.patron_id
    WHERE ab.transaction_timestamp_utc >= reports_start_time_utc_ca_on()
      AND ab.transaction_timestamp_utc < reports_end_time_utc_ca_on()
      AND transaction_type NOT IN('Bonus Playthrough','bonus playthrough cancellation')
    GROUP BY 1

    UNION ALL
    -- for patrons who were registered at the start of the reporting period
    -- playthrough bonus txs that happened within reporting period
    SELECT b.patron_id
                  , -1.0*SUM(bonus_released_all) AS bonus_adjustments
    FROM bonus_released_edgebook_all b
    INNER JOIN ca_on_reports.min_identity_users_in_state u
    ON b.patron_id=u.patron_id
    WHERE gaming_date >= reports_start_time_et_ca_on()
    AND gaming_date < reports_end_time_et_ca_on()
    AND b.patron_id IS NOT NULL
    GROUP BY 1

    UNION ALL

    -- for users who registered during the reporting period
    -- for new users PromoStartBalance=0 & PromoAdjustments=PromoEndBalance
    SELECT u.patron_id,
                    SUM(closing_bonus_cash_balance)
    FROM closing_bonus_balances b
    INNER JOIN new_region_users u
    ON u.patron_id=b.patron_id
    GROUP BY 1
)

-- , gaming_sessions_summary AS (
--     SELECT identity_user_id AS patron_id,
--            COUNT(DISTINCT game_session_id) num_game_sessions,
--            GREATEST(SUM(EXTRACT(EPOCH FROM (session_end_time - GREATEST(session_start_time, reports_start_time_et_ca_on()))))/60.0, 1)::int total_gameplay_duration
--     FROM ca_on_reports.max_gaming_sessions
--     -- sessions that start and ends during the reporting period (most cases)
--     WHERE (session_start_time >= reports_start_time_utc_ca_on() AND session_end_time < reports_end_time_utc_ca_on())
--     -- sessions that started in the previous reporting period but still have wagers after midnight
--     OR (session_start_time < reports_start_time_utc_ca_on() AND session_end_time > reports_start_time_utc_ca_on())
--     GROUP BY identity_user_id
-- )

, gaming_sessions_summary_helper AS (
    SELECT w.patron_id, casino_session_id
        , MIN(transaction_timestamp_utc) AS session_start_time
        , MAX(transaction_timestamp_utc) AS session_end_time
    FROM ca_on_reports.max_wager_and_game_info w
    LEFT JOIN ca_on_reports.max_patron_transactions pt ON w.patron_transaction_id = pt.id
    WHERE w.type NOT IN ('rollback')
        AND pt._changed_at < reports_end_time_utc_ca_on()
        AND pt._changed_at >= (reports_start_time_utc_ca_on() - INTERVAL '1 DAY')
    GROUP BY 1,2)


, gaming_sessions_summary AS (
SELECT patron_id
        , COUNT(DISTINCT casino_session_id) num_game_sessions
        , GREATEST(SUM(EXTRACT(EPOCH FROM (session_end_time - GREATEST(session_start_time, reports_start_time_utc_ca_on()))))/60.0, 1)::int total_gameplay_duration
    FROM gaming_sessions_summary_helper
    -- sessions that started and ended during the same reporting day
    WHERE (session_start_time >= reports_start_time_utc_ca_on() AND session_end_time < reports_end_time_utc_ca_on())
    -- sessions that started in the previous reporting period but still have wagers after midnight
    OR (session_start_time < reports_start_time_utc_ca_on() AND session_end_time > reports_start_time_utc_ca_on())
    GROUP BY patron_id)


, all_wagers AS (
    -- casino = transfer_to_casino + transfer_to_casino_rollback +
    -- free_rounds_wagered + free_rounds_wagered_rollback +
    SELECT transaction_timestamp
                  , patron_id
                  , category
                  , COUNT(DISTINCT patron_transaction_id) AS num_wagers
                  , SUM(transfer_to_casino) AS wager_amount
    FROM transfer_to_casino
    GROUP BY 1,2,3
        UNION ALL
    SELECT transaction_timestamp
                  , patron_id
                  , category
                  , -1.0*COUNT(DISTINCT patron_transaction_id) AS num_wagers
                  , -1.0*SUM(transfer_to_casino_rollback) AS wager_amount
    FROM transfer_to_casino_rollback
    GROUP BY 1,2,3
        UNION ALL
    SELECT transaction_timestamp
                  , patron_id
                  , category
                  , COUNT(DISTINCT patron_transaction_id) AS num_wagers
                  , SUM(free_rounds_wagered) AS wager_amount
    FROM free_rounds_wagered
    GROUP BY 1,2,3
        UNION ALL
    SELECT transaction_timestamp
                  , patron_id
                  , category
                  , -1.0*COUNT(DISTINCT patron_transaction_id) AS num_wagers
                  , -1.0*SUM(free_rounds_wagered_rollback) AS wager_amount
    FROM free_rounds_wagered_rollback
    GROUP BY 1,2,3
        UNION ALL

    -- sports = transfer_to_sports + cancelled_bets + voided_bets
    -- free_bets_wagered + cancelled_freebets + voided_freebets
    SELECT transaction_timestamp
                  , patron_id
                  , 'betting' AS category
                  , COUNT(DISTINCT bet_id) AS num_wagers
                  , SUM(transfer_to_sports) AS wager_amount
    FROM transfer_to_sports
    GROUP BY 1,2,3
        UNION ALL
    -- Deprecated due to Vegas
    -- SELECT DISTINCT transaction_timestamp
    --               , patron_id
    --               , 'betting' AS category
    --               , -1.0*COUNT(DISTINCT bet_id) AS num_wagers
    --               , -1.0*SUM(cancelled_amount) AS wager_amount
    -- FROM cancelled_bets
    -- GROUP BY 1,2,3
    --     UNION ALL
    SELECT transaction_timestamp
                  , patron_id
                  , 'betting' AS category
                  , -1.0*COUNT(DISTINCT bet_id) AS num_wagers
                  , -1.0*SUM(voided_amount) AS wager_amount
    FROM voided_bets
    GROUP BY 1,2,3
        UNION ALL
    SELECT transaction_timestamp
                  , patron_id
                  , 'betting' AS category
                  , COUNT(DISTINCT bet_id) AS num_wagers
                  , SUM(free_bets_wagered) AS wager_amount
    FROM free_bets_wagered
    GROUP BY 1,2,3
        UNION ALL
    -- Deprecated due to Vegas
    -- SELECT DISTINCT transaction_timestamp
    --               , patron_id
    --               , 'betting' AS category
    --               , -1.0*COUNT(DISTINCT bet_id) AS num_wagers
    --               , -1.0*SUM(cancelled_amount) AS wager_amount
    -- FROM cancelled_freebets
    -- GROUP BY 1,2,3
    --     UNION ALL
    SELECT transaction_timestamp
                  , patron_id
                  , 'betting' AS category
                  , -1.0*COUNT(DISTINCT bet_id) AS num_wagers
                  , -1.0*SUM(voided_amount) AS wager_amount
    FROM voided_freebets
    GROUP BY 1,2,3
)

, latest_activity AS (
SELECT p_patron_id as patron_id
, MAX(t_changed_at) AS last_active_date
FROM ca_edgebook_snapshot.max_accounting_tables_all
WHERE t_changed_at < reports_end_time_utc_ca_on()
AND gaming_state='CA-ON'
GROUP BY 1)

, dataset AS (
    -- StartBalance
    SELECT reports_start_time_et_ca_on()::TIMESTAMP AS gaming_date
        , patron_id
        , SUM(opening_cash_balance) AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , 0.0 AS adjustments
        , 0.0 AS player_win
        , 0.0 AS unsettled_wagers
        , 0.0 AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , 0.0 AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM opening_cash_balances
    GROUP BY 1,2
        UNION ALL
    -- Deposits
    SELECT transaction_timestamp::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , SUM(patron_cash_deposits) AS deposits
        , 0.0 AS withdrawals
        , 0.0 AS adjustments
        , 0.0 AS player_win
        , 0.0 AS unsettled_wagers
        , 0.0 AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , 0.0 AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM deposits
    WHERE  transaction_timestamp::TIMESTAMP >= reports_start_time_et_ca_on()
        AND transaction_timestamp::TIMESTAMP < reports_end_time_et_ca_on()
    GROUP BY 1,2
        UNION ALL
    -- Withdrawals=withdrawals+cancelled_withdrawals
    SELECT transaction_timestamp::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , SUM(patron_withdrawals) AS withdrawals
        , 0.0 AS adjustments
        , 0.0 AS player_win
        , 0.0 AS unsettled_wagers
        , 0.0 AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , 0.0 AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM withdrawals
    WHERE  transaction_timestamp::TIMESTAMP >= reports_start_time_et_ca_on()
        AND transaction_timestamp::TIMESTAMP < reports_end_time_et_ca_on()
    GROUP BY 1,2
        UNION ALL
    -- Withdrawals=withdrawals+cancelled_withdrawals
    SELECT transaction_timestamp::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , -1.0*SUM(patron_cancelled_withdrawals) AS withdrawals
        , 0.0 AS adjustments
        , 0.0 AS player_win
        , 0.0 AS unsettled_wagers
        , 0.0 AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , 0.0 AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM cancelled_withdrawals
    WHERE  transaction_timestamp::TIMESTAMP >= reports_start_time_et_ca_on()
        AND transaction_timestamp::TIMESTAMP < reports_end_time_et_ca_on()
    GROUP BY 1,2
        UNION ALL
    -- Adjustments=CashableAdjustments+bonus_released
    SELECT transaction_timestamp::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , SUM(adjustments) AS adjustments
        , 0.0 AS player_win
        , 0.0 AS unsettled_wagers
        , 0.0 AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , 0.0 AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM cashable_adjustments
    WHERE  transaction_timestamp::TIMESTAMP >= reports_start_time_et_ca_on()
        AND transaction_timestamp::TIMESTAMP < reports_end_time_et_ca_on()
    GROUP BY 1,2
        UNION ALL
    -- Adjustments=CashableAdjustments+bonus_released
    SELECT gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , SUM(bonus_released_all) AS adjustments
        , 0.0 AS player_win
        , 0.0 AS unsettled_wagers
        , 0.0 AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , 0.0 AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM bonus_released_edgebook_all
    WHERE  gaming_date >= reports_start_time_et_ca_on()
        AND gaming_date < reports_end_time_et_ca_on()
    GROUP BY 1,2
        UNION ALL
    -- PlayerWin = transfer_from_sports+transfer_from_casino+resettled_sport_wagers
    SELECT transaction_timestamp::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , 0.0 AS adjustments
        , SUM(transfer_from_sports) AS player_win
        , 0.0 AS unsettled_wagers
        , 0.0 AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , 0.0 AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM transfer_from_sports
    WHERE  transaction_timestamp::TIMESTAMP >= reports_start_time_et_ca_on()
        AND transaction_timestamp::TIMESTAMP < reports_end_time_et_ca_on()
    GROUP BY 1,2
        UNION ALL
    -- PlayerWin = transfer_from_sports+transfer_from_casino+resettled_sport_wagers
    SELECT transaction_timestamp::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , 0.0 AS adjustments
        , SUM(transfer_from_casino) AS player_win
        , 0.0 AS unsettled_wagers
        , 0.0 AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , 0.0 AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM transfer_from_casino
    WHERE  transaction_timestamp::TIMESTAMP >= reports_start_time_et_ca_on()
        AND transaction_timestamp::TIMESTAMP < reports_end_time_et_ca_on()
    GROUP BY 1,2
        UNION ALL
    -- PlayerWin = transfer_from_sports+transfer_from_casino+resettled_sport_wagers
    SELECT transaction_timestamp::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , 0.0 AS adjustments
        , -1.0*SUM(resettled_sport_wagers) AS player_win
        , 0.0 AS unsettled_wagers
        , 0.0 AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , 0.0 AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM resettled_sport_wagers
    WHERE  transaction_timestamp::TIMESTAMP >= reports_start_time_et_ca_on()
        AND transaction_timestamp::TIMESTAMP < reports_end_time_et_ca_on()
    GROUP BY 1,2
        UNION ALL
    -- UnsettledWagers = pending_sport_wagers
    SELECT transaction_timestamp::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , 0.0 AS adjustments
        , 0.0 AS player_win
        , SUM(pending_sport_wagers) AS unsettled_wagers
        , 0.0 AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , 0.0 AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM pending_sport_wagers
    WHERE  transaction_timestamp::TIMESTAMP >= reports_start_time_et_ca_on()
        AND transaction_timestamp::TIMESTAMP < reports_end_time_et_ca_on()
    GROUP BY 1,2
        UNION ALL
    -- WithdrawableWin =
    -- free_bets_payouts + resettled_sport_wagers_freebets + free_rounds_payouts
    SELECT transaction_timestamp::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , 0.0 AS adjustments
        , 0.0 AS player_win
        , 0.0 AS unsettled_wagers
        , SUM(free_bets_payouts) AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , 0.0 AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM free_bets_payouts
    WHERE  transaction_timestamp::TIMESTAMP >= reports_start_time_et_ca_on()
        AND transaction_timestamp::TIMESTAMP < reports_end_time_et_ca_on()
    GROUP BY 1,2
        UNION ALL
    -- WithdrawableWin =
    -- free_bets_payouts + resettled_sport_wagers_freebets + free_rounds_payouts
    SELECT transaction_timestamp::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , 0.0 AS adjustments
        , 0.0 AS player_win
        , 0.0 AS unsettled_wagers
        , -1.0*SUM(resettled_sport_wagers_freebets) AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , 0.0 AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM resettled_sport_wagers_freebets
    WHERE  transaction_timestamp::TIMESTAMP >= reports_start_time_et_ca_on()
        AND transaction_timestamp::TIMESTAMP < reports_end_time_et_ca_on()
    GROUP BY 1,2
        UNION ALL
    -- WithdrawableWin =
    -- free_bets_payouts + resettled_sport_wagers_freebets + free_rounds_payouts
    SELECT transaction_timestamp::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , 0.0 AS adjustments
        , 0.0 AS player_win
        , 0.0 AS unsettled_wagers
        , SUM(free_rounds_payouts) AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , 0.0 AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM free_rounds_payouts
    WHERE  transaction_timestamp::TIMESTAMP >= reports_start_time_et_ca_on()
        AND transaction_timestamp::TIMESTAMP < reports_end_time_et_ca_on()
    GROUP BY 1,2
        UNION ALL
    -- DATA-9000
    -- subtract jackpot payouts (FREE round only) from free round payouts
    -- add jackpot payouts (FREE round only) to cash winnings
    SELECT transaction_timestamp::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , 0.0 AS adjustments
        , SUM(free_round_payout) AS player_win -- DATA-9140
        , 0.0 AS unsettled_wagers
        , SUM(free_round_payout) * (-1) AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , 0.0 AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM jackpot_payout_free
    WHERE transaction_timestamp::TIMESTAMP >= reports_start_time_et_ca_on()
        AND transaction_timestamp::TIMESTAMP < reports_end_time_et_ca_on()
    GROUP BY 1,2
        UNION ALL
    -- EndBalance
    SELECT reports_start_time_et_ca_on()::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , 0.0 AS adjustments
        , 0.0 AS player_win
        , 0.0 AS unsettled_wagers
        , 0.0 AS withdrawable_win
        , SUM(closing_cash_balance) AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , 0.0 AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM closing_cash_balances
    GROUP BY 1,2
        UNION ALL
    -- PromoStartBalance
    SELECT reports_start_time_et_ca_on()::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , 0.0 AS adjustments
        , 0.0 AS player_win
        , 0.0 AS unsettled_wagers
        , 0.0 AS withdrawable_win
        , 0.0 AS end_balance
        , SUM(opening_bonus_cash_balance) AS promo_start_balance
        , 0.0 AS promo_adjustments
        , 0.0 AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM opening_bonus_balances_in_state
    GROUP BY 1,2
        UNION ALL
    -- PromoAdjustments
    SELECT reports_start_time_et_ca_on()::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , 0.0 AS adjustments
        , 0.0 AS player_win
        , 0.0 AS unsettled_wagers
        , 0.0 AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , SUM(bonus_adjustments) AS promo_adjustments
        , 0.0 AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM bonus_adjustments
    GROUP BY 1,2
        UNION ALL
    -- PromoPlayerWagers=
    -- free_bets_wagered + free_bets_cancelled + free_bets_voided
    -- free_rounds_wagered + free_rounds_wagered_rollback
    SELECT transaction_timestamp::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , 0.0 AS adjustments
        , 0.0 AS player_win
        , 0.0 AS unsettled_wagers
        , 0.0 AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , SUM(free_bets_wagered) AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM free_bets_wagered
    WHERE  transaction_timestamp::TIMESTAMP >= reports_start_time_et_ca_on()
        AND transaction_timestamp::TIMESTAMP < reports_end_time_et_ca_on()
    GROUP BY 1,2
        UNION ALL
    -- Deprecated due to Vegas
    -- SELECT DISTINCT
    --     transaction_timestamp::TIMESTAMP AS gaming_date
    --     , patron_id
    --     , 0.0 AS start_balance
    --     , 0.0 AS deposits
    --     , 0.0 AS withdrawals
    --     , 0.0 AS adjustments
    --     , 0.0 AS player_win
    --     , 0.0 AS unsettled_wagers
    --     , 0.0 AS withdrawable_win
    --     , 0.0 AS end_balance
    --     , 0.0 AS promo_start_balance
    --     , 0.0 AS promo_adjustments
    --     , -1.0*SUM(cancelled_amount) AS promo_player_wagers
    --     , 0.0 AS promo_unsettled_wagers
    --     , 0.0 AS promo_end_balance
    --     , 0.0 AS num_game_sessions
    --     , 0.0 AS num_wagers
    --     , 0.0 AS total_gameplay_duration
    --     , 0.0 AS wagers_slots
    --     , 0.0 AS wagers_tables
    --     , 0.0 AS wagers_livedealer
    --     , 0.0 AS wagers_betting
    --     , 0.0 AS wagers_other
    -- FROM cancelled_freebets
    -- WHERE  transaction_timestamp::TIMESTAMP >= reports_start_time_et_ca_on()
    --     AND transaction_timestamp::TIMESTAMP < reports_end_time_et_ca_on()
    -- GROUP BY 1,2
    --     UNION ALL
    SELECT transaction_timestamp::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , 0.0 AS adjustments
        , 0.0 AS player_win
        , 0.0 AS unsettled_wagers
        , 0.0 AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , -1.0*SUM(voided_amount) AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM voided_freebets
    WHERE  transaction_timestamp::TIMESTAMP >= reports_start_time_et_ca_on()
        AND transaction_timestamp::TIMESTAMP < reports_end_time_et_ca_on()
    GROUP BY 1,2
        UNION ALL
    -- PromoPlayerWagers=
    -- free_bets_wagered + free_bets_cancelled + free_bets_voided
    -- free_rounds_wagered + free_rounds_wagered_rollback
    SELECT transaction_timestamp::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , 0.0 AS adjustments
        , 0.0 AS player_win
        , 0.0 AS unsettled_wagers
        , 0.0 AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , SUM(free_rounds_wagered) AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM free_rounds_wagered
    WHERE  transaction_timestamp::TIMESTAMP >= reports_start_time_et_ca_on()
        AND transaction_timestamp::TIMESTAMP < reports_end_time_et_ca_on()
    GROUP BY 1,2
        UNION ALL
    -- PromoPlayerWagers=
    -- free_bets_wagered + free_bets_cancelled + free_bets_voided
    -- free_rounds_wagered + free_rounds_wagered_rollback
    SELECT transaction_timestamp::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , 0.0 AS adjustments
        , 0.0 AS player_win
        , 0.0 AS unsettled_wagers
        , 0.0 AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , -1.0*SUM(free_rounds_wagered_rollback) AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM free_rounds_wagered_rollback
    WHERE  transaction_timestamp::TIMESTAMP >= reports_start_time_et_ca_on()
        AND transaction_timestamp::TIMESTAMP < reports_end_time_et_ca_on()
    GROUP BY 1,2
        UNION ALL
    -- PromoUnsettledWagers
    SELECT transaction_timestamp::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , 0.0 AS adjustments
        , 0.0 AS player_win
        , 0.0 AS unsettled_wagers
        , 0.0 AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , 0.0 AS promo_player_wagers
        , SUM(pending_sport_wagers_freebets) AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM pending_sport_wagers_freebets
    WHERE  transaction_timestamp::TIMESTAMP >= reports_start_time_et_ca_on()
        AND transaction_timestamp::TIMESTAMP < reports_end_time_et_ca_on()
    GROUP BY 1,2
        UNION ALL
    -- PromoEndBalance
    SELECT reports_start_time_et_ca_on()::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , 0.0 AS adjustments
        , 0.0 AS player_win
        , 0.0 AS unsettled_wagers
        , 0.0 AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , 0.0 AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , SUM(closing_bonus_cash_balance) AS promo_end_balance
        , 0.0 AS num_game_sessions
        , 0.0 AS num_wagers
        , 0.0 AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM closing_bonus_balances
    GROUP BY 1,2
        UNION ALL
    --#ofGameSessions
    --TotalGameplayDuration
    SELECT reports_start_time_et_ca_on()::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , 0.0 AS adjustments
        , 0.0 AS player_win
        , 0.0 AS unsettled_wagers
        , 0.0 AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , 0.0 AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , SUM(num_game_sessions) AS num_game_sessions
        , 0.0 AS num_wagers
        , SUM(total_gameplay_duration) AS total_gameplay_duration
        , 0.0 AS wagers_slots
        , 0.0 AS wagers_tables
        , 0.0 AS wagers_livedealer
        , 0.0 AS wagers_betting
        , 0.0 AS wagers_other
    FROM gaming_sessions_summary
    GROUP BY 1,2
        UNION ALL
    --#ofWagers,WagersX
    SELECT transaction_timestamp::TIMESTAMP AS gaming_date
        , patron_id
        , 0.0 AS start_balance
        , 0.0 AS deposits
        , 0.0 AS withdrawals
        , 0.0 AS adjustments
        , 0.0 AS player_win
        , 0.0 AS unsettled_wagers
        , 0.0 AS withdrawable_win
        , 0.0 AS end_balance
        , 0.0 AS promo_start_balance
        , 0.0 AS promo_adjustments
        , 0.0 AS promo_player_wagers
        , 0.0 AS promo_unsettled_wagers
        , 0.0 AS promo_end_balance
        , 0.0 AS num_game_sessions
        , SUM(num_wagers) AS num_wagers
        , 0.0 AS total_gameplay_duration
        , SUM(CASE WHEN category IN ('slots') THEN wager_amount ELSE 0 END) AS wagers_slots
        , SUM(CASE WHEN category IN ('table', 'roulette') THEN wager_amount ELSE 0 END) AS wagers_tables
        , SUM(CASE WHEN category IN ('live') THEN wager_amount ELSE 0 END) AS wagers_livedealer
        , SUM(CASE WHEN category IN ('betting') THEN wager_amount ELSE 0 END)  AS wagers_betting
        , SUM(CASE WHEN category NOT IN ('slots', 'table', 'roulette', 'live', 'betting') THEN wager_amount ELSE 0 END) AS wagers_other
    FROM all_wagers
    WHERE  transaction_timestamp::TIMESTAMP >= reports_start_time_et_ca_on()
        AND transaction_timestamp::TIMESTAMP < reports_end_time_et_ca_on()
    GROUP BY 1,2
)

, final_dataset AS (
    SELECT DATE_TRUNC('day',gaming_date) AS gaming_date
   , patron_id
   , SUM(start_balance) AS start_balance
   , SUM(deposits) AS deposits
   , SUM(withdrawals) AS withdrawals
   , SUM(adjustments) AS adjustments
   , SUM(player_win)::NUMERIC(38,2) AS player_win
   , SUM(unsettled_wagers) AS unsettled_wagers
   , SUM(withdrawable_win)::NUMERIC(38,2) AS withdrawable_win
   , SUM(end_balance) AS end_balance
   , SUM(promo_start_balance) AS promo_start_balance
   , SUM(promo_adjustments) AS promo_adjustments
   , SUM(promo_player_wagers) AS promo_player_wagers
   , SUM(promo_unsettled_wagers) AS promo_unsettled_wagers
   , SUM(promo_end_balance) AS promo_end_balance
   , SUM(num_game_sessions) AS num_game_sessions
   , SUM(num_wagers) AS num_wagers
   , SUM(total_gameplay_duration) AS total_gameplay_duration
   , SUM(wagers_slots) AS wagers_slots
   , SUM(wagers_tables) AS wagers_tables
   , SUM(wagers_livedealer) AS wagers_livedealer
   , SUM(wagers_betting) AS wagers_betting
   , SUM(wagers_other) AS wagers_other
    FROM dataset
    GROUP BY 1, 2
)

SELECT DISTINCT reports_start_time_et_ca_on()::DATE AS "GamingDay"
     , 'S100001A' AS "GamingSiteID"
     , u.patron_id AS "PlayerID"
     , datediff(day, u.birthdate ,current_date)/365 as "Age"
     ,(CASE WHEN u.gender = 'male' THEN 'M' WHEN u.gender = 'female' THEN 'F' ELSE 'X' END) AS "Gender"
     , SUBSTRING(a.postal_code, 1, 3) AS "FSA"
     , TO_CHAR(registration_completed_at AT time zone 'utc' AT time zone 'America/Toronto','YYYYMMDD') AS "RegistrationDate"
     , COALESCE(TO_CHAR(la.last_active_date AT time zone 'utc' AT time zone 'America/Toronto', 'YYYYMMDD'), '00000000') AS "LastActiveDate"
     , start_balance AS "StartBalance"
     , deposits AS "Deposits"
     , withdrawals AS "Withdrawals"
     , adjustments AS "Adjustments"
     , wagers_slots + wagers_tables + wagers_livedealer + wagers_betting + wagers_other-promo_player_wagers AS "PlayerWagers"
     , player_win AS "PlayerWin"
     , unsettled_wagers AS "UnsettledWagers"
     , player_win-(wagers_slots + wagers_tables + wagers_livedealer + wagers_betting + wagers_other-promo_player_wagers) AS "Win/Loss"
     , withdrawable_win AS "WithdrawableWin"
     , end_balance AS "EndBalance"
     , promo_start_balance AS "PromoStartBalance"
     , promo_adjustments AS "PromoAdjustments"
     , promo_player_wagers AS "PromoPlayerWagers"
     , promo_unsettled_wagers AS "PromoUnsettledWagers"
     , 0 AS "PromoPlayerWin"
     , promo_end_balance AS "PromoEndBalance"
     , num_game_sessions AS "#ofGameSessions"
     , num_wagers AS "#ofWagers"
     , total_gameplay_duration AS "TotalGameplayDuration"
     , wagers_slots AS "WagersSlots"
     , wagers_tables AS "WagersTables"
     , wagers_livedealer AS "WagersLiveDealer"
     , wagers_betting AS "WagersBetting"
     , 0 AS "WagersP2PPoker"
     , wagers_other AS "WagersOther"
     , wagers_slots + wagers_tables + wagers_livedealer + wagers_betting + wagers_other AS "WagersMobile"
     , 0 AS "WagersPC"
    , CASE WHEN b."BreakEnd" IS NULL THEN '0' ELSE '1' END AS "BreakStatus"
    , COALESCE(b."BreakStart", '') AS "BreakStart"
    , COALESCE(b."BreakEnd", '') AS "BreakEnd"
    , CASE WHEN se."SEEnd" IS NULL THEN '0' ELSE '1' END AS "SEStatus"
    , COALESCE(se."SEStart", '') AS "SEStart"
    , COALESCE(se."SEEnd", '') AS "SEEnd"
    , '0' AS "HighRisk"
FROM final_dataset d
INNER JOIN ca_on_reports.max_identity_users_in_state u ON d.patron_id=u.patron_id AND u.is_tester IS FALSE
LEFT JOIN ca_on_reports.max_identity_addresses a ON a.user_id=d.patron_id
LEFT JOIN latest_activity la ON la.patron_id=d.patron_id
LEFT JOIN ca_on_reports.max_break_status b ON b.patron_id=d.patron_id
LEFT JOIN ca_on_reports.max_self_exclusion_status se ON se.patron_id=d.patron_id
