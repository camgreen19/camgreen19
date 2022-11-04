WITH dr_fu_cash AS NOT MATERIALIZED (
    SELECT DISTINCT ledger_transaction_id
        , a_patron_id AS patron_id
        , a_type AS type
        , debit_cents
        , t_changed_at AS updated_at
        , COALESCE(p_vegas_bet_id, p_bet_id) AS bet_id
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS a
    LEFT JOIN ca_finance_reports.free_bets AS fb
        ON COALESCE(a.p_vegas_bet_id, a.p_bet_id) = fb.bet_id
    WHERE p_type='sportsbook_bet'
        AND a_type='customer_liability'
        AND fb.bet_id IS NULL
        AND debit_cents>0
        AND t_changed_at >= reports_start_time_utc_ca_finance()
        AND t_changed_at < reports_end_time_utc_ca_finance()
)

, cr_cl_cash AS NOT MATERIALIZED (
    SELECT DISTINCT ledger_transaction_id
        , a_patron_id AS patron_id
        , a_type AS type
        , credit_cents
        , t_changed_at AS updated_at
        , COALESCE(p_vegas_bet_id, p_bet_id) AS bet_id
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS a
    LEFT JOIN ca_finance_reports.free_bets AS fb
        ON COALESCE(a.p_vegas_bet_id, a.p_bet_id) = fb.bet_id
    WHERE p_type='sportsbook_bet'
        AND a_type='sportsbook_unsettled_bets'
        AND fb.bet_id IS NULL
        AND credit_cents>0
        AND t_changed_at >= reports_start_time_utc_ca_finance()
        AND t_changed_at < reports_end_time_utc_ca_finance()
)

, transfer_to_sports AS NOT MATERIALIZED (
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
    FROM ca_finance_reports.free_bets_wagered AS fbw
    GROUP BY 1,2,3
)

-- Deprecated due to Vegas
-- CASH + FREE BETS CANCELLED
-- , cancelled_bets AS NOT MATERIALIZED (
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
, voided_bets AS NOT MATERIALIZED (
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
-- DATA-8784 Moved resettled_sport_wagers_accounting to separate model
, resettled_sport_wagers AS NOT MATERIALIZED (
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , bet_id
        , SUM(resettled_sport_wagers) AS resettled_sport_wagers
    FROM ca_finance_reports.resettled_sport_wagers_accounting
    --WHERE free_bet_id IS NULL
    GROUP BY 1,2,3
)

-- TRANSFER FROM SPORTS
, transfer_from_sports AS NOT MATERIALIZED (
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
    INNER JOIN ca_finance_reports.max_bet_states AS s
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
    INNER JOIN ca_on_reports.max_bet_states AS s
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

, dr_fu_cash_casino AS NOT MATERIALIZED (
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
        AND t_changed_at >= reports_start_time_utc_ca_finance()
        AND t_changed_at < reports_end_time_utc_ca_finance()
)

, cr_cl_cash_casino AS NOT MATERIALIZED (
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
        AND t_changed_at >= reports_start_time_utc_ca_finance()
        AND t_changed_at < reports_end_time_utc_ca_finance()
)

, transfer_to_casino AS NOT MATERIALIZED (
    -- CASH BETS
    SELECT DISTINCT
        d.updated_at AT time zone 'utc' AT time zone 'America/Toronto' AS wager_timestamp
        , d.patron_id
        , d.patron_transaction_id
        , w.game_category AS category
        , SUM(debit_cents)/100.0 AS transfer_to_casino
    FROM dr_fu_cash_casino AS d
    INNER JOIN cr_cl_cash_casino AS c
        ON d.ledger_transaction_id=c.ledger_transaction_id
        AND debit_cents=credit_cents
    LEFT JOIN ca_finance_reports.max_wager_and_game_info w ON w.patron_transaction_id=d.patron_transaction_id
    GROUP BY 1,2,3,4
)
-- , casino_free_wagers AS NOT MATERIALIZED (
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
, transfer_to_casino_rollback AS NOT MATERIALIZED (
    SELECT DISTINCT
           r._changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS wager_timestamp
         , wg.patron_id AS patron_id
         , r.patron_transaction_id AS patron_transaction_id
         , wg.game_category AS category
         , SUM(wg.amount_cents)/100.0 AS transfer_to_casino_rollback
    FROM ca_finance_reports.max_casino_wager_rollbacks_all r
    LEFT JOIN ca_finance_reports.max_wager_and_game_info wg ON r.patron_transaction_id=wg.patron_transaction_id
    WHERE wg.type IN ('rollback') AND wg.promo_engine_free_round_award_id IS NULL
      AND r._changed_at >= reports_start_time_utc_ca_finance()
      AND r._changed_at < reports_end_time_utc_ca_finance()
    GROUP BY 1,2,3,4
)

--new change
-- DATA-8784 Moved free_rounds_wagered, free_rounds_wagered_rollback to separate model

-- , casino_rollbacks AS NOT MATERIALIZED (
--     SELECT DISTINCT a.gaming_state
--         , t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS rollback_timestamp
--         , a_patron_id AS patron_id
--         , p_id AS patron_transaction_id
--         , p.promo_engine_free_round_award_id
--         , a.parent_id
--         , (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) AS casino_rollback_amount
--     FROM ca_edgebook_snapshot.max_accounting_tables_all a
--     LEFT JOIN ca_finance_reports.max_casino_payouts_all p ON p.patron_transaction_id=a.patron_transaction_id
--     WHERE p_type IN ('casino_rollback')
--          AND a_type='customer_liability'
--     GROUP BY 1,2,3,4,5,6
-- )

, casino_payouts AS NOT MATERIALIZED (
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

, casino_dataset AS NOT MATERIALIZED (
    SELECT 'CA-ON' AS gaming_state
    , DATE_TRUNC('day',wager_timestamp)::DATE AS gaming_date
    , DATE_TRUNC('seconds',wager_timestamp)::TIMESTAMP AS transaction_timestamp
    , patron_transaction_id as wager_id
    , patron_transaction_id as patron_transaction_id
    , patron_id as patron_id
    , transfer_to_casino as transfer_to_casino
    , 0.0 as rollback_casino_wagers
    , 0.0 as transfer_from_casino
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
    FROM ca_finance_reports.free_rounds_wagered
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
    FROM ca_finance_reports.free_rounds_wagered_rollback
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
    FROM casino_payouts
)

-- COMBINE INTO REQUIRED REPORT COLUMNS
, dataset AS NOT MATERIALIZED (
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

, final_revenue AS NOT MATERIALIZED (
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
        , CASE WHEN (bm.event_description = '' OR bm.event_description IS NULL) AND bm.bet_type NOT IN ('Parlay','Parlay_plus') THEN bm.market_type ELSE bm.event_description END AS event_description
        , DATE_TRUNC('seconds',bm.event_timestamp AT time zone 'utc' AT time zone 'America/Toronto') AS event_date
        , bm.selection
        , SUM(transfer_to_sports) AS wager_placed_amount
        , -1.0*SUM(transfer_from_sports) AS wager_paid_amount
        , -1.0*SUM(voided_sport_wagers) AS void_wager_amount
        , -1.0*SUM(cancelled_sport_wagers) AS cancelled_wager_amount
        , SUM(resettled_sport_wagers) AS resettled_wager_adjustment
        , 0.00 AS rollback_wager_amount
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
    , transfer_to_casino::NUMERIC(64,2) as wager_placed_amount
    , (-1.0*transfer_from_casino)::NUMERIC(64,2) AS wager_paid_amount
    , 0.00 AS void_wager_amount
    , 0.00 AS cancelled_wager_amount
    , 0.00 AS resettled_wager_adjustment
    , (-1.0*rollback_casino_wagers)::NUMERIC(64,2) AS rollback_wager_amount
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
    , wager_placed_amount::NUMERIC(64,2) AS "Wager Placed Amount"
    , wager_paid_amount::NUMERIC(64,2) AS "Wager Paid Amount"
    , void_wager_amount::NUMERIC(64,2) AS "Void Wager Amount"
    , cancelled_wager_amount::NUMERIC(64,2) AS "Cancelled Wager Amount"
    , resettled_wager_adjustment::NUMERIC(64,2) AS "Resettled Wager Adjustment"
    , rollback_wager_amount::NUMERIC(64,2) AS "Rollback Wager Amount"
    , (wager_placed_amount +
      wager_paid_amount +
      void_wager_amount +
      cancelled_wager_amount +
      resettled_wager_adjustment
      + rollback_wager_amount
      )::NUMERIC(64,2)
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
ORDER BY 1,2,3,4,5,6,7,8,9,10,11
