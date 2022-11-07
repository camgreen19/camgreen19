WITH report AS NOT MATERIALIZED (
    -- deposit_amount
    SELECT DISTINCT
        DATE_TRUNC('month',t_changed_at AT time zone 'utc' AT time zone 'America/Toronto') AS transaction_period
        -- , t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp_toronto
        , u.username AS username 
        , u.first_name
        , u.last_name
        , a_patron_id AS patron_id
        , u.employment_status
        , u.job_title
        , u.employer_name
        , gaming_state
        , (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) AS deposit_amount
        , COUNT(DISTINCT patron_transaction_id) AS deposits
        -- , patron_transaction_id AS transaction_id 
    FROM ca_edgebook_snapshot.max_accounting_tables_all d
    JOIN ca_aml_monthly_reports.max_identity_users as u
        ON u.patron_id = d.a_patron_id
        AND is_tester = false
    WHERE (
        (
            p_type='deposit'
            AND a_type='customer_liability'
        ) OR (
            (p_type='amendment' OR p_type='amendment_rg')
            AND adjustment_reason IN ('9998', '9990')
            AND adjustment_type = '"cash_deposit"'
            AND a_type='customer_liability'
        )
    ) AND (
        t_changed_at >= reports_start_time_utc_ca_aml_monthly()
        AND t_changed_at < reports_end_time_utc_ca_aml_monthly()
    )
    GROUP BY 1,2,3,4,5,6,7,8,9
    HAVING (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) >= 25000
)

, dr_withdrawal_initiated AS NOT MATERIALIZED (
    SELECT DISTINCT ledger_transaction_id
        , patron_transaction_id
        , a_patron_id AS patron_id
        , a_type
        , debit_cents
        , t_changed_at AS updated_at
    FROM ca_edgebook_snapshot.max_accounting_tables_all a
    JOIN report r
        ON r.patron_id = a.a_patron_id
    WHERE a_type='customer_liability'
    AND t_type='withdrawal'
    AND debit_cents>0
    AND (
        t_changed_at >= reports_start_time_utc_ca_aml_monthly()
        AND t_changed_at < reports_end_time_utc_ca_aml_monthly()
    )
)

, cr_withdrawal_initiated AS NOT MATERIALIZED (
    SELECT DISTINCT ledger_transaction_id
        , patron_transaction_id
        , a_patron_id AS patron_id
        , a_type
        , credit_cents
        , t_changed_at AS updated_at
    FROM ca_edgebook_snapshot.max_accounting_tables_all a
    JOIN report r
        ON r.patron_id = a.a_patron_id
    WHERE a_type='clearing_withdrawals'
        AND t_type='withdrawal'
        AND credit_cents>0
        AND (
            t_changed_at >= reports_start_time_utc_ca_aml_monthly()
            AND t_changed_at < reports_end_time_utc_ca_aml_monthly()
        )
)

, withdrawals AS NOT MATERIALIZED (
    -- withdrawals initiated
    SELECT
        DATE_TRUNC('month',d.updated_at AT time zone 'utc' AT time zone 'America/Toronto') AS transaction_period
        , d.patron_id
        , COUNT(DISTINCT d.patron_transaction_id) AS withdrawals 
        , SUM(debit_cents)/100.0 AS withdrawal_amount
    FROM dr_withdrawal_initiated AS d
    JOIN cr_withdrawal_initiated AS c
        ON d.ledger_transaction_id=c.ledger_transaction_id
        AND debit_cents=credit_cents
    GROUP BY 1,2
        UNION ALL
    -- wire transfer withdrawals
    SELECT DISTINCT
        DATE_TRUNC('month',t_changed_at AT time zone 'utc' AT time zone 'America/Toronto') AS transaction_period
        , a_patron_id AS patron_id
        , COUNT(DISTINCT patron_transaction_id) AS withdrawals 
        , (SUM(debit_cents)/100.0) - (SUM(credit_cents)/100.0) AS withdrawal_amount
    FROM ca_edgebook_snapshot.max_accounting_tables_all d
    JOIN report r
        ON d.a_patron_id = r.patron_id
    WHERE (p_type='amendment' OR p_type='amendment_rg')
         AND adjustment_reason IN ('9998', '9990')
         AND adjustment_type = '"cash_withdrawal"'
         AND a_type='customer_liability'
         AND (
            t_changed_at >= reports_start_time_utc_ca_aml_monthly()
            AND t_changed_at < reports_end_time_utc_ca_aml_monthly()
        )
    GROUP BY 1,2
)

, lt_withdrawals AS (
    -- withdrawals initiated
    SELECT 
        d.patron_id
        , COUNT(DISTINCT d.patron_transaction_id) AS lt_withdrawals 
        , SUM(debit_cents)/100.0 AS lt_withdrawal_amount
    FROM dr_withdrawal_initiated AS d
    JOIN cr_withdrawal_initiated AS c
        ON d.ledger_transaction_id=c.ledger_transaction_id
        AND debit_cents=credit_cents
    JOIN report r
        ON r.patron_id = d.patron_id
    GROUP BY 1
        UNION ALL
    -- wire transfer withdrawals
    SELECT  
        a_patron_id AS patron_id
        , COUNT(DISTINCT patron_transaction_id) AS lt_withdrawals 
        , (SUM(debit_cents)/100.0) - (SUM(credit_cents)/100.0) AS lt_withdrawal_amount
    FROM ca_edgebook_snapshot.max_accounting_tables_all a
    JOIN report d
        ON d.patron_id = a.a_patron_id
    WHERE (p_type='amendment' OR p_type='amendment_rg')
         AND adjustment_reason IN ('9998', '9990')
         AND adjustment_type = '"cash_withdrawal"'
         AND a_type='customer_liability'
    GROUP BY 1
)

, lt_deposits AS (
    SELECT DISTINCT
        a_patron_id AS patron_id
        , (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) AS lt_deposit_amount
        , COUNT(DISTINCT patron_transaction_id) AS lt_deposits
    FROM ca_edgebook_snapshot.max_accounting_tables_all d
    JOIN report r
        ON d.a_patron_id = r.patron_id
    WHERE (p_type='deposit'
        AND a_type='customer_liability') OR
        ((p_type='amendment' OR p_type='amendment_rg')
         AND adjustment_reason IN ('9998', '9990')
         AND adjustment_type = '"cash_deposit"'
         AND a_type='customer_liability')
    GROUP BY 1
    HAVING (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) >= 25000
)

, payment_info AS (
  SELECT DISTINCT
  iu.username 
  , pu.patron_id AS patron_id
  , DATE_TRUNC('seconds',iu.registration_completed_at AT time zone 'utc' AT time zone 'America/Toronto') AS kyc_date 
  , DATE_TRUNC('seconds',iu.last_login_at AT time zone 'utc' AT time zone 'America/Toronto') AS last_login_at
  , 'deposit' AS transaction 
  , COUNT(DISTINCT deposit_id) AS deposits 
  , SUM(1.0*ppdt.amount_requested_cents / 100) AS amount_dollars 
  , COUNT(DISTINCT pm.name) AS merchants 
  , COUNT(DISTINCT pp.name) AS providers
  , COUNT(DISTINCT COALESCE(TRIM('"' FROM ppdt.type), 'No Info')||'-'||COALESCE(TRIM('"' FROM ppdt.last_digits), 'No Info')) AS payment_cards 
  FROM ca_aml_monthly_reports.max_payment_provider_deposit_trans ppdt --payments_db.payment_provider_deposit_transactions ppdt
  JOIN ca_aml_monthly_reports.max_payment_providers pp --payments_db.payment_providers
    ON ppdt.provider_id = pp.id
  JOIN ca_aml_monthly_reports.max_payment_merchants pm --payments_db.payment_merchants
    ON pp.merchant_id = pm.id
  JOIN ca_aml_monthly_reports.max_payment_customers pc --payments_db.payment_customers
    ON pc.id = ppdt.customer_id
  JOIN ca_aml_monthly_reports.max_payment_users pu --payments_db.users
    ON pu.id = pc.user_id 
  JOIN ca_aml_monthly_reports.max_identity_users iu
    ON iu.patron_id = pu.patron_id
  JOIN report d
    ON d.patron_id = iu.patron_id
  WHERE state != 'created'
  GROUP BY 1,2,3,4,5
)

-- 1. CLEAN TABLES BY JOINING MAX ROWS IN DATE RANGE
--GGR
-- FREE BETS

, free_bets AS NOT MATERIALIZED (
    -- all free bets that were placed / closed with in the period
    SELECT DISTINCT b.id as bet_id
    FROM ca_aml_monthly_reports.max_vegas_bets_all AS b
    JOIN ca_edgebook_snapshot.max_accounting_tables_all a  
        ON b.id = COALESCE(a.p_vegas_bet_id, a.p_bet_id)
    JOIN report r
        ON r.patron_id = b.patron_id
    WHERE b.free_bet_id IS NOT NULL
    AND a.t_changed_at >= reports_start_time_utc_ca_aml_monthly()
    AND  a.t_changed_at < reports_end_time_utc_ca_aml_monthly()
)

, free_bets_wagered AS (
    SELECT DISTINCT
        b.patron_id
        , b.bet_amount_cents
        , b.placed_at
        , b.id AS bet_id
    FROM ca_aml_monthly_reports.max_vegas_bets_all AS b
    JOIN report r
        ON r.patron_id = b.patron_id
    WHERE b.free_bet_id IS NOT NULL
    AND b.placed_at >= reports_start_time_utc_ca_aml_monthly()
    AND b.placed_at < reports_end_time_utc_ca_aml_monthly()
)

-- 2. FETCH REQUIRED COLUMNS FROM CLEANED TABLES
-- TRANSFER TO SPORTS
-- cash bets
, dr_fu_cash AS (
    SELECT DISTINCT ledger_transaction_id
        , a_patron_id AS patron_id
        , a_type AS type
        , debit_cents
        , t_changed_at AS updated_at
        , COALESCE(p_vegas_bet_id, p_bet_id) AS bet_id
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS a
    LEFT JOIN free_bets AS fb
        ON COALESCE(a.p_vegas_bet_id, a.p_bet_id) = fb.bet_id
    JOIN report r
        ON r.patron_id = a.a_patron_id
    WHERE p_type in ('sportsbook_bet', 'casino_wager')
        AND a_type='customer_liability'
        AND fb.bet_id IS NULL
        AND debit_cents>0
        AND (
            t_changed_at >= reports_start_time_utc_ca_aml_monthly()
            AND t_changed_at < reports_end_time_utc_ca_aml_monthly()
        )
)

, cr_cl_cash AS (
    SELECT DISTINCT ledger_transaction_id
        , a_patron_id AS patron_id
        , a_type AS type
        , credit_cents
        , t_changed_at AS updated_at
        , COALESCE(p_vegas_bet_id, p_bet_id) AS bet_id
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS a
    LEFT JOIN free_bets AS fb
        ON COALESCE(a.p_vegas_bet_id, a.p_bet_id) = fb.bet_id
    JOIN report r
        ON r.patron_id = a.a_patron_id
    WHERE p_type in ('sportsbook_bet', 'casino_wager')
        AND a_type in ('sportsbook_unsettled_bets', 'casino_house_wins')
        AND fb.bet_id IS NULL
        AND credit_cents>0
        AND (
            t_changed_at >= reports_start_time_utc_ca_aml_monthly()
            AND t_changed_at < reports_end_time_utc_ca_aml_monthly()
        )
)

, transfer_to_sports AS (
    -- CASH BETS
    SELECT DISTINCT
        d.updated_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , d.patron_id
        , d.bet_id
        , SUM(debit_cents)/100.0 AS transfer_to_sports
    FROM dr_fu_cash AS d
    JOIN cr_cl_cash AS c
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
-- , cancelled_bets AS (
--     SELECT DISTINCT
--         b.patron_id
--         , b.id AS bet_id
--         , closed_at AT time zone 'utc' AT time zone 'America/Toronto' AS gaming_date
--         , SUM(bet_amount_cents)/100.0 AS cancelled_amount
--     FROM ca_aml_monthly_reports.max_vegas_bets_all AS b
--     JOIN report r
--         ON r.patron_id = b.patron_id
--     WHERE b.status = 'cancelled'
--         AND b.free_bet_id IS NULL
--         AND (
--             b.closed_at >= reports_start_time_utc_ca_aml_monthly()
--             AND b.closed_at < reports_end_time_utc_ca_aml_monthly()
--         )
--     GROUP BY 1,2,3
-- )

-- CASH + FREE BETS VOIDED
, voided_bets AS (
    SELECT DISTINCT
        b.patron_id
        , b.id AS bet_id
        , closed_at AT time zone 'utc' AT time zone 'America/Toronto' AS gaming_date
        , SUM(bet_amount_cents)/100.0 AS voided_amount
    FROM ca_aml_monthly_reports.max_vegas_bets_all AS b
    JOIN report r
        ON r.patron_id = b.patron_id
    WHERE b.status = 'voided'
        AND b.free_bet_id IS NULL
        AND (
            b.closed_at >= reports_start_time_utc_ca_aml_monthly()
            AND b.closed_at < reports_end_time_utc_ca_aml_monthly()
        )
    GROUP BY 1,2,3
)


-- -- cash + free bets resettled (regrades + ungrades)
, resettled_sport_wagers_accounting AS (
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
    JOIN ca_aml_monthly_reports.max_bet_states AS s
      ON p.bet_history_id = s.bet_history_id
    JOIN report r
        ON r.patron_id = p.a_patron_id
    WHERE (p.p_type='sportsbook_bet_resettlement' OR
        p.p_type='sportsbook_bet_ungrade' OR
        p.p_type='sportsbook_bet_void')
    AND (
            t_changed_at >= reports_start_time_utc_ca_aml_monthly()
            AND t_changed_at < reports_end_time_utc_ca_aml_monthly()
        )
    GROUP BY 1,2,3,4,5,6,7,8,9
)

, resettled_sport_wagers AS (
    SELECT DISTINCT
        transaction_timestamp
        , patron_id
        , bet_id
        , SUM(resettled_sport_wagers) AS resettled_sport_wagers
    FROM resettled_sport_wagers_accounting
    GROUP BY 1,2,3
)

-- TRANSFER FROM SPORTS
, transfer_from_sports AS (
    SELECT DISTINCT
        t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , a_patron_id AS patron_id
        , COALESCE(p_vegas_bet_id,p_bet_id) AS bet_id
        , (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) AS transfer_from_sports
    FROM ca_edgebook_snapshot.max_accounting_tables_all a
    JOIN report r
        ON r.patron_id = a.a_patron_id
    WHERE p_type IN ('sportsbook_cash_out_bet','sportsbook_payout','sportsbook_bet_lost', 'casino_payout')
         AND a_type='customer_liability'
    AND (
            t_changed_at >= reports_start_time_utc_ca_aml_monthly()
            AND t_changed_at < reports_end_time_utc_ca_aml_monthly()
        )
    GROUP BY 1,2,3

      UNION ALL

    SELECT DISTINCT
        t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , a_patron_id AS patron_id
        , COALESCE(p_vegas_bet_id,p_bet_id) AS bet_id
        , COALESCE( 
        CASE WHEN COALESCE(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NULL AND s.state='refund' 
            THEN (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0)
            WHEN COALESCE(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NOT NULL AND s.state='win' 
            THEN (SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0) 
            END, 0.0) AS transfer_from_sports
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS p
    INNER JOIN ca_aml_monthly_reports.max_bet_states AS s
    ON p.bet_history_id = s.bet_history_id
    WHERE (p.p_type='sportsbook_bet_resettlement')
        AND a_type IN ('customer_liability')
        AND s.state IN ('refund','win')
        AND previous_ordinal = '1'
        AND s.previous_state='open'
        AND t_changed_at >= reports_start_time_utc_ca_aml_monthly()
        AND t_changed_at < reports_end_time_utc_ca_aml_monthly()
    GROUP BY 1,2,3,COALESCE(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR), s.state
    
     UNION ALL

    SELECT DISTINCT
        t_changed_at AT time zone 'utc' AT time zone 'America/Toronto' AS transaction_timestamp
        , a_patron_id AS patron_id
        , COALESCE(p_vegas_bet_id,p_bet_id) AS bet_id
        , COALESCE((SUM(credit_cents)/100.0) - (SUM(debit_cents)/100.0)
                , 0.0)
            AS transfer_from_sports
    FROM ca_edgebook_snapshot.max_accounting_tables_all AS p
    INNER JOIN ca_aml_monthly_reports.max_bet_states AS s
    ON p.bet_history_id = s.bet_history_id
    WHERE (p.p_type='sportsbook_bet_resettlement')
        AND a_type IN ('customer_liability')
        AND COALESCE(s.promo_engine_free_bet_id::VARCHAR,s.free_bet_id::VARCHAR) IS NULL
        AND s.state='refund'
        AND s.previous_state='open'
        AND previous_ordinal > '1'
    AND t_changed_at >= reports_start_time_utc_ca_aml_monthly()
        AND t_changed_at < reports_end_time_utc_ca_aml_monthly()
    GROUP BY 1,2,3
)


-- COMBINE INTO REQUIRED REPORT COLUMNS
, dataset AS (
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
    -- CANCELLED SPORTS WAGERS
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
        -- UNION ALL
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

, final_revenue AS (
    SELECT
        DATE_TRUNC('day',gaming_date) AS gaming_date
        , DATE_TRUNC('seconds',gaming_date) AS transaction_time
        , d.bet_id
        , d.patron_id AS account_number
        , bm.event_type AS event_type
        , bm.bet_type AS bet_description
        , CASE WHEN (bm.event_description = '' OR bm.event_description IS NULL) AND bm.bet_type NOT IN ('Parlay','Parlay_plus') THEN bm.market_type ELSE bm.event_description END AS event_description
        , DATE_TRUNC('seconds',bm.event_timestamp AT time zone 'utc' AT time zone 'America/Toronto') AS event_date
        , bm.selection
        , SUM(transfer_to_sports) AS bet_placed_amount
        , -1.0*SUM(transfer_from_sports) AS bet_paid_amount
        , -1.0*SUM(voided_sport_wagers) AS void_bet_amount
        , -1.0*SUM(cancelled_sport_wagers) AS cancelled_bet_amount
        , SUM(resettled_sport_wagers) AS resettled_bet_adjustment
    FROM dataset AS d
    LEFT JOIN ca_aml_monthly_reports.bet_and_market_info AS bm
        USING(bet_id)
    JOIN report r
        ON d.patron_id = r.patron_id
    WHERE gaming_date >= reports_start_time_et_ca_aml_monthly()
        AND gaming_date < reports_end_time_et_ca_aml_monthly()
    GROUP BY 1,2,3,4,5,6,7,8,9
)

, ggr AS (
    SELECT
        account_number AS patron_id
        , SUM((bet_placed_amount +
        bet_paid_amount +
        void_bet_amount +
        cancelled_bet_amount +
        resettled_bet_adjustment))::NUMERIC(64,2)
        AS lt_ggr_cash
    FROM final_revenue
    WHERE (bet_placed_amount!=0 OR
        bet_paid_amount!=0 OR
        void_bet_amount!=0 OR
        cancelled_bet_amount!=0 OR
        resettled_bet_adjustment!=0)
    GROUP BY 1
)

, bets AS (
    SELECT 
        DATE_TRUNC('month',closed_at AT time zone 'utc' AT time zone 'America/Toronto') AS transaction_period  
        , b.patron_id 
        , COUNT(DISTINCT id) AS bets 
        , COUNT(DISTINCT CASE WHEN free_bet_id IS NOT NULL THEN id END) AS free_bets 
        , SUM(bet_amount_cents/100.0) AS settled_cash 
        , SUM(CASE WHEN free_bet_id IS NOT NULL THEN bet_amount_cents/100.0 END) AS settled_free 
    FROM ca_aml_monthly_reports.max_vegas_bets_all b
    JOIN report r
        ON r.patron_id = b.patron_id 
    WHERE status = 'settled'
    AND (
            b.closed_at >= reports_start_time_utc_ca_aml_monthly()
            AND b.closed_at < reports_end_time_utc_ca_aml_monthly()
        )
    GROUP BY 1,2
)

, lt_bets AS (
  SELECT 
    b.patron_id 
    , COUNT(DISTINCT id) AS lt_bets 
    , COUNT(DISTINCT CASE WHEN free_bet_id IS NOT NULL THEN id END) AS lt_free_bets 
    , SUM(bet_amount_cents/100.0) AS lt_settled_cash 
    , SUM(CASE WHEN free_bet_id IS NOT NULL THEN bet_amount_cents/100.0 ELSE 0.0 END) AS lt_settled_free 
    -- , SUM(CASE WHEN free_bet_id IS NULL THEN gross_gaming_revenue END) AS ggr_cash --how to calculate? cash+free-payout-cancelled-voided
  FROM ca_aml_monthly_reports.max_vegas_bets_all b
  JOIN report d
    ON d.patron_id = b.patron_id
  WHERE status = 'settled'
  GROUP BY 1
) 

SELECT
    d.transaction_period AS deposit_period
    , d.patron_id
    , d.username 
    , CASE
        WHEN d.employment_status IN ('retired', 'homemaker','student','unemployed')
        THEN true
        ELSE false
    END AS is_non_earning
    , d.first_name
    , d.last_name
    , d.job_title AS occupation 
    , d.employment_status AS occupation_type
    , d.employer_name
    , b.bets
    , b.free_bets 
    , b.settled_cash 
    , b.settled_free 
    , d.deposits 
    , d.deposit_amount
    , COALESCE(w.withdrawals,0) AS withdrawals 
    , COALESCE(w.withdrawal_amount,0) AS withdrawal_amount  
    , b.settled_cash / d.deposit_amount AS "Settled:Deposit" 
    , lb.lt_bets 
    , lb.lt_free_bets
    , lb.lt_settled_cash
    , lb.lt_settled_free
    , ld.lt_deposits 
    , ld.lt_deposit_amount 
    , COALESCE(lw.lt_withdrawals,0) AS lt_withdrawals  
    , COALESCE(lw.lt_withdrawal_amount,0) AS lt_withdrawal_amount
    , lb.lt_settled_cash / ld.lt_deposit_amount AS "LT Settled:Deposit"
    , ggr.lt_ggr_cash 
    , p.merchants
    , p.providers
    , p.payment_cards
    --   , i.install_ids
    , p.kyc_date 
    , p.last_login_at
FROM report d 
LEFT JOIN bets b 
  USING(patron_id, transaction_period)
LEFT JOIN lt_bets lb 
  USING(patron_id)
LEFT JOIN withdrawals w
  USING(patron_id, transaction_period) 
LEFT JOIN lt_withdrawals lw 
  USING(patron_id)
LEFT JOIN lt_deposits ld 
  USING(patron_id)  
LEFT JOIN payment_info p 
  USING(patron_id)
-- LEFT JOIN install_ids i 
--   USING(patron_id)
LEFT JOIN ggr 
  USING(patron_id)
WHERE d.transaction_period>= reports_start_time_et_ca_aml_monthly() --'2021-11-01'
AND d.transaction_period< reports_end_time_et_ca_aml_monthly() --'2021-12-01'
-- AND LOWER(u.employment_status) IN ('retired', 'homemaker','student','unemployed')
ORDER BY 1 DESC, deposit_amount DESC 
