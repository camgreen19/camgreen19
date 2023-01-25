WITH settled_bets AS (
    SELECT DISTINCT patron_id
        , id::VARCHAR as wager_id
        , b.free_bet_id::VARCHAR AS promo_free_wager_id
        , outcome
        , False as is_jackpot
        , bet_amount_cents/100.0 AS wager_amount
        , payout_amount_cents/100.0 AS payout_amount
        -- DATA-9008
        -- add is_jackpot(boolean), jackpot_payout_amount to be consistent with casino_dataset
        , 0.0 AS jackpot_payout_amount
        , placed_at
        , closed_at
        , regraded
        , gaming_state
        , 'Sports' AS product
        , 'Sports' AS game_category
      FROM ca_finance_reports.max_vegas_bets_all b
    WHERE b.free_bet_id IS NOT NULL
    AND status = 'settled'
    AND outcome != 'push'
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


, casino_free_wagers AS (
    SELECT w.*
    FROM ca_finance_reports.max_wager_and_game_info w
    JOIN ca_finance_reports.max_patron_transactions_all pt ON w.patron_transaction_id = pt.id
    WHERE w.type = 'wager' and promo_engine_free_round_award_id IS NOT NULL
        AND pt._changed_at < reports_end_time_utc_ca_finance()
        AND pt._changed_at >= reports_start_time_utc_ca_finance()
)

, casino_completed_wagers AS (
    SELECT w.*
    , CASE WHEN a.type = 'payout' THEN a.patron_transaction_id END as payout_patron_transaction_id
    , CASE WHEN a.type = 'rollback' THEN a.patron_transaction_id END as rollback_patron_transaction_id
    , CASE WHEN a.type = 'payout' THEN a.amount_cents END as payout_amount_cents
    -- DATA-9008 add jackpot amount to transaction level
    , COALESCE(j.amount_precision, 0.0) as jackpot_payout
    , CASE WHEN a.type = 'rollback' THEN a.amount_cents END as rollback_amount_cents
    , CASE WHEN a.type = 'rollback' THEN TRUE ELSE FALSE END as rollback_flag
    , CASE WHEN a.type = 'payout' AND a.amount_cents > 0 THEN  'win'
            WHEN a.type = 'payout' AND a.amount_cents = 0 THEN 'loss'
            WHEN a.type = 'rollback' then 'rollback'
        END AS outcome
    , CASE WHEN j.amount_precision IS NOT NULL THEN True ELSE False END as is_jackpot
    FROM casino_free_wagers w
    LEFT JOIN ca_finance_reports.max_wager_and_game_info a
        ON w.patron_transaction_id::VARCHAR = a.parent_id AND a.type IN ('payout','rollback')
    LEFT JOIN JACKPOTS_TEST j
        ON a.patron_transaction_id::VARCHAR = j.remote_wallet_transaction_id
    WHERE a.parent_id IS NOT NULL and j.type = 'win'
)

, free_wager_metadata AS (
    SELECT DISTINCT w.gaming_state
    , w._changed_at as wager_timestamp
    , p._changed_at as payout_timestamp
    , r._changed_at as rollback_timestamp
    ,c.*
    FROM casino_completed_wagers c
    JOIN ca_finance_reports.max_casino_wagers_all w ON w.patron_transaction_id::VARCHAR = c.patron_transaction_id::VARCHAR
    LEFT JOIN ca_finance_reports.max_casino_payouts_all p ON p.patron_transaction_id::VARCHAR = c.payout_patron_transaction_id::VARCHAR
    LEFT JOIN ca_finance_reports.max_casino_wager_rollbacks_all r ON r.patron_transaction_id::VARCHAR = c.rollback_patron_transaction_id::VARCHAR
)

, casino_dataset AS (
    SELECT DISTINCT patron_id
        , patron_transaction_id::VARCHAR AS wager_id
        , promo_engine_free_round_award_id::VARCHAR AS promo_free_wager_id
        , outcome
        , is_jackpot -- DATA-9008
        , amount_cents/100.0 AS wager_amount
        -- DATA-9008 back out jackpot payout amount from payout_amount
        , (COALESCE(rollback_amount_cents, payout_amount_cents, 0.0)/100.0 - jackpot_payout) as payout_amount
        , jackpot_payout AS jackpot_payout_amount
        , wager_timestamp AS placed_at
        , GREATEST(wager_timestamp,payout_timestamp,rollback_timestamp) AS closed_at
        , rollback_flag AS regraded
        , gaming_state
        , 'Casino' AS product
        , game_category
    FROM free_wager_metadata
)

, final_dataset AS (
    SELECT * FROM settled_bets

    UNION ALL

    SELECT * FROM casino_dataset
)

, c_free_wager_awards AS (
    SELECT distinct 'free bet' as type
        , a.id as free_wager_id
        ,first_value(a.user_id)
            over(partition by a.id order by a.updated_at desc rows between unbounded preceding and current row) as customer_id
        ,first_value(b.id)
            over(partition by a.id order by a.updated_at desc rows between unbounded preceding and current row) as award_program_id
        ,first_value(awarded_at at time zone 'utc' at time zone 'America/Toronto')
            over(partition by a.id order by a.updated_at desc rows between unbounded preceding and current row) as awarded_date
        ,first_value(b.amount_cents/100.0)
            over(partition by a.id order by a.updated_at desc rows between unbounded preceding and current row) as coupon_amount
    FROM ca_promotions_snapshot.max_user_award_free_bets a
    JOIN ca_promotions_snapshot.max_award_free_bets b
        ON a.award_free_bet_id = b.id

    UNION ALL

    SELECT distinct 'free wager' as type
        , a.id as free_wager_id
        ,first_value(a.user_id)
            over(partition by a.id order by a.updated_at desc rows between unbounded preceding and current row) as customer_id
        ,first_value(b.id)
            over(partition by a.id order by a.updated_at desc rows between unbounded preceding and current row) as award_program_id
        ,first_value(awarded_at at time zone 'utc' at time zone 'America/Toronto')
            over(partition by a.id order by a.updated_at desc rows between unbounded preceding and current row) as awarded_date
        ,first_value(b.amount_cents/100.0)
            over(partition by a.id order by a.updated_at desc rows between unbounded preceding and current row) as coupon_amount
    FROM ca_promotions_snapshot.max_user_award_free_wagers a
    JOIN ca_promotions_snapshot.max_award_free_wagers b
        ON a.award_free_wager_id = b.id

)

, c_award_programs AS (
    SELECT distinct 'free bet award' as type
        ,b.id as award_program_id
        ,first_value(a.id)
            over(partition by b.id order by a.updated_at desc rows between unbounded preceding and current row) as program_code
        ,first_value(a.name)
            over(partition by b.id order by a.updated_at desc rows between unbounded preceding and current row) as program_name
        ,first_value(a.promotion_type)
            over(partition by b.id order by a.updated_at desc rows between unbounded preceding and current row) as award_type
    FROM ca_promotions_snapshot.max_promotions a
    JOIN ca_promotions_snapshot.max_award_free_bets b
        ON a.id = b.promotion_id

    UNION ALL

    SELECT distinct 'free wager award' as type
        ,b.id as award_program_id
        ,first_value(a.id)
            over(partition by b.id order by a.updated_at desc rows between unbounded preceding and current row) as program_code
        ,first_value(a.name)
            over(partition by b.id order by a.updated_at desc rows between unbounded preceding and current row) as program_name
        ,first_value(a.promotion_type)
            over(partition by b.id order by a.updated_at desc rows between unbounded preceding and current row) as award_type
    FROM ca_promotions_snapshot.max_promotions a
    JOIN ca_promotions_snapshot.max_award_free_wagers b
        ON a.id = b.promotion_id
)

SELECT DISTINCT DATE_TRUNC('seconds',s.closed_at at time zone 'utc' at time zone 'America/Toronto') as "Settled Timestamp Toronto"
    , s.patron_id AS "Patron ID"
    , s.product AS "Product"
    ,  (CASE WHEN s.game_category = 'Sports' THEN s.game_category
                        WHEN s.game_category='live' THEN 'Live Dealer'
                        WHEN s.game_category='roulette' THEN 'Table'
                        ELSE UPPER(LEFT(s.game_category, 1))|| LOWER(SUBSTRING(s.game_category, 2, LENGTH(s.game_category))) END
                        ) AS "Game Type"
    , w.award_program_id AS "Award Program ID"
    , coalesce(a.program_name ,'Manual Award') as "Program Name"
    , s.wager_id AS "Wager ID"
    , s.promo_free_wager_id AS "Promo Free Wager ID"
    , DATE_TRUNC('seconds',s.placed_at at time zone 'utc' at time zone 'America/Toronto') AS "Placed At Timestamp Toronto"
    , s.outcome AS "Outcome"
    , s.wager_amount AS "Wager Amount"
    -- DATA-9008 precise payout amount to transaction level
    , s.payout_amount AS "Payout Amount"
    -- DATA-9008 add new column "Jackpot Win Amount"
    , s.jackpot_payout_amount AS "Jackpot Win Amount"
    , s.gaming_state AS "Gaming State"
    , s.regraded::bool AS "Regrade"
    -- DATA-9008 add new column "Jackpot Win" (boolean)
    , s.is_jackpot AS "Jackpot Win"
    , GETDATE() AT time zone 'utc' AT time zone 'America/Toronto' AS report_run_timestamp_toronto
FROM final_dataset s
JOIN ca_finance_reports.max_identity_users AS u
    ON s.patron_id=u.patron_id
LEFT JOIN c_free_wager_awards w
  ON w.free_wager_id::VARCHAR = s.promo_free_wager_id
LEFT JOIN c_award_programs a
  ON a.award_program_id::varchar = w.award_program_id::varchar
WHERE (
      (s.closed_at at time zone 'utc' at time zone 'America/Toronto') BETWEEN
      reports_start_time_et_ca_finance()
      AND reports_end_time_et_ca_finance()
      )
    --AND is_tester='false'
