--Reporting 2.0 Jackpots Model
--In report models when referencing jackpot payout then WHERE type = 'win', if contributions then no WHERE clause needed


--MAX_WALLET_TRANSACTIONS
WITH max_row_wallet_transactions AS (
    SELECT DISTINCT b.id
                  , MAX(b.updated_at) AS updated_at
    FROM casino_db.wallet_transactions b
    WHERE b.updated_at < to_timestamp('2023-02-15 00:00:00','yyyy-mm-dd hh24:mi:ss') + INTERVAL '2 MINUTES'
    GROUP BY 1
)
, wallet_transactions_temp AS (
    SELECT DISTINCT m.id,
                    m.updated_at,
                    MAX(b.updated_at) AS max_updated_at
    FROM casino_db.wallet_transactions  b
    INNER JOIN max_row_wallet_transactions m ON m.id = b.id AND m.updated_at = b.updated_at
    GROUP BY 1, 2
)
, max_wallet_transactions AS (
SELECT b.*
FROM casino_db.wallet_transactions  AS b
INNER JOIN wallet_transactions_temp AS m
ON b.id = m.id
AND b.updated_at = m.max_updated_at
AND b.updated_at = m.updated_at)

--MAX_WALLET_TRANSACTION_JACKPOTS
, max_row_wallet_transaction_jackpots AS (
    SELECT DISTINCT b.wallet_transaction_id
                  , b.jackpot_id
                  , MAX(b.updated_at) AS updated_at
    FROM casino_db.wallet_transaction_jackpots b
WHERE b.updated_at < to_timestamp('2023-02-15 00:00:00','yyyy-mm-dd hh24:mi:ss') + INTERVAL '2 MINUTES'
    GROUP BY 1,2
)
, max_wallet_transaction_jackpots_temp AS (
    SELECT DISTINCT m.wallet_transaction_id,
                    m.jackpot_id,
                    m.updated_at,
                    MAX(b.updated_at) AS max_updated_at
    FROM casino_db.wallet_transaction_jackpots  b
    INNER JOIN max_row_wallet_transaction_jackpots m ON m.wallet_transaction_id = b.wallet_transaction_id AND m.jackpot_id = b.jackpot_id AND m.updated_at = b.updated_at
    GROUP BY 1, 2, 3
)

, max_wallet_transaction_jackpots AS (
SELECT b.*
FROM casino_db.wallet_transaction_jackpots  AS b
INNER JOIN max_wallet_transaction_jackpots_temp AS m
ON m.wallet_transaction_id = b.wallet_transaction_id
AND m.jackpot_id = b.jackpot_id
AND b.updated_at = m.max_updated_at
AND b.updated_at = m.updated_at
)

--MAX_JACKPOTS
, max_row_jackpots AS (
    SELECT DISTINCT b.id
                  , MAX(b.updated_at) AS updated_at
    FROM casino_db.jackpots b
WHERE b.updated_at < to_timestamp('2023-02-15 00:00:00','yyyy-mm-dd hh24:mi:ss') + INTERVAL '2 MINUTES'
    GROUP BY 1
)
, max_jackpots_temp AS (
    SELECT DISTINCT m.id,
                    m.updated_at,
                    MAX(b.updated_at) AS max_updated_at
    FROM casino_db.jackpots b
    INNER JOIN max_row_jackpots m ON m.id = b.id AND m.updated_at = b.updated_at
    GROUP BY 1, 2
)

, max_jackpots AS (
SELECT b.*
FROM casino_db.jackpots  AS b
INNER JOIN max_jackpots_temp AS m
ON b.id = m.id
AND b.updated_at = m.max_updated_at
AND b.updated_at = m.updated_at
    )

SELECT DISTINCT  j.wallet_transaction_id
, t.identity_user_id AS patron_id
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
JOIN casino_db.jackpots jp
    ON jp.id = j.jackpot_id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16;
