WITH max_row_bets AS NOT MATERIALIZED (
    SELECT DISTINCT id
                  , MAX(_changed_at) AS _changed_at
    FROM edgebook_db_in_history.bets
    WHERE _changed_at < reports_end_time_utc_in()
    GROUP BY 1
)
, temp AS NOT MATERIALIZED (
    SELECT DISTINCT m.id,
                    m._changed_at,
                    MAX(updated_at) updated_at
    FROM edgebook_db_in_history.bets  b
    INNER JOIN max_row_bets m ON m.id = b.id AND m._changed_at = b._changed_at
    GROUP BY 1, 2
)

SELECT u.first_name, 
u.last_name,
b.patron_id,
b.id,
b.type,
b.price,
b.bet_amount_cents,
b.win_amount_cents,
b.payout_amount_cents,
COALESCE(CAST (b.free_bet_id AS VARCHAR), CAST(b.promo_engine_free_bet_id AS VARCHAR)) as free_bet_id,
b.status,
b.outcome,
b.placed_at,
b.closed_at
FROM edgebook_db_in_history.bets  AS b
INNER JOIN temp AS m
ON b.id = m.id
AND b._changed_at = m._changed_at
AND b.updated_at = m.updated_at
LEFT JOIN in_reports.max_identity_users u ON b.patron_id = u.patron_id
WHERE b.patron_id IN ('da57358c-a075-4abb-b8d5-1980355900bb',
                    '497eea56-00e1-47c7-a5fb-eac737ab6c64',
                    '78745faf-9408-4126-834e-5753b2758d5d',
                    '510655a3-7d3b-4aae-a7f1-0649ef8b0673',
                    'e17f6601-8985-4709-9c1e-c7ed23cb751f',
                    '61f4aa6a-5b8e-4a33-8067-1b44d42302b0',
                    'e5d24faf-6898-49ad-b05d-16efbbd75e94',
                    '2c0f3671-d0b9-4fe2-b0d3-47d4867d2db8',
                    '473445f6-2395-4208-9f1e-cefa28b8d47c',
                    '4a97c2c6-ea01-4df8-9a0e-0b0672c7a0c2',
                    '5a112216-8ee1-44d0-aa6b-fa44c36718ae',
                    '82058583-3afc-4e23-89e2-396e0bc28bab',
                    'af84d578-6e88-4fe3-95e3-ad30f9a337d4',
                    'fc091a18-d6ac-4086-86bf-5b3dd20dcff7',
                    '298a094c-08eb-48d8-a564-e4f1678c8306',
                    'a9f3344e-a446-4016-919f-1b9ba715a3d8',
                    '1ed6b82d-7d0b-49a2-8786-43fa5aefdaa9')
ORDER BY b.patron_id, b.id, b._changed_at;
