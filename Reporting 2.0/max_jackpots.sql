SELECT DISTINCT j._changed_at
, j.wallet_transaction_id
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
FROM ca_on_reports.max_wallet_transactions t
JOIN ca_on_reports.max_wallet_transaction_jackpots j
    ON j.wallet_transaction_id = t.id
JOIN ca_on_reports.max_jackpots jp
    ON jp.id = j.jackpot_id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
;
