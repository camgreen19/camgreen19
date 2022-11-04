WITH parlays AS (
    SELECT DISTINCT vegas_bet_id AS bet_id,
           b.type AS market_type,
           b.type AS bet_type,
           b.type AS league,
           b.type AS event_type,
           NULL AS event_description,
           NULL::timestamp AS event_timestamp,
           NULL AS selection,
           MAX(l.num_legs) num_legs,
           MAX(l.num_live_legs) num_live_legs
    FROM ca_aml_reports.max_vegas_bets_all b
    LEFT JOIN ca_aml_reports.max_legs_all l
    ON b.id=l.vegas_bet_id
    WHERE b.type in ('parlay','parlay_plus')
    GROUP BY 1, b.type
)
, straights AS (
    SELECT DISTINCT vegas_bet_id AS bet_id,
           REPLACE(market, '\|', '|')::json->>'name' AS market_type,
           CASE WHEN market_classification='main'         THEN 'Straight Wager'
                WHEN market_classification='player_prop'  THEN 'Future Wager'
                WHEN market_classification='game_prop'    THEN 'Future Wager'
                WHEN market_classification='future'       THEN 'Future Wager'
                WHEN market_classification='promo'        THEN 'Future Wager' END AS bet_type,
           UPPER(REPLACE(REPLACE(l.event, '\|', '|')::json->>'competition', '\|', '|')::json->>'name') AS league,
           INITCAP(REPLACE(REPLACE(l.event, '\|', '|')::json->>'sport', '\|', '|')::json->>'name') AS event_type,
           REPLACE(l.event, '\|', '|')::json->>'name' AS event_description,
           (REPLACE(l.event, '\|', '|')::json->>'start_time')::timestamp AS event_timestamp,
           l.market_selection_name AS selection,
           l.num_legs AS num_legs,
           l.num_live_legs AS num_live_legs
    FROM ca_aml_reports.max_vegas_bets_all b
    LEFT JOIN ca_aml_reports.max_legs_all l
    ON b.id=l.vegas_bet_id
    WHERE b.type='straight'
)

SELECT * FROM straights
UNION ALL
SELECT * FROM parlays
