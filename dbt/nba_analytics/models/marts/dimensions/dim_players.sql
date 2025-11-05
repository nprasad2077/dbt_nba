{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH player_game_stats AS (
    SELECT
        stats.player_id,
        stats.player_name,
        games.game_date,
        -- Rank each player's name records by game date, with the most recent being #1
        ROW_NUMBER() OVER (PARTITION BY stats.player_id ORDER BY games.game_date DESC) as rn
    FROM {{ ref('stg_player_game_basic_stats') }} AS stats
    LEFT JOIN {{ ref('stg_games') }} AS games
        ON stats.game_id = games.game_id
)

SELECT
    -- Surrogate Key: A unique key for the player based on their ID
    {{ dbt_utils.generate_surrogate_key(['player_id']) }} AS player_key,

    -- Player Attributes
    player_id,
    player_name

FROM player_game_stats
-- Filter to only the most recent record for each player
WHERE rn = 1
ORDER BY player_name
