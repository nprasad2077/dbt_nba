{{
    config(
        materialized='view',
        schema='staging',
        alias='stg_player_game_basic_stats'
    )
}}

WITH source_data AS (
    SELECT * FROM {{ source('raw_nba', 'player_game_basic_stats') }}
    WHERE deleted_at IS NULL
),

cleaned AS (
    SELECT
        -- Keys
        game_id,
        player_id,
        team,
        
        -- Player Information
        player_name,
        COALESCE(status, 'Unknown') AS player_status,
        CASE 
            WHEN status = 'Played' THEN TRUE
            ELSE FALSE
        END AS did_play,
        
        -- Minutes Played
        mp AS minutes_played_str,
        -- Convert MM:SS to decimal minutes
        -- FIX: Convert MM:SS to a numeric type with 2 decimal places
        CAST(
            CASE 
                WHEN mp IS NOT NULL AND mp != '' AND mp LIKE '%:%' THEN
                    CAST(SPLIT_PART(mp, ':', 1) AS NUMERIC) + 
                    (CAST(SPLIT_PART(mp, ':', 2) AS NUMERIC) / 60.0)
                WHEN mp IS NOT NULL AND mp != '' THEN
                    CAST(mp AS NUMERIC)
                ELSE 0
            END 
        AS NUMERIC(10, 2)) AS minutes_played,
        
        -- Shooting Stats
        COALESCE(fg, 0) AS field_goals_made,
        COALESCE(fga, 0) AS field_goals_attempted,
        COALESCE(fg_percent, 0) AS field_goal_pct,
        
        -- Three Point Shooting
        COALESCE(three_p, 0) AS three_pointers_made,
        COALESCE(three_pa, 0) AS three_pointers_attempted,
        COALESCE(three_p_percent, 0) AS three_point_pct,
        
        -- Free Throws
        COALESCE(ft, 0) AS free_throws_made,
        COALESCE(fta, 0) AS free_throws_attempted,
        COALESCE(ft_percent, 0) AS free_throw_pct,
        
        -- Rebounds
        COALESCE(orb, 0) AS offensive_rebounds,
        COALESCE(drb, 0) AS defensive_rebounds,
        COALESCE(trb, 0) AS total_rebounds,
        
        -- Other Stats
        COALESCE(ast, 0) AS assists,
        COALESCE(stl, 0) AS steals,
        COALESCE(blk, 0) AS blocks,
        COALESCE(tov, 0) AS turnovers,
        COALESCE(pf, 0) AS personal_fouls,
        
        -- Points
        COALESCE(pts, 0) AS points,
        
        -- Advanced
        COALESCE(gm_sc, 0) AS game_score,
        COALESCE(plus_minus, 0) AS plus_minus,
        
        -- Derived Stats
        -- Two point shots
        COALESCE(fg, 0) - COALESCE(three_p, 0) AS two_pointers_made,
        COALESCE(fga, 0) - COALESCE(three_pa, 0) AS two_pointers_attempted,
        CASE 
            WHEN (COALESCE(fga, 0) - COALESCE(three_pa, 0)) > 0 THEN
                (COALESCE(fg, 0) - COALESCE(three_p, 0))::NUMERIC / 
                (COALESCE(fga, 0) - COALESCE(three_pa, 0))::NUMERIC
            ELSE 0
        END AS two_point_pct,
        
        -- Scoring efficiency
        CASE 
            WHEN COALESCE(fga, 0) > 0 THEN
                COALESCE(pts, 0)::NUMERIC / COALESCE(fga, 0)::NUMERIC
            ELSE 0
        END AS points_per_shot,
        
        -- Double-doubles and Triple-doubles
        CASE 
            WHEN (
                (CASE WHEN COALESCE(pts, 0) >= 10 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(trb, 0) >= 10 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(ast, 0) >= 10 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(stl, 0) >= 10 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(blk, 0) >= 10 THEN 1 ELSE 0 END)
            ) >= 2 THEN TRUE
            ELSE FALSE
        END AS is_double_double,
        
        CASE 
            WHEN (
                (CASE WHEN COALESCE(pts, 0) >= 10 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(trb, 0) >= 10 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(ast, 0) >= 10 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(stl, 0) >= 10 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(blk, 0) >= 10 THEN 1 ELSE 0 END)
            ) >= 3 THEN TRUE
            ELSE FALSE
        END AS is_triple_double,
        
        -- Starter identification (typically play > 20 minutes)
        CASE 
            WHEN mp IS NOT NULL AND mp LIKE '%:%' AND
                 CAST(SPLIT_PART(mp, ':', 1) AS NUMERIC) >= 20 THEN TRUE
            ELSE FALSE
        END AS likely_starter,
        
        -- Metadata
        created_at,
        updated_at,
        CURRENT_TIMESTAMP AS dbt_loaded_at
        
    FROM source_data
    WHERE 
        game_id IS NOT NULL
        AND player_id IS NOT NULL
)

SELECT * FROM cleaned