{{
    config(
        materialized='table',
        schema='staging',
        alias='stg_team_game_adv_stats'
    )
}}

WITH source_data AS (
    SELECT * FROM {{ source('raw_nba', 'team_game_adv_stats') }}
    WHERE deleted_at IS NULL
),

cleaned AS (
    SELECT
        -- Keys
        game_id,
        team,
        
        -- Minutes (should typically be 240 for regulation)
        COALESCE(mp, 240) AS minutes_played,
        
        -- Shooting Efficiency
        COALESCE(ts_percent, 0) AS true_shooting_pct,
        COALESCE(efg_percent, 0) AS effective_fg_pct,
        
        -- Shot Selection
        COALESCE(three_p_ar, 0) AS three_point_attempt_rate,
        COALESCE(f_tr, 0) AS free_throw_rate,
        
        -- Rebounding
        COALESCE(orb_percent, 0) AS offensive_rebound_pct,
        COALESCE(drb_percent, 0) AS defensive_rebound_pct,
        COALESCE(trb_percent, 0) AS total_rebound_pct,
        
        -- Ball Movement & Defense
        COALESCE(ast_percent, 0) AS assist_pct,
        COALESCE(stl_percent, 0) AS steal_pct,
        COALESCE(blk_percent, 0) AS block_pct,
        COALESCE(tov_percent, 0) AS turnover_pct,
        
        -- Usage (should always be 100 for team)
        COALESCE(usg_percent, 100) AS usage_pct,
        
        -- Ratings
        COALESCE(o_rtg, 0) AS offensive_rating,
        COALESCE(d_rtg, 0) AS defensive_rating,
        
        -- Derived Metrics
        COALESCE(o_rtg, 0) - COALESCE(d_rtg, 0) AS net_rating,
        
        -- Performance Categories
        CASE 
            WHEN COALESCE(o_rtg, 0) >= 120 THEN 'Elite Offense'
            WHEN COALESCE(o_rtg, 0) >= 112 THEN 'Good Offense'
            WHEN COALESCE(o_rtg, 0) >= 104 THEN 'Average Offense'
            ELSE 'Below Average Offense'
        END AS offensive_tier,
        
        CASE 
            WHEN COALESCE(d_rtg, 0) <= 104 THEN 'Elite Defense'
            WHEN COALESCE(d_rtg, 0) <= 112 THEN 'Good Defense'
            WHEN COALESCE(d_rtg, 0) <= 120 THEN 'Average Defense'
            ELSE 'Below Average Defense'
        END AS defensive_tier,
        
        -- Play Style Indicators
        CASE 
            WHEN COALESCE(three_p_ar, 0) >= 0.41 THEN 'Three Point Heavy'
            WHEN COALESCE(three_p_ar, 0) >= 0.3 THEN 'Balanced'
            ELSE 'Inside Focused'
        END AS shot_selection_style,
        
        CASE 
            WHEN COALESCE(ast_percent, 0) >= 65 THEN 'High Ball Movement'
            WHEN COALESCE(ast_percent, 0) >= 55 THEN 'Average Ball Movement'
            ELSE 'Isolation Heavy'
        END AS ball_movement_style,
        
        -- Rebounding Dominance
        CASE 
            WHEN COALESCE(orb_percent, 0) >= 29 THEN 'Elite Offensive Rebounding'
            WHEN COALESCE(orb_percent, 0) >= 24 THEN 'Good Offensive Rebounding'
            ELSE 'Average Offensive Rebounding'
        END AS offensive_rebounding_tier,
        
        CASE 
            WHEN COALESCE(drb_percent, 0) >= 81 THEN 'Elite Defensive Rebounding'
            WHEN COALESCE(drb_percent, 0) >= 76 THEN 'Good Defensive Rebounding'
            ELSE 'Average Defensive Rebounding'
        END AS defensive_rebounding_tier,
        
        -- Turnover Control
        CASE 
            WHEN COALESCE(tov_percent, 0) <= 12.5 THEN 'Excellent Ball Security'
            WHEN COALESCE(tov_percent, 0) <= 15 THEN 'Good Ball Security'
            ELSE 'Poor Ball Security'
        END AS ball_security_tier,
        
        -- Defensive Activity
        CASE 
            WHEN (COALESCE(stl_percent, 0) + COALESCE(blk_percent, 0)) >= 20 THEN 'Very Active Defense'
            WHEN (COALESCE(stl_percent, 0) + COALESCE(blk_percent, 0)) >= 15 THEN 'Active Defense'
            ELSE 'Passive Defense'
        END AS defensive_activity,
        
        -- Metadata
        created_at,
        updated_at,
        CURRENT_TIMESTAMP AS dbt_loaded_at
        
    FROM source_data
    WHERE 
        game_id IS NOT NULL
        AND team IS NOT NULL
)

SELECT * FROM cleaned