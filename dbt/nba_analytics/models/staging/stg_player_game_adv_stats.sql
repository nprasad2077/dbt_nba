{{
    config(
        materialized='view',
        schema='staging',
        alias='stg_player_game_adv_stats'
    )
}}

WITH source_data AS (
    SELECT * FROM {{ source('raw_nba', 'player_game_adv_stats') }}
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
        
        -- Minutes Played
        mp AS minutes_played_str,
        -- Convert MM:SS to decimal minutes
        CASE 
            WHEN mp IS NOT NULL AND mp != '' AND mp LIKE '%:%' THEN
                CAST(SPLIT_PART(mp, ':', 1) AS NUMERIC) + 
                (CAST(SPLIT_PART(mp, ':', 2) AS NUMERIC) / 60.0)
            WHEN mp IS NOT NULL AND mp != '' THEN
                CAST(mp AS NUMERIC)
            ELSE 0
        END AS minutes_played,
        
        -- Shooting Efficiency
        COALESCE(ts_percent, 0) AS true_shooting_pct,
        COALESCE(efg_percent, 0) AS effective_fg_pct,
        
        -- Shot Selection
        COALESCE(three_p_ar, 0) AS three_point_attempt_rate,
        COALESCE(f_tr, 0) AS free_throw_rate,
        
        -- Rebounding Percentages
        COALESCE(orb_percent, 0) AS offensive_rebound_pct,
        COALESCE(drb_percent, 0) AS defensive_rebound_pct,
        COALESCE(trb_percent, 0) AS total_rebound_pct,
        
        -- Playmaking & Defense
        COALESCE(ast_percent, 0) AS assist_pct,
        COALESCE(stl_percent, 0) AS steal_pct,
        COALESCE(blk_percent, 0) AS block_pct,
        COALESCE(tov_percent, 0) AS turnover_pct,
        
        -- Usage & Impact
        COALESCE(usg_percent, 0) AS usage_pct,
        COALESCE(o_rtg, 0) AS offensive_rating,
        COALESCE(d_rtg, 0) AS defensive_rating,
        COALESCE(bpm, 0) AS box_plus_minus,
        
        -- Derived Advanced Metrics
        COALESCE(o_rtg, 0) - COALESCE(d_rtg, 0) AS net_rating,
        
        -- Efficiency Categories
        CASE 
            WHEN COALESCE(ts_percent, 0) >= 0.60 THEN 'Elite'
            WHEN COALESCE(ts_percent, 0) >= 0.55 THEN 'Good'
            WHEN COALESCE(ts_percent, 0) >= 0.50 THEN 'Average'
            ELSE 'Below Average'
        END AS shooting_efficiency_tier,
        
        CASE 
            WHEN COALESCE(usg_percent, 0) >= 30 THEN 'Primary Option'
            WHEN COALESCE(usg_percent, 0) >= 25 THEN 'Secondary Option'
            WHEN COALESCE(usg_percent, 0) >= 20 THEN 'Role Player'
            ELSE 'Limited Role'
        END AS usage_tier,
        
        -- Player Impact Categories
        CASE 
            WHEN COALESCE(bpm, 0) >= 10 THEN 'Elite Impact'
            WHEN COALESCE(bpm, 0) >= 5 THEN 'High Impact'
            WHEN COALESCE(bpm, 0) >= 0 THEN 'Positive Impact'
            WHEN COALESCE(bpm, 0) >= -5 THEN 'Negative Impact'
            ELSE 'Very Negative Impact'
        END AS impact_tier,
        
        -- Versatility Indicators
        CASE 
            WHEN COALESCE(ast_percent, 0) >= 20 
                 AND COALESCE(trb_percent, 0) >= 15 THEN TRUE
            ELSE FALSE
        END AS is_versatile,
        
        -- Defensive Specialist Indicator
        CASE 
            WHEN (COALESCE(stl_percent, 0) >= 2.5 OR COALESCE(blk_percent, 0) >= 4)
                 AND COALESCE(d_rtg, 0) < 105 THEN TRUE
            ELSE FALSE
        END AS is_defensive_specialist,
        
        -- Three and D Player
        CASE 
            WHEN COALESCE(three_p_ar, 0) >= 0.4 
                 AND COALESCE(d_rtg, 0) < 110 THEN TRUE
            ELSE FALSE
        END AS is_three_and_d,
        
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