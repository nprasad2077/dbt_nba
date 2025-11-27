{{
    config(
        materialized='table',
        schema='staging',
        alias='stg_player_game_adv_stats_extended'
    )
}}

-- Define minutes thresholds as variables for clarity and maintainability (Foundation from A)
-- We add the exact quantile values for the new `minutes_quartile` column (from B)
{% set minutes_insufficient = 5 %}
{% set p5_threshold = 7 %}         -- Based on 5th percentile of 6.87
{% set q1_threshold_val = 16.68 %}
{% set q1_threshold_cat = 17 %}    -- Rounded for categorization
{% set median_threshold_val = 24.67 %}
{% set median_threshold_cat = 25 %}-- Rounded for categorization
{% set q3_threshold_val = 32.18 %}
{% set q3_threshold_cat = 32 %}    -- Rounded for categorization
{% set p95_threshold = 39 %}       -- Based on 95th percentile of 39.25

WITH source_data AS (
    SELECT * FROM {{ source('raw_nba', 'player_game_adv_stats') }}
    WHERE deleted_at IS NULL
),

with_minutes AS (
    SELECT
        *,
        -- Robust casting from Solution A
        CASE 
            WHEN mp IS NOT NULL AND mp != '' AND mp LIKE '%:%' THEN
                CAST(SPLIT_PART(mp, ':', 1) AS NUMERIC) + (CAST(SPLIT_PART(mp, ':', 2) AS NUMERIC) / 60.0)
            WHEN mp IS NOT NULL AND mp != '' THEN
                CASE WHEN mp ~ '^[0-9\.]+$' THEN CAST(mp AS NUMERIC) ELSE 0 END
            ELSE 0
        END AS minutes_played
    FROM source_data
),

cleaned AS (
    SELECT
        -- Keys & Base Info
        game_id,
        player_id,
        team,
        player_name,
        mp AS minutes_played_str,
        minutes_played,
        
        -- Base Metrics
        COALESCE(ts_percent, 0) AS true_shooting_pct,
        COALESCE(efg_percent, 0) AS effective_fg_pct,
        COALESCE(three_p_ar, 0) AS three_point_attempt_rate,
        COALESCE(f_tr, 0) AS free_throw_rate,
        COALESCE(orb_percent, 0) AS offensive_rebound_pct,
        COALESCE(drb_percent, 0) AS defensive_rebound_pct,
        COALESCE(trb_percent, 0) AS total_rebound_pct,
        COALESCE(ast_percent, 0) AS assist_pct,
        COALESCE(stl_percent, 0) AS steal_pct,
        COALESCE(blk_percent, 0) AS block_pct,
        COALESCE(tov_percent, 0) AS turnover_pct,
        COALESCE(usg_percent, 0) AS usage_pct,
        COALESCE(o_rtg, 0) AS offensive_rating,
        COALESCE(d_rtg, 0) AS defensive_rating,
        COALESCE(bpm, 0) AS box_plus_minus,
        
        -- Derived Metric
        COALESCE(o_rtg, 0) - COALESCE(d_rtg, 0) AS net_rating,
        
        -- Tiering & Categorization (Best of A & B)
        
        -- `minutes_based_role` - Adopting the more granular logic from Solution B
        CASE
            WHEN minutes_played >= {{ p95_threshold }} THEN 'Elite Minutes (Top 5%)'
            WHEN minutes_played >= {{ q3_threshold_cat }} THEN 'Starter'
            WHEN minutes_played >= {{ median_threshold_cat }} THEN 'Key Rotation'
            WHEN minutes_played >= {{ q1_threshold_cat }} THEN 'Regular Rotation'
            WHEN minutes_played >= {{ p5_threshold }} THEN 'Deep Bench'
            WHEN minutes_played >= {{ minutes_insufficient }} THEN 'Garbage Time'
            ELSE 'Insufficient Minutes'
        END AS minutes_based_role,

        -- `minutes_quartile` - Excellent analytical column from Solution B
        CASE
            WHEN minutes_played < {{ minutes_insufficient }} THEN 'Insufficient'
            WHEN minutes_played < {{ q1_threshold_val }} THEN 'Q1'
            WHEN minutes_played < {{ median_threshold_val }} THEN 'Q2'
            WHEN minutes_played < {{ q3_threshold_val }} THEN 'Q3'
            ELSE 'Q4'
        END AS minutes_quartile,
        
        -- Other Tiers (Logic is similar, using variables for consistency)
        CASE 
            WHEN minutes_played < {{ minutes_insufficient }} THEN 'Insufficient Minutes'
            WHEN usg_percent >= 25 AND minutes_played >= {{ q3_threshold_cat }} THEN 'Primary Option'
            WHEN usg_percent >= 20 AND minutes_played >= {{ median_threshold_cat }} THEN 'Secondary Option'
            WHEN minutes_played >= {{ q1_threshold_cat }} THEN 'Role Player'
            ELSE 'Limited Minutes'
        END AS usage_tier,

        CASE 
            WHEN minutes_played < {{ minutes_insufficient }} THEN 'Insufficient Minutes'
            WHEN COALESCE(bpm, 0) >= 10 THEN 'Elite Impact'
            WHEN COALESCE(bpm, 0) >= 5 THEN 'High Impact'
            WHEN COALESCE(bpm, 0) >= 0 THEN 'Positive Impact'
            ELSE 'Negative Impact'
        END AS impact_tier,

        -- Boolean Flags (Using Q1 as the "meaningful minutes" threshold)
        minutes_played >= {{ q1_threshold_cat }} 
            AND COALESCE(ast_percent, 0) >= 20 
            AND COALESCE(trb_percent, 0) >= 15 AS is_versatile,
        
        minutes_played >= {{ q1_threshold_cat }}
            AND (COALESCE(stl_percent, 0) >= 2.5 OR COALESCE(blk_percent, 0) >= 4)
            AND COALESCE(d_rtg, 0) < 105 AS is_defensive_specialist,
            
        minutes_played >= {{ q1_threshold_cat }}
            AND COALESCE(three_p_ar, 0) >= 0.4 
            AND COALESCE(d_rtg, 0) < 110 AS is_three_and_d,

        -- New boolean flags from Solution B, implemented with Jinja variables
        minutes_played >= {{ p95_threshold }} AS is_high_minutes_outlier,
        
        minutes_played >= {{ q1_threshold_cat }} AND minutes_played < {{ q3_threshold_cat }} AS is_regular_rotation,

        -- Metadata
        created_at,
        updated_at,
        CURRENT_TIMESTAMP AS dbt_loaded_at
        
    FROM with_minutes
    WHERE 
        game_id IS NOT NULL
        AND player_id IS NOT NULL
)

SELECT * FROM cleaned
