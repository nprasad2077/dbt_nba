{{
    config(
        materialized='table',
        schema='staging',
        alias='stg_player_game_adv_stats_extended'
    )
}}

-- =================================================================
-- THRESHOLD DEFINITIONS
-- =================================================================

-- Minutes Played Thresholds (from previous analysis)
{% set minutes_insufficient = 5 %}
{% set p5_threshold = 6 %}         -- Based on 5th percentile of 6.87
{% set q1_threshold_val = 16.18 %}
{% set q1_threshold_cat = 16 %}     -- Rounded for categorization
{% set median_threshold_val = 24.38 %}
{% set median_threshold_cat = 24 %}  -- Rounded for categorization
{% set q3_threshold_val = 32.18 %}
{% set q3_threshold_cat = 32 %}    -- Rounded for categorization
{% set p90_threshold = 36 %}      -- 90th percentile
{% set p95_threshold = 39 %}       -- Based on 95th percentile of 39.25

-- Usage Percentage Thresholds (from new analysis)
{% set usg_p5_threshold = 7.3 %}    -- 5th percentile
{% set usg_q1_threshold = 13.4 %}   -- Q1
{% set usg_median_threshold = 18.4 %} -- Median
{% set usg_q3_threshold = 24.2 %}   -- Q3
{% set usg_p90_threshold = 30.2 %} -- 90th percentile
{% set usg_p95_threshold = 33.1 %} -- 95th percentile

-- Box Plus/Minus (BPM) Thresholds
{% set bpm_p5_threshold = -15.6 %}   -- 5th percentile
{% set bpm_q1_threshold = -6.3 %}    -- Q1
{% set bpm_median_threshold = -0.6 %} -- Median
{% set bpm_q3_threshold = 4.8 %}     -- Q3
{% set bpm_p90_threshold = 10.1 %}   -- 90th percentile
{% set bpm_p95_threshold = 13.9 %}   -- 95th percentile

-- Shooting Efficiency Thresholds
{% set ts_p5_threshold = 1 %}
{% set ts_q1_threshold = 40 %}
{% set ts_median_threshold = 53 %}
{% set ts_q3_threshold = 68 %}
{% set ts_p90_threshold = 80 %}
{% set ts_p95_threshold = 95 %}

-- =================================================================
-- MODEL LOGIC
-- =================================================================

WITH source_data AS (
    SELECT * FROM {{ source('raw_nba', 'player_game_adv_stats') }}
    WHERE deleted_at IS NULL
),

with_minutes AS (
    SELECT
        *,
        -- Robust casting for minutes played
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
        
        -- =================================================================
        -- TIERING & CATEGORIZATION
        -- =================================================================
        
        -- Minutes-based role
        CASE
            WHEN minutes_played >= {{ p95_threshold }} THEN 'Elite Minutes (Top 5%)'
            WHEN minutes_played >= {{ p90_threshold }} THEN 'Elite Minutes (Top 10%)'
            WHEN minutes_played >= {{ q3_threshold_cat }} THEN 'Starter'
            WHEN minutes_played >= {{ median_threshold_cat }} THEN 'Key Rotation'
            WHEN minutes_played >= {{ q1_threshold_cat }} THEN 'Regular Rotation'
            WHEN minutes_played >= {{ p5_threshold }} THEN 'Deep Bench'
            WHEN minutes_played >= {{ minutes_insufficient }} THEN 'Garbage Time'
            ELSE 'Insufficient Minutes'
        END AS minutes_based_role,

        -- Minutes quartile
        CASE
            WHEN minutes_played < {{ minutes_insufficient }} THEN 'Insufficient'
            WHEN minutes_played < {{ q1_threshold_val }} THEN 'Q1'
            WHEN minutes_played < {{ median_threshold_val }} THEN 'Q2'
            WHEN minutes_played < {{ q3_threshold_val }} THEN 'Q3'
            ELSE 'Q4'
        END AS minutes_quartile,
        
        -- ENHANCED Usage Tier
        CASE 
            -- First, handle cases with insufficient playing time
            WHEN minutes_played < {{ minutes_insufficient }} THEN 'Insufficient Minutes'
            
            -- Heliocentric Option: Elite usage (top 10%) with meaningful minutes
            WHEN COALESCE(usg_percent, 0) >= {{ usg_p90_threshold }} 
                AND minutes_played >= {{ q1_threshold_cat }} THEN 'Heliocentric Option'
            
            -- Primary Option: High usage (Q3+) with starter minutes
            WHEN COALESCE(usg_percent, 0) >= {{ usg_q3_threshold }} 
                AND minutes_played >= {{ q3_threshold_cat }} THEN 'Primary Option'
            
            -- Secondary Option: Above-median usage with key rotation minutes
            WHEN COALESCE(usg_percent, 0) >= {{ usg_median_threshold }} 
                AND minutes_played >= {{ median_threshold_cat }} THEN 'Secondary Option'
            
            -- Role Player: Above-Q1 usage with regular rotation minutes
            WHEN COALESCE(usg_percent, 0) >= {{ usg_q1_threshold }} 
                AND minutes_played >= {{ q1_threshold_cat }} THEN 'Role Player'
            
            -- Connector/Specialist: Regular minutes but low usage
            WHEN minutes_played >= {{ q1_threshold_cat }} 
                AND COALESCE(usg_percent, 0) >= {{ usg_p5_threshold }} THEN 'Connector/Specialist'
            
            -- Low Usage: Some minutes but very low usage
            WHEN minutes_played >= {{ p5_threshold }} THEN 'Low Usage Player'
            
            -- Limited Role: Everything else
            ELSE 'Limited Role'
        END AS usage_tier,

        -- Usage quartile
        CASE
            WHEN COALESCE(usg_percent, 0) < {{ usg_q1_threshold }} THEN 'Q1'
            WHEN COALESCE(usg_percent, 0) < {{ usg_median_threshold }} THEN 'Q2'
            WHEN COALESCE(usg_percent, 0) < {{ usg_q3_threshold }} THEN 'Q3'
            ELSE 'Q4'
        END AS usage_quartile,

        -- UPDATED Impact Tier with data-driven thresholds
        CASE 
            -- First check for insufficient minutes
            WHEN minutes_played < {{ minutes_insufficient }} THEN 'Insufficient Minutes'
            
            -- Elite Impact: Top 10% of BPM (>13.9)
            WHEN COALESCE(bpm, 0) >= {{ bpm_p90_threshold }} THEN 'Elite Impact'
            
            -- High Impact: Top quartile (>5.0)
            WHEN COALESCE(bpm, 0) >= {{ bpm_q3_threshold }} THEN 'High Impact'
            
            -- Positive Impact: Above median (>-0.6)
            WHEN COALESCE(bpm, 0) >= {{ bpm_median_threshold }} THEN 'Positive Impact'
            
            -- Neutral Impact: Between median and Q1 (-6.3 to -0.6)
            WHEN COALESCE(bpm, 0) >= {{ bpm_q1_threshold }} THEN 'Neutral Impact'
            
            -- Negative Impact: Between Q1 and 5th percentile (-15.6 to -6.3)
            WHEN COALESCE(bpm, 0) >= {{ bpm_p5_threshold }} THEN 'Negative Impact'
            
            -- Very Negative Impact: Bottom 5% (< -15.6)
            ELSE 'Very Negative Impact'
        END AS impact_tier,

        -- BPM quartile for analytical purposes
        CASE
            WHEN COALESCE(bpm, 0) < {{ bpm_q1_threshold }} THEN 'Q1'
            WHEN COALESCE(bpm, 0) < {{ bpm_median_threshold }} THEN 'Q2'
            WHEN COALESCE(bpm, 0) < {{ bpm_q3_threshold }} THEN 'Q3'
            ELSE 'Q4'
        END AS bpm_quartile,

        -- Shooting efficiency tier
        CASE 
            WHEN minutes_played < {{ minutes_insufficient }} THEN 'Insufficient Minutes'
            WHEN COALESCE(ts_percent, 0) >= 0.68 THEN 'Elite'
            WHEN COALESCE(ts_percent, 0) >= 0.53 THEN 'Good'
            WHEN COALESCE(ts_percent, 0) >= 0.40 THEN 'Average'
            ELSE 'Below Average'
        END AS shooting_efficiency_tier,

        -- =================================================================
        -- BOOLEAN FLAGS
        -- =================================================================
        
        -- UPDATED Minutes-based flags with proper categorization
        minutes_played >= {{ p90_threshold }} AS is_extreme_minutes,  -- Top 10% (39+ minutes)
        minutes_played >= {{ q3_threshold_cat }} AS is_starter_minutes,  -- Q3+ (32+ minutes)
        minutes_played >= {{ q3_threshold_cat }} AND minutes_played < {{ p90_threshold }} AS is_normal_starter_minutes,  -- Q3-p90 (32-39 minutes)
        minutes_played >= {{ median_threshold_cat }} AND minutes_played < {{ q3_threshold_cat }} AS is_rotation_minutes,  -- Median-Q3 (25-32 minutes)
        minutes_played >= {{ q1_threshold_cat }} AND minutes_played < {{ median_threshold_cat }} AS is_bench_minutes,  -- Q1-Median (17-25 minutes)
        minutes_played >= {{ q1_threshold_cat }} AS is_meaningful_minutes,  -- Q1+ (17+ minutes)
        
        -- Player type flags (require meaningful minutes - Q1 threshold)
        minutes_played >= {{ q1_threshold_cat }} 
            AND COALESCE(ast_percent, 0) >= 20 
            AND COALESCE(trb_percent, 0) >= 13 AS is_versatile,
        
        minutes_played >= {{ q1_threshold_cat }}
            AND (COALESCE(stl_percent, 0) >= 2.5 OR COALESCE(blk_percent, 0) >= 3)
            AND COALESCE(d_rtg, 0) <= 110 AS is_defensive_specialist,
            
        minutes_played >= {{ q1_threshold_cat }}
            AND COALESCE(three_p_ar, 0) >= 0.4 
            AND COALESCE(d_rtg, 0) <= 110 AS is_three_and_d,
        
        -- Usage-based flags
        COALESCE(usg_percent, 0) >= {{ usg_p90_threshold }} AS is_extreme_usage,  -- Top 10%
        COALESCE(usg_percent, 0) >= {{ usg_q3_threshold }} AS is_high_usage,  -- Q3+
        COALESCE(usg_percent, 0) >= {{ usg_q3_threshold }} 
            AND COALESCE(usg_percent, 0) < {{ usg_p90_threshold }} AS is_normal_high_usage,  -- Q3-P90
        COALESCE(usg_percent, 0) >= {{ usg_median_threshold }} AS is_above_average_usage,  -- Median+
        
        -- Combined usage and minutes flags
        COALESCE(usg_percent, 0) >= {{ usg_q3_threshold }} 
            AND minutes_played >= {{ q3_threshold_cat }} AS is_primary_offensive_player,
        COALESCE(usg_percent, 0) >= {{ usg_median_threshold }} 
            AND minutes_played >= {{ median_threshold_cat }} AS is_significant_contributor,
        
        -- Impact-based flags
        COALESCE(bpm, 0) >= {{ bpm_p90_threshold }} AS is_elite_impact,  -- Top 10%
        COALESCE(bpm, 0) >= {{ bpm_q3_threshold }} AS is_positive_impact,  -- Q3+
        COALESCE(bpm, 0) >= {{ bpm_q3_threshold }} 
            AND COALESCE(bpm, 0) < {{ bpm_p90_threshold }} AS is_normal_positive_impact,  -- Q3-P90
        COALESCE(bpm, 0) >= {{ bpm_median_threshold }} AS is_above_average_impact,  -- Median+
        COALESCE(bpm, 0) <= {{ bpm_p5_threshold }} AS is_very_negative_impact,  -- Bottom 5%
        
        -- Core active player flags (multiple definitions for flexibility)
        -- Standard active player: Q1-Q3 minutes, P5-p90 usage, above P5 impact
        minutes_played >= {{ q1_threshold_cat }} 
            AND minutes_played < {{ q3_threshold_cat }}
            AND COALESCE(usg_percent, 0) >= {{ usg_p5_threshold }}
            AND COALESCE(usg_percent, 0) < {{ usg_p90_threshold }}
            AND COALESCE(bpm, 0) >= {{ bpm_p5_threshold }} AS is_standard_player,
            
        -- Core rotation player: Q1-P90 minutes (includes starters)
        minutes_played >= {{ q1_threshold_cat }} 
            AND minutes_played < {{ p90_threshold }}
            AND COALESCE(usg_percent, 0) >= {{ usg_p5_threshold }}
            AND COALESCE(bpm, 0) >= {{ bpm_p5_threshold }} AS is_core_rotation_player,
            
        -- Quality starter: Q3-P90 minutes with positive impact
        minutes_played >= {{ q3_threshold_cat }} 
            AND minutes_played < {{ p90_threshold }}
            AND COALESCE(bpm, 0) >= {{ bpm_median_threshold }} AS is_quality_starter,

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
