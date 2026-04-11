{{
    config(
        materialized='table',
        schema='staging',
        alias='stg_team_game_adv_stats',
        tags=["staging"],
    )
}}

{#
    Team game advanced stats with dynamic, per-season tiering.

    Instead of hardcoded thresholds that apply uniformly across all seasons,
    this model joins to stg_team_season_thresholds to obtain percentile
    breakpoints computed from each season's own data distribution.

    Tiers have been expanded from the original 3-level system to 5 levels
    for better descriptive granularity:
      - Elite (Top 10%)
      - Above Average (Q3 to P90)
      - Average (Q1 to Q3 — the middle 50%)
      - Below Average (P10 to Q1)
      - Poor (Bottom 10%)

    NOTE: For "inverted" metrics where lower is better (defensive rating,
    turnover percentage), the tier logic flips — low values earn "Elite"
    and high values earn "Poor".
#}

WITH source_data AS (
    SELECT * FROM {{ source('raw_nba', 'team_game_adv_stats') }}
    WHERE deleted_at IS NULL
),

games AS (
    SELECT
        game_id,
        season_start_year
    FROM {{ ref('stg_games') }}
),

team_season_thresholds AS (
    SELECT * FROM {{ ref('stg_team_season_thresholds') }}
),

cleaned AS (
    SELECT
        -- =================================================================
        -- KEYS
        -- =================================================================
        sd.game_id,
        sd.team,

        -- =================================================================
        -- BASE METRICS
        -- =================================================================

        -- Minutes (should typically be 240 for regulation)
        COALESCE(sd.mp, 240) AS minutes_played,

        -- Shooting Efficiency
        COALESCE(sd.ts_percent, 0) AS true_shooting_pct,
        COALESCE(sd.efg_percent, 0) AS effective_fg_pct,

        -- Shot Selection
        COALESCE(sd.three_p_ar, 0) AS three_point_attempt_rate,
        COALESCE(sd.f_tr, 0) AS free_throw_rate,

        -- Rebounding
        COALESCE(sd.orb_percent, 0) AS offensive_rebound_pct,
        COALESCE(sd.drb_percent, 0) AS defensive_rebound_pct,
        COALESCE(sd.trb_percent, 0) AS total_rebound_pct,

        -- Ball Movement & Defense
        COALESCE(sd.ast_percent, 0) AS assist_pct,
        COALESCE(sd.stl_percent, 0) AS steal_pct,
        COALESCE(sd.blk_percent, 0) AS block_pct,
        COALESCE(sd.tov_percent, 0) AS turnover_pct,

        -- Usage (should always be 100 for team)
        COALESCE(sd.usg_percent, 100) AS usage_pct,

        -- Ratings
        COALESCE(sd.o_rtg, 0) AS offensive_rating,
        COALESCE(sd.d_rtg, 0) AS defensive_rating,

        -- Derived Metrics
        COALESCE(sd.o_rtg, 0) - COALESCE(sd.d_rtg, 0) AS net_rating,

        -- =================================================================
        -- PERFORMANCE TIERS (dynamic per-season, 5 levels)
        -- =================================================================

        -- Offensive Tier (higher o_rtg = better)
        CASE
            WHEN COALESCE(sd.o_rtg, 0) >= t.ortg_p90 THEN 'Elite Offense'
            WHEN COALESCE(sd.o_rtg, 0) >= t.ortg_q3  THEN 'Above Average Offense'
            WHEN COALESCE(sd.o_rtg, 0) >= t.ortg_q1  THEN 'Average Offense'
            WHEN COALESCE(sd.o_rtg, 0) >= t.ortg_p10 THEN 'Below Average Offense'
            ELSE 'Poor Offense'
        END AS offensive_tier,

        -- Defensive Tier (lower d_rtg = better — inverted logic)
        CASE
            WHEN COALESCE(sd.d_rtg, 0) <= t.drtg_p10 THEN 'Elite Defense'
            WHEN COALESCE(sd.d_rtg, 0) <= t.drtg_q1  THEN 'Above Average Defense'
            WHEN COALESCE(sd.d_rtg, 0) <= t.drtg_q3  THEN 'Average Defense'
            WHEN COALESCE(sd.d_rtg, 0) <= t.drtg_p90 THEN 'Below Average Defense'
            ELSE 'Poor Defense'
        END AS defensive_tier,

        -- =================================================================
        -- PLAY STYLE INDICATORS (dynamic per-season, 5 levels)
        -- =================================================================

        -- Shot Selection Style (3PA rate distribution)
        CASE
            WHEN COALESCE(sd.three_p_ar, 0) >= t.tpar_p90 THEN 'Three Point Heavy'
            WHEN COALESCE(sd.three_p_ar, 0) >= t.tpar_q3  THEN 'Three Point Leaning'
            WHEN COALESCE(sd.three_p_ar, 0) >= t.tpar_q1  THEN 'Balanced'
            WHEN COALESCE(sd.three_p_ar, 0) >= t.tpar_p10 THEN 'Inside Leaning'
            ELSE 'Inside Focused'
        END AS shot_selection_style,

        -- Ball Movement Style (assist % distribution)
        CASE
            WHEN COALESCE(sd.ast_percent, 0) >= t.ast_p90 THEN 'Elite Ball Movement'
            WHEN COALESCE(sd.ast_percent, 0) >= t.ast_q3  THEN 'High Ball Movement'
            WHEN COALESCE(sd.ast_percent, 0) >= t.ast_q1  THEN 'Average Ball Movement'
            WHEN COALESCE(sd.ast_percent, 0) >= t.ast_p10 THEN 'Low Ball Movement'
            ELSE 'Isolation Heavy'
        END AS ball_movement_style,

        -- =================================================================
        -- REBOUNDING TIERS (dynamic per-season, 5 levels)
        -- =================================================================

        -- Offensive Rebounding (higher = better)
        CASE
            WHEN COALESCE(sd.orb_percent, 0) >= t.orb_p90 THEN 'Elite Offensive Rebounding'
            WHEN COALESCE(sd.orb_percent, 0) >= t.orb_q3  THEN 'Above Average Offensive Rebounding'
            WHEN COALESCE(sd.orb_percent, 0) >= t.orb_q1  THEN 'Average Offensive Rebounding'
            WHEN COALESCE(sd.orb_percent, 0) >= t.orb_p10 THEN 'Below Average Offensive Rebounding'
            ELSE 'Poor Offensive Rebounding'
        END AS offensive_rebounding_tier,

        -- Defensive Rebounding (higher = better)
        CASE
            WHEN COALESCE(sd.drb_percent, 0) >= t.drb_p90 THEN 'Elite Defensive Rebounding'
            WHEN COALESCE(sd.drb_percent, 0) >= t.drb_q3  THEN 'Above Average Defensive Rebounding'
            WHEN COALESCE(sd.drb_percent, 0) >= t.drb_q1  THEN 'Average Defensive Rebounding'
            WHEN COALESCE(sd.drb_percent, 0) >= t.drb_p10 THEN 'Below Average Defensive Rebounding'
            ELSE 'Poor Defensive Rebounding'
        END AS defensive_rebounding_tier,

        -- =================================================================
        -- BALL SECURITY TIER (lower tov% = better — inverted logic)
        -- =================================================================
        CASE
            WHEN COALESCE(sd.tov_percent, 0) <= t.tov_p10 THEN 'Elite Ball Security'
            WHEN COALESCE(sd.tov_percent, 0) <= t.tov_q1  THEN 'Above Average Ball Security'
            WHEN COALESCE(sd.tov_percent, 0) <= t.tov_q3  THEN 'Average Ball Security'
            WHEN COALESCE(sd.tov_percent, 0) <= t.tov_p90 THEN 'Below Average Ball Security'
            ELSE 'Poor Ball Security'
        END AS ball_security_tier,

        -- =================================================================
        -- DEFENSIVE ACTIVITY (stl% + blk% combined)
        -- =================================================================
        CASE
            WHEN (COALESCE(sd.stl_percent, 0) + COALESCE(sd.blk_percent, 0)) >= t.stl_blk_p90 THEN 'Elite Defensive Activity'
            WHEN (COALESCE(sd.stl_percent, 0) + COALESCE(sd.blk_percent, 0)) >= t.stl_blk_q3  THEN 'High Defensive Activity'
            WHEN (COALESCE(sd.stl_percent, 0) + COALESCE(sd.blk_percent, 0)) >= t.stl_blk_q1  THEN 'Average Defensive Activity'
            WHEN (COALESCE(sd.stl_percent, 0) + COALESCE(sd.blk_percent, 0)) >= t.stl_blk_p10 THEN 'Low Defensive Activity'
            ELSE 'Passive Defense'
        END AS defensive_activity,

        -- =================================================================
        -- ADDITIONAL TIERS (new — not in original model)
        -- =================================================================

        -- True Shooting Tier (higher = better)
        CASE
            WHEN COALESCE(sd.ts_percent, 0) >= t.ts_p90 THEN 'Elite Shooting'
            WHEN COALESCE(sd.ts_percent, 0) >= t.ts_q3  THEN 'Above Average Shooting'
            WHEN COALESCE(sd.ts_percent, 0) >= t.ts_q1  THEN 'Average Shooting'
            WHEN COALESCE(sd.ts_percent, 0) >= t.ts_p10 THEN 'Below Average Shooting'
            ELSE 'Poor Shooting'
        END AS true_shooting_tier,

        -- Free Throw Rate Tier (higher = more FT generation)
        CASE
            WHEN COALESCE(sd.f_tr, 0) >= t.ftr_p90 THEN 'Elite FT Generation'
            WHEN COALESCE(sd.f_tr, 0) >= t.ftr_q3  THEN 'High FT Generation'
            WHEN COALESCE(sd.f_tr, 0) >= t.ftr_q1  THEN 'Average FT Generation'
            WHEN COALESCE(sd.f_tr, 0) >= t.ftr_p10 THEN 'Low FT Generation'
            ELSE 'Minimal FT Generation'
        END AS free_throw_generation_tier,

        -- =================================================================
        -- COMPOSITE STYLE LABEL
        -- Combines offense + defense tier into a single game identity.
        -- Uses Q3/Q1 as the dividing lines for simplicity.
        -- =================================================================
        CASE
            WHEN COALESCE(sd.o_rtg, 0) >= t.ortg_q3
                AND COALESCE(sd.d_rtg, 0) <= t.drtg_q1 THEN 'Dominant Both Ends'
            WHEN COALESCE(sd.o_rtg, 0) >= t.ortg_q3
                AND COALESCE(sd.d_rtg, 0) <= t.drtg_q3 THEN 'Offense First, Solid Defense'
            WHEN COALESCE(sd.o_rtg, 0) >= t.ortg_q3  THEN 'High Powered Offense'
            WHEN COALESCE(sd.d_rtg, 0) <= t.drtg_q1  THEN 'Defensive Juggernaut'
            WHEN COALESCE(sd.d_rtg, 0) <= t.drtg_q3
                AND COALESCE(sd.o_rtg, 0) >= t.ortg_q1 THEN 'Defense First, Solid Offense'
            WHEN COALESCE(sd.o_rtg, 0) >= t.ortg_q1
                AND COALESCE(sd.d_rtg, 0) <= t.drtg_q3 THEN 'Balanced Competitive'
            WHEN COALESCE(sd.o_rtg, 0) < t.ortg_q1
                AND COALESCE(sd.d_rtg, 0) > t.drtg_q3 THEN 'Struggled Both Ends'
            ELSE 'Middle of the Pack'
        END AS game_performance_profile,

        -- =================================================================
        -- METADATA
        -- =================================================================
        sd.created_at,
        sd.updated_at,
        CURRENT_TIMESTAMP AS dbt_loaded_at

    FROM source_data AS sd
    LEFT JOIN games AS g
        ON sd.game_id = g.game_id
    LEFT JOIN team_season_thresholds AS t
        ON g.season_start_year = t.season_start_year
    WHERE
        sd.game_id IS NOT NULL
        AND sd.team IS NOT NULL
)

SELECT * FROM cleaned
