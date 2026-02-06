{{
    config(
        materialized='incremental',
        schema='marts',
        unique_key='player_game_shooting_key',
        tags=["facts"],
        indexes=[
            {'columns': ['player_key']},
            {'columns': ['team_key']},
            {'columns': ['date_key']},
            {'columns': ['game_id']}
        ]
    )
}}

{#
    Grain: One row per player per game.

    Aggregates individual shot chart data AND box-score free throws
    to the game level, providing complete scoring breakdowns.

    Total points = FG points (from shot chart) + FT points (from box score)

    This model complements fct_player_game_stats by adding spatial
    and contextual shooting detail that box scores do not capture,
    while also arriving at accurate total points.

    NOTE: Only games with shot chart coverage are included.
    This is NOT a complete record of all player games.
#}

WITH shots AS (
    SELECT *
    FROM {{ ref('int_player_shots_enriched') }}
    WHERE has_game_match = TRUE
    {% if is_incremental() %}
      AND game_date >= (SELECT MAX(game_date) FROM {{ this }}) - INTERVAL '30 days'
    {% endif %}
),

game_agg AS (
    SELECT
        -- Grain Keys
        game_id,
        player_id,
        team,
        opponent,

        -- Game Context (take from any row — same per game/player)
        MIN(game_date) AS game_date,
        MIN(season_start_year) AS season_start_year,
        BOOL_OR(is_playoff) AS is_playoff,
        MIN(team_location) AS team_location,
        MIN(game_result) AS game_result,

        -- ==========================================
        -- COMPLETE SCORING TOTALS (FG + FT)
        -- ==========================================
        SUM(points_generated) AS total_points,
        COUNT(*) AS total_shot_attempts,
        SUM(shot_made_flag) AS total_shots_made,

        -- ==========================================
        -- FIELD GOAL TOTALS (shot_chart source only)
        -- ==========================================
        COUNT(*) FILTER (WHERE shot_source = 'shot_chart') AS fg_attempts,
        SUM(shot_made_flag) FILTER (WHERE shot_source = 'shot_chart') AS fg_makes,
        SUM(shot_missed_flag) FILTER (WHERE shot_source = 'shot_chart') AS fg_misses,
        CASE WHEN COUNT(*) FILTER (WHERE shot_source = 'shot_chart') > 0
            THEN SUM(shot_made_flag) FILTER (WHERE shot_source = 'shot_chart')::NUMERIC
                 / COUNT(*) FILTER (WHERE shot_source = 'shot_chart')::NUMERIC
            ELSE 0
        END AS fg_pct,
        SUM(points_generated) FILTER (WHERE shot_source = 'shot_chart') AS fg_points,

        -- ==========================================
        -- FREE THROW TOTALS (box_score_ft source)
        -- ==========================================
        COUNT(*) FILTER (WHERE shot_source = 'box_score_ft') AS ft_attempts,
        SUM(shot_made_flag) FILTER (WHERE shot_source = 'box_score_ft') AS ft_makes,
        SUM(shot_missed_flag) FILTER (WHERE shot_source = 'box_score_ft') AS ft_misses,
        CASE WHEN COUNT(*) FILTER (WHERE shot_source = 'box_score_ft') > 0
            THEN SUM(shot_made_flag) FILTER (WHERE shot_source = 'box_score_ft')::NUMERIC
                 / COUNT(*) FILTER (WHERE shot_source = 'box_score_ft')::NUMERIC
            ELSE 0
        END AS ft_pct,
        SUM(points_generated) FILTER (WHERE shot_source = 'box_score_ft') AS ft_points,

        -- ==========================================
        -- BY SHOT TYPE (FG breakdown)
        -- ==========================================
        -- Two-pointers
        COUNT(*) FILTER (WHERE is_three_pointer = FALSE AND shot_source = 'shot_chart') AS two_point_attempts,
        SUM(shot_made_flag) FILTER (WHERE is_three_pointer = FALSE AND shot_source = 'shot_chart') AS two_point_makes,
        CASE WHEN COUNT(*) FILTER (WHERE is_three_pointer = FALSE AND shot_source = 'shot_chart') > 0
            THEN SUM(shot_made_flag) FILTER (WHERE is_three_pointer = FALSE AND shot_source = 'shot_chart')::NUMERIC
                 / COUNT(*) FILTER (WHERE is_three_pointer = FALSE AND shot_source = 'shot_chart')::NUMERIC
            ELSE 0
        END AS two_point_fg_pct,

        -- Three-pointers
        COUNT(*) FILTER (WHERE is_three_pointer = TRUE) AS three_point_attempts,
        SUM(shot_made_flag) FILTER (WHERE is_three_pointer = TRUE) AS three_point_makes,
        CASE WHEN COUNT(*) FILTER (WHERE is_three_pointer = TRUE) > 0
            THEN SUM(shot_made_flag) FILTER (WHERE is_three_pointer = TRUE)::NUMERIC
                 / COUNT(*) FILTER (WHERE is_three_pointer = TRUE)::NUMERIC
            ELSE 0
        END AS three_point_fg_pct,

        -- ==========================================
        -- BY DISTANCE ZONE (FG only — excludes FTs)
        -- ==========================================
        COUNT(*) FILTER (WHERE shot_distance_zone = 'At Rim (0-3 ft)') AS at_rim_attempts,
        SUM(shot_made_flag) FILTER (WHERE shot_distance_zone = 'At Rim (0-3 ft)') AS at_rim_makes,
        CASE WHEN COUNT(*) FILTER (WHERE shot_distance_zone = 'At Rim (0-3 ft)') > 0
            THEN SUM(shot_made_flag) FILTER (WHERE shot_distance_zone = 'At Rim (0-3 ft)')::NUMERIC
                 / COUNT(*) FILTER (WHERE shot_distance_zone = 'At Rim (0-3 ft)')::NUMERIC
            ELSE 0
        END AS at_rim_fg_pct,

        COUNT(*) FILTER (WHERE shot_distance_zone = 'Short Range (4-10 ft)') AS short_range_attempts,
        SUM(shot_made_flag) FILTER (WHERE shot_distance_zone = 'Short Range (4-10 ft)') AS short_range_makes,

        COUNT(*) FILTER (WHERE shot_distance_zone = 'Mid Range (11-16 ft)') AS mid_range_attempts,
        SUM(shot_made_flag) FILTER (WHERE shot_distance_zone = 'Mid Range (11-16 ft)') AS mid_range_makes,

        COUNT(*) FILTER (WHERE shot_distance_zone = 'Long Mid Range (17-23 ft)') AS long_mid_range_attempts,
        SUM(shot_made_flag) FILTER (WHERE shot_distance_zone = 'Long Mid Range (17-23 ft)') AS long_mid_range_makes,

        COUNT(*) FILTER (WHERE shot_distance_zone IN ('Three Point (24-27 ft)', 'Deep Three (28+ ft)')) AS beyond_arc_attempts,
        SUM(shot_made_flag) FILTER (WHERE shot_distance_zone IN ('Three Point (24-27 ft)', 'Deep Three (28+ ft)')) AS beyond_arc_makes,

        -- Free Throw zone (for completeness in zone analysis)
        COUNT(*) FILTER (WHERE shot_distance_zone = 'Free Throw (15 ft)') AS free_throw_line_attempts,
        SUM(shot_made_flag) FILTER (WHERE shot_distance_zone = 'Free Throw (15 ft)') AS free_throw_line_makes,

        -- ==========================================
        -- BY QUARTER (FG only — FTs have quarter_number = 0)
        -- ==========================================
        COUNT(*) FILTER (WHERE quarter_number = 1 AND shot_source = 'shot_chart') AS q1_fg_attempts,
        SUM(shot_made_flag) FILTER (WHERE quarter_number = 1 AND shot_source = 'shot_chart') AS q1_fg_makes,
        COUNT(*) FILTER (WHERE quarter_number = 2 AND shot_source = 'shot_chart') AS q2_fg_attempts,
        SUM(shot_made_flag) FILTER (WHERE quarter_number = 2 AND shot_source = 'shot_chart') AS q2_fg_makes,
        COUNT(*) FILTER (WHERE quarter_number = 3 AND shot_source = 'shot_chart') AS q3_fg_attempts,
        SUM(shot_made_flag) FILTER (WHERE quarter_number = 3 AND shot_source = 'shot_chart') AS q3_fg_makes,
        COUNT(*) FILTER (WHERE quarter_number = 4 AND shot_source = 'shot_chart') AS q4_fg_attempts,
        SUM(shot_made_flag) FILTER (WHERE quarter_number = 4 AND shot_source = 'shot_chart') AS q4_fg_makes,

        -- ==========================================
        -- CLUTCH SHOOTING (FG only — FTs lack timing)
        -- ==========================================
        COUNT(*) FILTER (WHERE is_clutch_shot = TRUE) AS clutch_fg_attempts,
        SUM(shot_made_flag) FILTER (WHERE is_clutch_shot = TRUE) AS clutch_fg_makes,
        CASE WHEN COUNT(*) FILTER (WHERE is_clutch_shot = TRUE) > 0
            THEN SUM(shot_made_flag) FILTER (WHERE is_clutch_shot = TRUE)::NUMERIC
                 / COUNT(*) FILTER (WHERE is_clutch_shot = TRUE)::NUMERIC
            ELSE 0
        END AS clutch_fg_pct,
        SUM(points_generated) FILTER (WHERE is_clutch_shot = TRUE) AS clutch_fg_points,

        -- ==========================================
        -- LEADING vs TRAILING (FG only — FTs lack score context)
        -- ==========================================
        COUNT(*) FILTER (WHERE team_had_lead = TRUE AND shot_source = 'shot_chart') AS fg_while_leading,
        SUM(shot_made_flag) FILTER (WHERE team_had_lead = TRUE AND shot_source = 'shot_chart') AS fg_makes_while_leading,
        COUNT(*) FILTER (WHERE team_had_lead = FALSE AND shot_source = 'shot_chart') AS fg_while_trailing,
        SUM(shot_made_flag) FILTER (WHERE team_had_lead = FALSE AND shot_source = 'shot_chart') AS fg_makes_while_trailing,

        -- ==========================================
        -- SHOT DISTRIBUTION RATIOS (FG only)
        -- ==========================================
        CASE WHEN COUNT(*) FILTER (WHERE shot_source = 'shot_chart') > 0
            THEN COUNT(*) FILTER (WHERE is_three_pointer = TRUE)::NUMERIC
                 / COUNT(*) FILTER (WHERE shot_source = 'shot_chart')::NUMERIC
            ELSE 0
        END AS three_point_rate,

        CASE WHEN COUNT(*) FILTER (WHERE shot_source = 'shot_chart') > 0
            THEN COUNT(*) FILTER (WHERE shot_distance_zone = 'At Rim (0-3 ft)')::NUMERIC
                 / COUNT(*) FILTER (WHERE shot_source = 'shot_chart')::NUMERIC
            ELSE 0
        END AS at_rim_rate,

        CASE WHEN COUNT(*) FILTER (WHERE shot_source = 'shot_chart') > 0
            THEN COUNT(*) FILTER (
                    WHERE shot_distance_zone IN ('Mid Range (11-16 ft)', 'Long Mid Range (17-23 ft)')
                 )::NUMERIC
                 / COUNT(*) FILTER (WHERE shot_source = 'shot_chart')::NUMERIC
            ELSE 0
        END AS mid_range_rate,

        -- Free throw rate (FTA / FGA) — a key Four Factors metric
        CASE WHEN COUNT(*) FILTER (WHERE shot_source = 'shot_chart') > 0
            THEN COUNT(*) FILTER (WHERE shot_source = 'box_score_ft')::NUMERIC
                 / COUNT(*) FILTER (WHERE shot_source = 'shot_chart')::NUMERIC
            ELSE 0
        END AS free_throw_rate,

        -- Average FG shot distance
        AVG(distance_ft) FILTER (WHERE shot_source = 'shot_chart') AS avg_fg_distance_ft,

        -- True Shooting Percentage: PTS / (2 * (FGA + 0.44 * FTA))
        CASE WHEN (
                COUNT(*) FILTER (WHERE shot_source = 'shot_chart')
                + 0.44 * COUNT(*) FILTER (WHERE shot_source = 'box_score_ft')
            ) > 0
            THEN SUM(points_generated)::NUMERIC / (
                2.0 * (
                    COUNT(*) FILTER (WHERE shot_source = 'shot_chart')
                    + 0.44 * COUNT(*) FILTER (WHERE shot_source = 'box_score_ft')
                )
            )
            ELSE 0
        END AS true_shooting_pct

    FROM shots
    GROUP BY game_id, player_id, team, opponent
),

final AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['ga.game_id', 'ga.player_id']) }} AS player_game_shooting_key,

        -- Foreign Keys
        p.player_key,
        t.team_key,
        opp_t.team_key AS opponent_key,
        d.date_key,
        s.season_key,

        -- Degenerate Dimensions
        ga.game_id,

        -- Game Context
        ga.is_playoff,
        ga.team_location,
        ga.game_result,

        -- ==========================================
        -- COMPLETE SCORING TOTALS
        -- ==========================================
        ga.total_points,
        ga.total_shot_attempts,
        ga.total_shots_made,

        -- Field Goal Totals
        ga.fg_attempts,
        ga.fg_makes,
        ga.fg_misses,
        ga.fg_pct,
        ga.fg_points,

        -- Free Throw Totals
        ga.ft_attempts,
        ga.ft_makes,
        ga.ft_misses,
        ga.ft_pct,
        ga.ft_points,

        -- True Shooting %
        ga.true_shooting_pct,

        -- By Shot Type
        ga.two_point_attempts,
        ga.two_point_makes,
        ga.two_point_fg_pct,
        ga.three_point_attempts,
        ga.three_point_makes,
        ga.three_point_fg_pct,

        -- By Distance Zone
        ga.at_rim_attempts,
        ga.at_rim_makes,
        ga.at_rim_fg_pct,
        ga.short_range_attempts,
        ga.short_range_makes,
        ga.mid_range_attempts,
        ga.mid_range_makes,
        ga.long_mid_range_attempts,
        ga.long_mid_range_makes,
        ga.beyond_arc_attempts,
        ga.beyond_arc_makes,
        ga.free_throw_line_attempts,
        ga.free_throw_line_makes,

        -- By Quarter (FG only)
        ga.q1_fg_attempts,
        ga.q1_fg_makes,
        ga.q2_fg_attempts,
        ga.q2_fg_makes,
        ga.q3_fg_attempts,
        ga.q3_fg_makes,
        ga.q4_fg_attempts,
        ga.q4_fg_makes,

        -- Clutch Shooting (FG only)
        ga.clutch_fg_attempts,
        ga.clutch_fg_makes,
        ga.clutch_fg_pct,
        ga.clutch_fg_points,

        -- Leading vs Trailing (FG only)
        ga.fg_while_leading,
        ga.fg_makes_while_leading,
        ga.fg_while_trailing,
        ga.fg_makes_while_trailing,

        -- Shot Profile Ratios
        ga.three_point_rate,
        ga.at_rim_rate,
        ga.mid_range_rate,
        ga.free_throw_rate,
        ga.avg_fg_distance_ft,

        -- Shot Profile Classification (based on FG distribution)
        CASE
            WHEN ga.three_point_rate >= 0.50 THEN 'Perimeter Heavy'
            WHEN ga.at_rim_rate >= 0.50 THEN 'Rim Attacker'
            WHEN ga.mid_range_rate >= 0.40 THEN 'Mid Range Heavy'
            WHEN ga.three_point_rate >= 0.35 AND ga.at_rim_rate >= 0.30 THEN 'Modern (Rim & Three)'
            ELSE 'Balanced'
        END AS shot_profile_type,

        -- Scoring Method Classification
        CASE
            WHEN ga.ft_attempts = 0 THEN 'No FTs'
            WHEN ga.ft_points::NUMERIC / NULLIF(ga.total_points, 0)::NUMERIC >= 0.40 THEN 'FT Dependent'
            WHEN ga.ft_points::NUMERIC / NULLIF(ga.total_points, 0)::NUMERIC >= 0.25 THEN 'High FT Volume'
            ELSE 'Field Goal Driven'
        END AS scoring_method_type,

        -- Game Date for incremental logic
        CAST(ga.game_date AS DATE) AS game_date

    FROM game_agg AS ga

    LEFT JOIN {{ ref('dim_players') }} AS p
        ON ga.player_id = p.player_id
    LEFT JOIN {{ ref('dim_teams') }} AS t
        ON ga.team = t.team_abbr
    LEFT JOIN {{ ref('dim_teams') }} AS opp_t
        ON ga.opponent = opp_t.team_abbr
    LEFT JOIN {{ ref('dim_dates') }} AS d
        ON CAST(ga.game_date AS DATE) = d.full_date
    LEFT JOIN {{ ref('dim_seasons') }} AS s
        ON ga.season_start_year = s.season_start_year
)

SELECT * FROM final