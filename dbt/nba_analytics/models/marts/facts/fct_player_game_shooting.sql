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

    Aggregates individual shot chart data to the game level, providing
    shooting breakdowns by zone, quarter, and clutch context.
    This model complements fct_player_game_stats by adding spatial
    and contextual shooting detail that box scores do not capture.


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
        -- OVERALL SHOOTING
        -- ==========================================
        COUNT(*) AS total_shots,
        SUM(shot_made_flag) AS total_makes,
        SUM(shot_missed_flag) AS total_misses,
        CASE WHEN COUNT(*) > 0
            THEN SUM(shot_made_flag)::NUMERIC / COUNT(*)::NUMERIC
            ELSE 0
        END AS overall_fg_pct,
        SUM(points_generated) AS total_points_from_shots,

        -- ==========================================
        -- BY SHOT TYPE
        -- ==========================================
        -- Two-pointers
        COUNT(*) FILTER (WHERE is_three_pointer = FALSE) AS two_point_attempts,
        SUM(shot_made_flag) FILTER (WHERE is_three_pointer = FALSE) AS two_point_makes,
        CASE WHEN COUNT(*) FILTER (WHERE is_three_pointer = FALSE) > 0
            THEN SUM(shot_made_flag) FILTER (WHERE is_three_pointer = FALSE)::NUMERIC
                 / COUNT(*) FILTER (WHERE is_three_pointer = FALSE)::NUMERIC
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
        -- BY DISTANCE ZONE
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

        -- ==========================================
        -- BY QUARTER
        -- ==========================================
        COUNT(*) FILTER (WHERE quarter_number = 1) AS q1_attempts,
        SUM(shot_made_flag) FILTER (WHERE quarter_number = 1) AS q1_makes,
        COUNT(*) FILTER (WHERE quarter_number = 2) AS q2_attempts,
        SUM(shot_made_flag) FILTER (WHERE quarter_number = 2) AS q2_makes,
        COUNT(*) FILTER (WHERE quarter_number = 3) AS q3_attempts,
        SUM(shot_made_flag) FILTER (WHERE quarter_number = 3) AS q3_makes,
        COUNT(*) FILTER (WHERE quarter_number = 4) AS q4_attempts,
        SUM(shot_made_flag) FILTER (WHERE quarter_number = 4) AS q4_makes,

        -- ==========================================
        -- CLUTCH SHOOTING
        -- ==========================================
        COUNT(*) FILTER (WHERE is_clutch_shot = TRUE) AS clutch_attempts,
        SUM(shot_made_flag) FILTER (WHERE is_clutch_shot = TRUE) AS clutch_makes,
        CASE WHEN COUNT(*) FILTER (WHERE is_clutch_shot = TRUE) > 0
            THEN SUM(shot_made_flag) FILTER (WHERE is_clutch_shot = TRUE)::NUMERIC
                 / COUNT(*) FILTER (WHERE is_clutch_shot = TRUE)::NUMERIC
            ELSE 0
        END AS clutch_fg_pct,
        SUM(points_generated) FILTER (WHERE is_clutch_shot = TRUE) AS clutch_points,

        -- ==========================================
        -- LEADING vs TRAILING
        -- ==========================================
        COUNT(*) FILTER (WHERE team_had_lead = TRUE) AS shots_while_leading,
        SUM(shot_made_flag) FILTER (WHERE team_had_lead = TRUE) AS makes_while_leading,
        COUNT(*) FILTER (WHERE team_had_lead = FALSE) AS shots_while_trailing,
        SUM(shot_made_flag) FILTER (WHERE team_had_lead = FALSE) AS makes_while_trailing,

        -- ==========================================
        -- SHOT DISTRIBUTION RATIOS
        -- ==========================================
        CASE WHEN COUNT(*) > 0
            THEN COUNT(*) FILTER (WHERE is_three_pointer = TRUE)::NUMERIC / COUNT(*)::NUMERIC
            ELSE 0
        END AS three_point_rate,

        CASE WHEN COUNT(*) > 0
            THEN COUNT(*) FILTER (WHERE shot_distance_zone = 'At Rim (0-3 ft)')::NUMERIC / COUNT(*)::NUMERIC
            ELSE 0
        END AS at_rim_rate,

        CASE WHEN COUNT(*) > 0
            THEN COUNT(*) FILTER (WHERE shot_distance_zone IN ('Mid Range (11-16 ft)', 'Long Mid Range (17-23 ft)'))::NUMERIC
                 / COUNT(*)::NUMERIC
            ELSE 0
        END AS mid_range_rate,

        -- Average shot distance
        AVG(distance_ft) AS avg_shot_distance_ft

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

        -- Overall Shooting
        ga.total_shots,
        ga.total_makes,
        ga.total_misses,
        ga.overall_fg_pct,
        ga.total_points_from_shots,

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

        -- By Quarter
        ga.q1_attempts,
        ga.q1_makes,
        ga.q2_attempts,
        ga.q2_makes,
        ga.q3_attempts,
        ga.q3_makes,
        ga.q4_attempts,
        ga.q4_makes,

        -- Clutch Shooting
        ga.clutch_attempts,
        ga.clutch_makes,
        ga.clutch_fg_pct,
        ga.clutch_points,

        -- Leading vs Trailing
        ga.shots_while_leading,
        ga.makes_while_leading,
        ga.shots_while_trailing,
        ga.makes_while_trailing,

        -- Shot Profile
        ga.three_point_rate,
        ga.at_rim_rate,
        ga.mid_range_rate,
        ga.avg_shot_distance_ft,

        -- Shot Profile Classification
        CASE
            WHEN ga.three_point_rate >= 0.50 THEN 'Perimeter Heavy'
            WHEN ga.at_rim_rate >= 0.50 THEN 'Rim Attacker'
            WHEN ga.mid_range_rate >= 0.40 THEN 'Mid Range Heavy'
            WHEN ga.three_point_rate >= 0.35 AND ga.at_rim_rate >= 0.30 THEN 'Modern (Rim & Three)'
            ELSE 'Balanced'
        END AS shot_profile_type,

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