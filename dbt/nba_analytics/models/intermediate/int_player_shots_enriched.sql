{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

{#
    This model produces a unified shot-level dataset by combining:
      1. Field goal attempts from the shot chart source (with court location & timing)
      2. Free throw attempts derived from stg_player_game_basic_stats (no timing/location)

    Game ID resolution matches shots to games via date + team + opponent.
    FTs use quarter_number = 0 since we cannot determine when they occurred.

    This ensures downstream aggregations produce accurate total points:
      FG points (from shot chart) + FT points (from box score) = actual game points

    Not every shot will match a game — the shot chart dataset has incomplete coverage.
    Unmatched shots are KEPT with a NULL game_id for visibility, but downstream
    fact tables should filter to only matched rows.
#}

-- =================================================================
-- PART 1: SHOT CHART FIELD GOALS
-- =================================================================

WITH shot_charts AS (
    SELECT *
    FROM {{ ref('stg_player_shot_charts') }}
),

team_abbr_map AS (
    SELECT
        source_abbr,
        team_abbr AS conformed_abbr
    FROM {{ ref('team_abbreviation_mappings') }}
),

games AS (
    SELECT
        g.game_id,
        g.game_date,
        g.season_start_year,
        g.is_playoff,
        g.arena,
        g.home_points,
        g.visitor_points,
        g.point_differential,
        g.is_overtime,
        home_map.team_abbr AS home_team_abbr,
        visitor_map.team_abbr AS visitor_team_abbr,
        winning_map.team_abbr AS winning_team_abbr
    FROM {{ ref('stg_games') }} AS g
    LEFT JOIN {{ ref('team_maps') }} AS home_map
        ON g.home_team = home_map.full_name
    LEFT JOIN {{ ref('team_maps') }} AS visitor_map
        ON g.visitor_team = visitor_map.full_name
    LEFT JOIN {{ ref('team_maps') }} AS winning_map
        ON g.winning_team = winning_map.full_name
    WHERE (g.season_start_year >= home_map.start_year AND g.season_start_year < home_map.end_year)
      AND (g.season_start_year >= visitor_map.start_year AND g.season_start_year < visitor_map.end_year)
      AND (g.season_start_year >= winning_map.start_year AND g.season_start_year < winning_map.end_year)
),

shots_conformed AS (
    SELECT
        sc.*,
        COALESCE(tm.conformed_abbr, sc.team_abbr_raw) AS team_conformed,
        COALESCE(opp.conformed_abbr, sc.opponent_abbr_raw) AS opponent_conformed
    FROM shot_charts AS sc
    LEFT JOIN team_abbr_map AS tm
        ON sc.team_abbr_raw = tm.source_abbr
    LEFT JOIN team_abbr_map AS opp
        ON sc.opponent_abbr_raw = opp.source_abbr
),

shots_with_game AS (
    SELECT
        sc.*,
        g.game_id,
        g.is_playoff,
        g.arena,
        g.is_overtime AS game_had_overtime,
        CASE
            WHEN sc.team_conformed = g.home_team_abbr THEN 'HOME'
            WHEN sc.team_conformed = g.visitor_team_abbr THEN 'AWAY'
            ELSE NULL
        END AS team_location,
        CASE
            WHEN sc.team_conformed = g.winning_team_abbr THEN 'W'
            ELSE 'L'
        END AS game_result
    FROM shots_conformed AS sc
    LEFT JOIN games AS g
        ON CAST(sc.game_date AS DATE) = CAST(g.game_date AS DATE)
        AND (
            (sc.team_conformed = g.home_team_abbr AND sc.opponent_conformed = g.visitor_team_abbr)
            OR
            (sc.team_conformed = g.visitor_team_abbr AND sc.opponent_conformed = g.home_team_abbr)
        )
),

-- Format FG shots into the unified output schema
fg_shots AS (
    SELECT
        shot_id,
        game_id,
        player_id,
        team_conformed AS team,
        opponent_conformed AS opponent,
        game_date,
        game_date_raw,
        season_start_year,
        is_playoff,
        team_location,
        game_result,
        game_had_overtime,
        quarter_raw,
        quarter_number,
        is_overtime_shot,
        time_remaining_raw,
        seconds_remaining_in_quarter,
        shot_x_coordinate,
        shot_y_coordinate,
        is_made,
        shot_made_flag,
        shot_missed_flag,
        shot_type_raw,
        shot_point_value,
        is_three_pointer,
        is_free_throw,
        distance_ft,
        shot_distance_zone,
        points_generated,
        team_had_lead,
        team_score_at_shot,
        opponent_score_at_shot,
        score_margin_at_shot,
        is_clutch_shot,
        CASE WHEN game_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_game_match,
        'shot_chart' AS shot_source,
        created_at,
        updated_at,
        dbt_loaded_at
    FROM shots_with_game
),

-- =================================================================
-- PART 2: FREE THROW PSEUDO-SHOTS FROM BOX SCORE
-- =================================================================

-- Get distinct player-game context from matched FG shots
-- This ensures we only add FTs for games that have shot chart coverage
matched_player_games AS (
    SELECT DISTINCT
        game_id,
        player_id,
        team,
        opponent,
        game_date,
        game_date_raw,
        season_start_year,
        is_playoff,
        team_location,
        game_result,
        game_had_overtime
    FROM fg_shots
    WHERE has_game_match = TRUE
),

-- Pull FT counts from box score for those player-games
ft_source AS (
    SELECT
        mpg.*,
        bs.free_throws_made,
        bs.free_throws_attempted
    FROM matched_player_games AS mpg
    INNER JOIN {{ ref('stg_player_game_basic_stats') }} AS bs
        ON mpg.game_id = bs.game_id
        AND mpg.player_id = bs.player_id
    WHERE bs.did_play = TRUE
      AND bs.free_throws_attempted > 0
),

-- Expand made FTs into individual rows
ft_made_expanded AS (
    SELECT
        ft.*,
        gs.n AS ft_seq,
        TRUE AS ft_is_made
    FROM ft_source AS ft
    CROSS JOIN LATERAL generate_series(1, GREATEST(ft.free_throws_made, 0)) AS gs(n)
    WHERE ft.free_throws_made > 0
),

-- Expand missed FTs into individual rows
ft_missed_expanded AS (
    SELECT
        ft.*,
        gs.n AS ft_seq,
        FALSE AS ft_is_made
    FROM ft_source AS ft
    CROSS JOIN LATERAL generate_series(
        1,
        GREATEST(ft.free_throws_attempted - ft.free_throws_made, 0)
    ) AS gs(n)
    WHERE ft.free_throws_attempted > ft.free_throws_made
),

-- Format FT rows into the unified output schema
ft_shots AS (
    SELECT
        -- Synthetic shot_id: deterministic negative bigint to avoid collision with source IDs
        -(ABS(
            hashtext(
                ft.game_id || '|' || ft.player_id || '|FT_'
                || CASE WHEN ft.ft_is_made THEN 'MADE' ELSE 'MISS' END
                || '|' || ft.ft_seq::TEXT
            )
        )::BIGINT + 1) AS shot_id,

        ft.game_id,
        ft.player_id,
        ft.team,
        ft.opponent,
        ft.game_date,
        ft.game_date_raw,
        ft.season_start_year,
        ft.is_playoff,
        ft.team_location,
        ft.game_result,
        ft.game_had_overtime,

        -- Quarter & Time: unknown for box-score-derived FTs
        'Free Throw'::TEXT AS quarter_raw,
        0 AS quarter_number,
        FALSE AS is_overtime_shot,
        NULL::TEXT AS time_remaining_raw,
        NULL::INT AS seconds_remaining_in_quarter,

        -- Shot Location: N/A for free throws
        NULL::BIGINT AS shot_x_coordinate,
        NULL::BIGINT AS shot_y_coordinate,

        -- Shot Outcome
        ft.ft_is_made AS is_made,
        CASE WHEN ft.ft_is_made THEN 1 ELSE 0 END AS shot_made_flag,
        CASE WHEN ft.ft_is_made THEN 0 ELSE 1 END AS shot_missed_flag,

        -- Shot Type
        'free-throw'::TEXT AS shot_type_raw,
        1 AS shot_point_value,
        FALSE AS is_three_pointer,
        TRUE AS is_free_throw,
        15 AS distance_ft,
        'Free Throw (15 ft)'::TEXT AS shot_distance_zone,
        CASE WHEN ft.ft_is_made THEN 1 ELSE 0 END AS points_generated,

        -- Score Context: not available for box-score-derived FTs
        NULL::BOOLEAN AS team_had_lead,
        NULL::BIGINT AS team_score_at_shot,
        NULL::BIGINT AS opponent_score_at_shot,
        NULL::INT AS score_margin_at_shot,
        FALSE AS is_clutch_shot,

        -- Flags
        TRUE AS has_game_match,
        'box_score_ft'::TEXT AS shot_source,

        -- Metadata
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at,
        CURRENT_TIMESTAMP AS dbt_loaded_at

    FROM (
        SELECT * FROM ft_made_expanded
        UNION ALL
        SELECT * FROM ft_missed_expanded
    ) AS ft
),

-- =================================================================
-- PART 3: UNION ALL SHOTS
-- =================================================================

final AS (
    SELECT * FROM fg_shots
    UNION ALL
    SELECT * FROM ft_shots
)

SELECT * FROM final