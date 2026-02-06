{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

{#
    This model resolves the game_id for each shot by matching on:
      1. Game date
      2. Team abbreviation (conformed) matching either home or visitor
      3. Opponent abbreviation (conformed) matching the other side

    Not every shot will match a game — the shot chart dataset has incomplete coverage.
    Unmatched shots are KEPT with a NULL game_id for visibility, but downstream
    fact tables should filter to only matched rows.
#}

WITH shot_charts AS (
    SELECT *
    FROM {{ ref('stg_player_shot_charts') }}
),

-- Conform the shot chart team abbreviation to the canonical abbreviation
-- using the team_abbreviation_mappings seed (handles NJN->BKN, NOH->NOP, etc.)
team_abbr_map AS (
    SELECT
        source_abbr,
        team_abbr AS conformed_abbr
    FROM {{ ref('team_abbreviation_mappings') }}
),

-- Get games with conformed team abbreviations for matching
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

-- Conform shot chart abbreviations
shots_conformed AS (
    SELECT
        sc.*,

        -- Conform team abbreviation
        COALESCE(tm.conformed_abbr, sc.team_abbr_raw) AS team_conformed,

        -- Conform opponent abbreviation
        COALESCE(opp.conformed_abbr, sc.opponent_abbr_raw) AS opponent_conformed

    FROM shot_charts AS sc
    LEFT JOIN team_abbr_map AS tm
        ON sc.team_abbr_raw = tm.source_abbr
    LEFT JOIN team_abbr_map AS opp
        ON sc.opponent_abbr_raw = opp.source_abbr
),

-- Match shots to games
-- A shot matches a game when:
--   1. The date matches
--   2. The conformed team is either the home or visitor team
--   3. The conformed opponent is the other team
shots_with_game AS (
    SELECT
        sc.*,
        g.game_id,
        g.is_playoff,
        g.arena,
        g.is_overtime AS game_had_overtime,

        -- Determine if the player's team was home or away
        CASE
            WHEN sc.team_conformed = g.home_team_abbr THEN 'HOME'
            WHEN sc.team_conformed = g.visitor_team_abbr THEN 'AWAY'
            ELSE NULL
        END AS team_location,

        -- Game outcome for the shooting player's team
        CASE
            WHEN sc.team_conformed = g.winning_team_abbr THEN 'W'
            ELSE 'L'
        END AS game_result,

        -- Final game scores
        g.home_points AS game_home_points,
        g.visitor_points AS game_visitor_points

    FROM shots_conformed AS sc
    LEFT JOIN games AS g
        ON CAST(sc.game_date AS DATE) = CAST(g.game_date AS DATE)
        AND (
            -- Team is home AND opponent is visitor
            (sc.team_conformed = g.home_team_abbr AND sc.opponent_conformed = g.visitor_team_abbr)
            OR
            -- Team is visitor AND opponent is home
            (sc.team_conformed = g.visitor_team_abbr AND sc.opponent_conformed = g.home_team_abbr)
        )
),

final AS (
    SELECT
        -- Keys
        shot_id,
        game_id,  -- Will be NULL if no matching game found
        player_id,

        -- Conformed Team References
        team_conformed AS team,
        opponent_conformed AS opponent,

        -- Game Context (from game match)
        game_date,
        game_date_raw,
        season_start_year,
        is_playoff,
        team_location,
        game_result,
        game_had_overtime,

        -- Quarter & Time
        quarter_raw,
        quarter_number,
        is_overtime_shot,
        time_remaining_raw,
        seconds_remaining_in_quarter,

        -- Shot Location
        shot_x_coordinate,
        shot_y_coordinate,

        -- Shot Details
        is_made,
        shot_made_flag,
        shot_missed_flag,
        shot_type_raw,
        shot_point_value,
        is_three_pointer,
        distance_ft,
        shot_distance_zone,
        points_generated,

        -- Score Context
        team_had_lead,
        team_score_at_shot,
        opponent_score_at_shot,
        score_margin_at_shot,
        is_clutch_shot,

        -- Quality Flag: did this shot successfully match to a game?
        CASE
            WHEN game_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS has_game_match,

        -- Metadata
        created_at,
        updated_at,
        dbt_loaded_at

    FROM shots_with_game
)

SELECT * FROM final