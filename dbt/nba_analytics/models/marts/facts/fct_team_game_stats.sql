{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH team_performance AS (
    SELECT *
    FROM {{ ref('int_team_performance') }}
),

game_details AS (
    SELECT
        game_id,
        arena AS arena_name
    FROM {{ ref('stg_games') }}
),

final AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['tp.game_id', 'tp.team']) }} AS team_game_key,

        -- Foreign Keys
        d.date_key,
        t.team_key,
        opp_t.team_key AS opponent_team_key,
        s.season_key,
        a.arena_key,

        -- Degenerate Dimension
        tp.game_id,

        -- Game Context
        tp.game_result,

        -- Team's Own Performance Metrics
        tp.points AS points_scored,
        opponent.points AS points_allowed,
        tp.offensive_rating,
        tp.defensive_rating,
        tp.net_rating,
        tp.pace,
        tp.effective_fg_pct,
        tp.turnover_rate,
        tp.offensive_rebound_rate,
        tp.free_throw_rate,

        -- Opponent's Performance Metrics (for context)
        opponent.offensive_rating AS opponent_offensive_rating,
        opponent.defensive_rating AS opponent_defensive_rating,
        opponent.net_rating AS opponent_net_rating,
        opponent.pace AS opponent_pace,
        
        -- Categorical Tiers & Styles
        tp.offensive_tier,
        tp.defensive_tier,
        tp.shot_selection_style,
        tp.ball_movement_style

    FROM team_performance AS tp
    
    -- Self-join to get opponent's stats for the same game
    LEFT JOIN team_performance AS opponent
        ON tp.game_id = opponent.game_id
        AND tp.opponent_team = opponent.team

    LEFT JOIN game_details AS gd
        ON tp.game_id = gd.game_id

    -- Joins to Dimension Tables
    LEFT JOIN {{ ref('dim_dates') }} AS d
        ON CAST(tp.game_date AS DATE) = d.full_date
    LEFT JOIN {{ ref('dim_teams') }} AS t
        ON tp.team = t.team_abbr
    LEFT JOIN {{ ref('dim_teams') }} AS opp_t
        ON tp.opponent_team = opp_t.team_abbr
    LEFT JOIN {{ ref('dim_seasons') }} AS s
        ON tp.season_start_year = s.season_start_year
    LEFT JOIN {{ ref('dim_arenas') }} AS a
        ON gd.arena_name = a.arena_name
)

SELECT * FROM final
