{{
    config(
        materialized='table',
        schema='intermediate',
        indexes=[
            {'columns': ['game_id', 'team'], 'unique': True},
            {'columns': ['team', 'game_date']}
        ]
    )
}}

WITH basic_stats AS (
    SELECT *
    FROM {{ ref('stg_team_game_basic_stats') }}
),

adv_stats AS (
    SELECT *
    FROM {{ ref('stg_team_game_adv_stats') }}
),

games AS (
    SELECT 
        game_id,
        game_date,
        season_start_year,
        is_playoff,
        winning_team,
        home_team,
        visitor_team
    FROM {{ ref('stg_games') }}
),

final AS (
    SELECT
        -- Keys
        b.game_id,
        b.team,

        -- Game Context
        g.game_date,
        g.season_start_year,
        g.is_playoff,
        CASE 
            WHEN b.team = g.winning_team THEN 'W'
            ELSE 'L'
        END AS game_result,
        CASE 
            WHEN b.team = g.home_team THEN g.visitor_team
            ELSE g.home_team
        END AS opponent_team,

        -- Core Performance
        b.points,
        a.offensive_rating,
        a.defensive_rating,
        a.net_rating,
        
        -- Pace & Four Factors
        b.pace,
        b.effective_fg_pct,
        b.turnover_rate,
        b.offensive_rebound_rate,
        b.free_throw_rate,

        -- Advanced Tiers & Styles
        a.offensive_tier,
        a.defensive_tier,
        a.shot_selection_style,
        a.ball_movement_style,
        a.ball_security_tier,
        a.defensive_activity

    FROM basic_stats AS b
    LEFT JOIN adv_stats AS a
        ON b.game_id = a.game_id
        AND b.team = a.team
    LEFT JOIN games AS g
        ON b.game_id = g.game_id
)

SELECT * FROM final
