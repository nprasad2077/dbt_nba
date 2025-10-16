{{
    config(
        materialized='table',
        schema='intermediate'
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

final AS (
    SELECT
        -- Keys
        basic_stats.game_id,
        basic_stats.team,

        -- Core stats
        basic_stats.points,
        basic_stats.assists,
        basic_stats.total_rebounds,
        basic_stats.steals,
        basic_stats.blocks,
        basic_stats.turnovers,

        -- Derived basic metrics
        basic_stats.possessions_estimate,
        basic_stats.pace,

        -- Four Factors
        basic_stats.effective_fg_pct,
        basic_stats.turnover_rate,
        basic_stats.offensive_rebound_rate,
        basic_stats.free_throw_rate,

        -- Advanced stats
        adv_stats.offensive_rating,
        adv_stats.defensive_rating,
        adv_stats.net_rating,
        adv_stats.true_shooting_pct,

        -- Derived advanced tiers & styles
        adv_stats.offensive_tier,
        adv_stats.defensive_tier,
        adv_stats.shot_selection_style,
        adv_stats.ball_movement_style,
        adv_stats.ball_security_tier,
        adv_stats.defensive_activity

    FROM basic_stats
    LEFT JOIN adv_stats
        ON basic_stats.game_id = adv_stats.game_id
        AND basic_stats.team = adv_stats.team
)

SELECT * FROM final