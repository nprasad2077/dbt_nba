{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

WITH basic_stats AS (
    SELECT *
    FROM {{ ref('stg_player_game_basic_stats') }}
),

adv_stats AS (
    SELECT *
    FROM {{ ref('stg_player_game_adv_stats') }}
),

final AS (
    SELECT
        -- Keys from basic stats
        basic_stats.game_id,
        basic_stats.player_id,
        basic_stats.team,
        basic_stats.player_name,

        -- Core basic stats
        basic_stats.did_play,
        basic_stats.minutes_played,
        basic_stats.points,
        basic_stats.assists,
        basic_stats.total_rebounds,
        basic_stats.steals,
        basic_stats.blocks,
        basic_stats.turnovers,
        basic_stats.personal_fouls,
        basic_stats.plus_minus,
        basic_stats.game_score,

        -- Shooting basic stats
        basic_stats.field_goals_made,
        basic_stats.field_goals_attempted,
        basic_stats.field_goal_pct,
        basic_stats.three_pointers_made,
        basic_stats.three_pointers_attempted,
        basic_stats.three_point_pct,
        basic_stats.free_throws_made,
        basic_stats.free_throws_attempted,
        basic_stats.free_throw_pct,

        -- Derived basic stats
        basic_stats.is_double_double,
        basic_stats.is_triple_double,
        basic_stats.likely_starter,
        
        -- Core advanced stats from adv_stats
        adv_stats.usage_pct,
        adv_stats.offensive_rating,
        adv_stats.defensive_rating,
        adv_stats.net_rating,
        adv_stats.box_plus_minus,
        adv_stats.true_shooting_pct,
        adv_stats.effective_fg_pct,

        -- Derived advanced tiers & flags from adv_stats
        adv_stats.shooting_efficiency_tier,
        adv_stats.usage_tier,
        adv_stats.impact_tier,
        adv_stats.minutes_based_role,
        adv_stats.is_versatile,
        adv_stats.is_defensive_specialist,
        adv_stats.is_three_and_d

    FROM basic_stats
    LEFT JOIN adv_stats
        ON basic_stats.game_id = adv_stats.game_id
        AND basic_stats.player_id = adv_stats.player_id
)

SELECT * FROM final