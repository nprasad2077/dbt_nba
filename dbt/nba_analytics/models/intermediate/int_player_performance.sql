{{
    config(
        materialized='table',
        schema='intermediate',
        indexes=[
            {'columns': ['game_id', 'player_id'], 'unique': True},
            {'columns': ['player_id', 'game_date']},
            {'columns': ['team', 'game_date']}
        ]
    )
}}

WITH basic_stats AS (
    SELECT *
    FROM {{ ref('stg_player_game_basic_stats') }}
    WHERE did_play = TRUE
),

adv_stats AS (
    SELECT *
    FROM {{ ref('stg_player_game_adv_stats') }}
),

games AS (
    SELECT 
        game_id,
        game_date,
        season_start_year,
        is_playoff,
        home_team,
        winning_team
    FROM {{ ref('stg_games') }}
),

final AS (
    SELECT
        -- Primary Keys
        b.game_id,
        b.player_id,

        -- Player & Team Info
        b.player_name,
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
            WHEN b.team = g.home_team THEN 'HOME'
            ELSE 'AWAY'
        END AS team_location,

        -- Core Performance Stats
        b.minutes_played,
        b.points,
        b.assists,
        b.total_rebounds,
        b.steals,
        b.blocks,
        b.turnovers,
        b.plus_minus,
        a.net_rating,
        a.box_plus_minus,
        
        -- Shooting Stats
        b.field_goals_made,
        b.field_goals_attempted,
        b.field_goal_pct,
        b.three_pointers_made,
        b.three_pointers_attempted,
        b.three_point_pct,
        a.true_shooting_pct,
        a.effective_fg_pct,

        -- Advanced Stats & Tiers
        a.usage_pct,
        a.offensive_rating,
        a.defensive_rating,
        a.usage_tier,
        a.impact_tier,
        a.shooting_efficiency_tier,
        a.minutes_based_role,

        -- Milestones & Indicators
        b.is_double_double,
        b.is_triple_double,
        a.is_versatile,
        a.is_defensive_specialist,
        a.is_three_and_d

    FROM basic_stats AS b
    LEFT JOIN adv_stats AS a
        ON b.game_id = a.game_id
        AND b.player_id = a.player_id
    LEFT JOIN games AS g
        ON b.game_id = g.game_id
)

SELECT * FROM final
