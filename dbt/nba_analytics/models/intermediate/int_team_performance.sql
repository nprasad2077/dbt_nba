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
    -- Use the team_mappings seed to get standardized team abbreviations
    SELECT 
        g.game_id,
        g.game_date,
        g.season_start_year,
        g.is_playoff,
        home_map.team_abbr AS home_team_abbr,
        visitor_map.team_abbr AS visitor_team_abbr,
        winning_map.team_abbr AS winning_team_abbr
    FROM {{ ref('stg_games') }} g
    LEFT JOIN {{ ref('team_mappings') }} AS home_map ON g.home_team = home_map.full_name
    LEFT JOIN {{ ref('team_mappings') }} AS visitor_map ON g.visitor_team = visitor_map.full_name
    LEFT JOIN {{ ref('team_mappings') }} AS winning_map ON g.winning_team = winning_map.full_name
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
        -- Corrected CASE statements using conformed abbreviations
        CASE 
            WHEN b.team = g.winning_team_abbr THEN 'W'
            ELSE 'L'
        END AS game_result,
        CASE 
            WHEN b.team = g.home_team_abbr THEN g.visitor_team_abbr
            ELSE g.home_team_abbr
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
