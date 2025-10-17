{{
    config(
        materialized='table',
        schema='intermediate',
        indexes=[
            {'columns': ['game_id'], 'unique': True},
            {'columns': ['game_date']},
            {'columns': ['home_team', 'visitor_team']},
            {'columns': ['season_start_year']}
        ]
    )
}}

WITH games AS (
    SELECT *
    FROM {{ ref('stg_games') }}
),

team_performance AS (
    SELECT *
    FROM {{ ref('int_team_performance') }}
),

final AS (
    SELECT
        -- Core Game Details
        games.game_id,
        games.game_date, -- FIX: Ensured this is game_date, not date
        games.season_start_year,
        games.is_playoff,
        games.arena,
        
        -- Team Identifiers
        games.home_team,
        games.visitor_team,
        games.winning_team,
        
        -- Final Score & Outcome
        games.home_points,
        games.visitor_points,
        games.point_differential,
        games.total_points,
        games.is_overtime,
        
        -- Home Team Performance (prefixed with 'home_')
        home_stats.offensive_rating AS home_offensive_rating,
        home_stats.defensive_rating AS home_defensive_rating,
        home_stats.net_rating AS home_net_rating,
        home_stats.pace AS home_pace,
        home_stats.effective_fg_pct AS home_effective_fg_pct,
        home_stats.turnover_rate AS home_turnover_rate,
        home_stats.offensive_tier AS home_offensive_tier,
        home_stats.defensive_tier AS home_defensive_tier,
        
        -- Visitor Team Performance (prefixed with 'visitor_')
        visitor_stats.offensive_rating AS visitor_offensive_rating,
        visitor_stats.defensive_rating AS visitor_defensive_rating,
        visitor_stats.net_rating AS visitor_net_rating,
        visitor_stats.pace AS visitor_pace,
        visitor_stats.effective_fg_pct AS visitor_effective_fg_pct,
        visitor_stats.turnover_rate AS visitor_visitor_turnover_rate,
        visitor_stats.offensive_tier AS visitor_offensive_tier,
        visitor_stats.defensive_tier AS visitor_defensive_tier,

        -- Calculated Matchup Metrics
        (home_stats.pace + visitor_stats.pace) / 2 AS matchup_pace,
        home_stats.offensive_rating - visitor_stats.defensive_rating AS home_off_vs_visitor_def_advantage,
        visitor_stats.offensive_rating - home_stats.defensive_rating AS visitor_off_vs_home_def_advantage

    FROM games
    
    -- Join for home team stats
    LEFT JOIN team_performance AS home_stats
        ON games.game_id = home_stats.game_id
        AND games.home_team = home_stats.team
        
    -- Join for visitor team stats
    LEFT JOIN team_performance AS visitor_stats
        ON games.game_id = visitor_stats.game_id
        AND games.visitor_team = visitor_stats.team
)

SELECT * FROM final
