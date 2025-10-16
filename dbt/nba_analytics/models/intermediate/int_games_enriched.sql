{{
    config(
        materialized='table',
        schema='intermediate'
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
        -- Core game details
        games.game_id,
        games.game_date,
        games.season_start_year,
        games.is_playoff,
        games.home_team,
        games.visitor_team,
        games.winning_team,
        games.losing_team,
        games.point_differential,
        games.total_points,
        games.is_overtime,
        
        -- Home team stats (prefixed with 'home_')
        home_team_stats.points AS home_points,
        home_team_stats.offensive_rating AS home_offensive_rating,
        home_team_stats.defensive_rating AS home_defensive_rating,
        home_team_stats.net_rating AS home_net_rating,
        home_team_stats.pace AS home_pace,
        home_team_stats.effective_fg_pct AS home_effective_fg_pct,
        home_team_stats.turnover_rate AS home_turnover_rate,
        home_team_stats.offensive_tier AS home_offensive_tier,
        home_team_stats.defensive_tier AS home_defensive_tier,
        
        -- Visitor team stats (prefixed with 'visitor_')
        visitor_team_stats.points AS visitor_points,
        visitor_team_stats.offensive_rating AS visitor_offensive_rating,
        visitor_team_stats.defensive_rating AS visitor_defensive_rating,
        visitor_team_stats.net_rating AS visitor_net_rating,
        visitor_team_stats.pace AS visitor_pace,
        visitor_team_stats.effective_fg_pct AS visitor_effective_fg_pct,
        visitor_team_stats.turnover_rate AS visitor_turnover_rate,
        visitor_team_stats.offensive_tier AS visitor_offensive_tier,
        visitor_team_stats.defensive_tier AS visitor_defensive_tier,

        -- Calculated matchup metrics
        (home_team_stats.pace + visitor_team_stats.pace) / 2 AS matchup_pace,
        home_team_stats.offensive_rating - visitor_team_stats.defensive_rating AS home_off_vs_visitor_def,
        visitor_team_stats.offensive_rating - home_team_stats.defensive_rating AS visitor_off_vs_home_def

    FROM games
    
    -- Join for home team stats
    LEFT JOIN team_performance AS home_team_stats
        ON games.game_id = home_team_stats.game_id
        AND games.home_team = home_team_stats.team
        
    -- Join for visitor team stats
    LEFT JOIN team_performance AS visitor_team_stats
        ON games.game_id = visitor_team_stats.game_id
        AND games.visitor_team = visitor_team_stats.team
)

SELECT * FROM final