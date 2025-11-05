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
    -- Use the team_mappings seed to get standardized team abbreviations
    SELECT
        g.*,
        home_map.team_abbr AS home_team_abbr,
        visitor_map.team_abbr AS visitor_team_abbr,
        winning_map.team_abbr AS winning_team_abbr
    FROM {{ ref('stg_games') }} AS g
    LEFT JOIN {{ ref('team_mappings') }} AS home_map ON g.home_team = home_map.full_name
    LEFT JOIN {{ ref('team_mappings') }} AS visitor_map ON g.visitor_team = visitor_map.full_name
    LEFT JOIN {{ ref('team_mappings') }} AS winning_map ON g.winning_team = winning_map.full_name
),

team_performance AS (
    SELECT *
    FROM {{ ref('int_team_performance') }}
),

final AS (
    SELECT
        -- Core Game Details
        games.game_id,
        games.game_date,
        games.season_start_year,
        games.is_playoff,
        games.arena,
        
        -- Team Identifiers (using conformed abbreviations)
        games.home_team_abbr AS home_team,
        games.visitor_team_abbr AS visitor_team,
        games.winning_team_abbr AS winning_team,
        
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
        visitor_stats.turnover_rate AS visitor_turnover_rate, -- Typo fixed
        visitor_stats.offensive_tier AS visitor_offensive_tier,
        visitor_stats.defensive_tier AS visitor_defensive_tier,

        -- Calculated Matchup Metrics
        (home_stats.pace + visitor_stats.pace) / 2 AS matchup_pace,
        home_stats.offensive_rating - visitor_stats.defensive_rating AS home_off_vs_visitor_def_advantage,
        visitor_stats.offensive_rating - home_stats.defensive_rating AS visitor_off_vs_home_def_advantage

    FROM games
    
    -- Join for home team stats using the conformed abbreviation
    LEFT JOIN team_performance AS home_stats
        ON games.game_id = home_stats.game_id
        AND games.home_team_abbr = home_stats.team
        
    -- Join for visitor team stats using the conformed abbreviation
    LEFT JOIN team_performance AS visitor_stats
        ON games.game_id = visitor_stats.game_id
        AND games.visitor_team_abbr = visitor_stats.team
)

SELECT * FROM final
