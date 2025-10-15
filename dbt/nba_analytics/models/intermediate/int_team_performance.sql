{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['game_id', 'team'], 'unique': True},
            {'columns': ['team']},
            {'columns': ['game_date']},
            {'columns': ['season_start_year']}
        ]
    )
}}

WITH game_enriched AS (
    SELECT * FROM {{ ref('int_games_enriched') }}
),

team_stats_combined AS (
    SELECT
        -- Keys
        b.game_id,
        b.team,
        
        -- Game context from enriched games
        CASE 
            WHEN b.team = g.home_team THEN 'HOME'
            ELSE 'AWAY'
        END AS home_away,
        
        g.game_date,
        g.season_start_year,
        g.is_playoff,
        g.arena,
        
        CASE 
            WHEN b.team = g.winning_team THEN TRUE
            ELSE FALSE
        END AS is_win,
        
        CASE 
            WHEN b.team = g.home_team THEN g.visitor_team
            ELSE g.home_team
        END AS opponent,
        
        g.point_differential AS game_point_differential,
        g.is_overtime,
        g.game_competitiveness,
        g.was_clutch_game,
        
        -- Basic team stats
        b.minutes_played,
        b.field_goals_made,
        b.field_goals_attempted,
        b.field_goal_pct,
        b.three_pointers_made,
        b.three_pointers_attempted,
        b.three_point_pct,
        b.two_pointers_made,
        b.two_pointers_attempted,
        b.two_point_pct,
        b.free_throws_made,
        b.free_throws_attempted,
        b.free_throw_pct,
        b.offensive_rebounds,
        b.defensive_rebounds,
        b.total_rebounds,
        b.assists,
        b.steals,
        b.blocks,
        b.turnovers,
        b.personal_fouls,
        b.points,
        
        -- Four factors
        b.effective_fg_pct AS four_factors_efg_pct,
        b.turnover_rate AS four_factors_tov_rate,
        b.offensive_rebound_rate AS four_factors_orb_rate,
        b.free_throw_rate AS four_factors_ft_rate,
        
        -- Advanced stats
        a.true_shooting_pct,
        a.three_point_attempt_rate,
        a.free_throw_rate,
        a.offensive_rebound_pct,
        a.defensive_rebound_pct,
        a.total_rebound_pct,
        a.assist_pct,
        a.steal_pct,
        a.block_pct,
        a.turnover_pct,
        a.offensive_rating,
        a.defensive_rating,
        a.net_rating,
        
        -- Performance tiers
        a.offensive_tier,
        a.defensive_tier,
        a.shot_selection_style,
        a.ball_movement_style,
        a.offensive_rebounding_tier,
        a.defensive_rebounding_tier,
        a.ball_security_tier,
        a.defensive_activity,
        
        -- Pace and possessions
        b.possessions_estimate,
        b.pace,
        b.points_per_possession,
        b.ast_to_tov_ratio,
        
        -- Quarter-by-quarter performance
        CASE 
            WHEN b.team = g.home_team THEN g.home_q1
            ELSE g.visitor_q1
        END AS q1_points,
        
        CASE 
            WHEN b.team = g.home_team THEN g.home_q2
            ELSE g.visitor_q2
        END AS q2_points,
        
        CASE 
            WHEN b.team = g.home_team THEN g.home_q3
            ELSE g.visitor_q3
        END AS q3_points,
        
        CASE 
            WHEN b.team = g.home_team THEN g.home_q4
            ELSE g.visitor_q4
        END AS q4_points,
        
        CASE 
            WHEN b.team = g.home_team THEN g.home_first_half
            ELSE g.visitor_first_half
        END AS first_half_points,
        
        CASE 
            WHEN b.team = g.home_team THEN g.home_second_half
            ELSE g.visitor_second_half
        END AS second_half_points,
        
        -- Performance vs opponent
        b.points - CASE 
            WHEN b.team = g.home_team THEN g.visitor_points
            ELSE g.home_points
        END AS point_margin,
        
        -- Shooting performance categories
        CASE 
            WHEN b.field_goal_pct >= 0.50 THEN 'Excellent Shooting'
            WHEN b.field_goal_pct >= 0.45 THEN 'Good Shooting'
            WHEN b.field_goal_pct >= 0.40 THEN 'Average Shooting'
            ELSE 'Poor Shooting'
        END AS shooting_performance,
        
        -- Three point volume
        CASE 
            WHEN b.three_pointers_made >= 15 THEN 'Three Point Barrage'
            WHEN b.three_pointers_made >= 12 THEN 'High Three Volume'
            WHEN b.three_pointers_made >= 9 THEN 'Average Three Volume'
            ELSE 'Low Three Volume'
        END AS three_point_volume,
        
        -- Overall game grade (A-F scale)
        CASE 
            WHEN a.net_rating >= 20 AND b.field_goal_pct >= 0.50 THEN 'A+'
            WHEN a.net_rating >= 15 AND b.field_goal_pct >= 0.48 THEN 'A'
            WHEN a.net_rating >= 10 AND b.field_goal_pct >= 0.46 THEN 'A-'
            WHEN a.net_rating >= 5 AND b.field_goal_pct >= 0.44 THEN 'B+'
            WHEN a.net_rating >= 0 AND b.field_goal_pct >= 0.42 THEN 'B'
            WHEN a.net_rating >= -5 AND b.field_goal_pct >= 0.40 THEN 'B-'
            WHEN a.net_rating >= -10 THEN 'C'
            WHEN a.net_rating >= -15 THEN 'D'
            ELSE 'F'
        END AS performance_grade,
        
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM {{ ref('stg_team_game_basic_stats') }} b
    LEFT JOIN {{ ref('stg_team_game_adv_stats') }} a
        ON b.game_id = a.game_id AND b.team = a.team
    LEFT JOIN game_enriched g
        ON b.game_id = g.game_id
)

SELECT * FROM team_stats_combined
