{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['game_id'], 'unique': True},
            {'columns': ['game_date']},
            {'columns': ['home_team']},
            {'columns': ['visitor_team']},
            {'columns': ['season_start_year']}
        ]
    )
}}

WITH game_base AS (
    SELECT
        g.game_id,
        g.game_date,
        g.season_year,
        g.season_start_year,
        g.game_month,
        g.game_day,
        g.game_day_of_week,
        g.is_playoff,
        g.game_time_slot,
        g.arena,
        g.home_team,
        g.visitor_team,
        g.home_points,
        g.visitor_points,
        g.winning_team,
        g.losing_team,
        g.winner_location,
        g.point_differential,
        g.total_points,
        g.game_duration,
        g.is_overtime,
        g.created_at,
        g.updated_at
    FROM {{ ref('stg_games') }} g
),

home_line_scores AS (
    SELECT
        ls.game_id,
        ls.q1_points AS home_q1,
        ls.q2_points AS home_q2,
        ls.q3_points AS home_q3,
        ls.q4_points AS home_q4,
        ls.ot1_points AS home_ot1,
        ls.ot2_points AS home_ot2,
        ls.ot3_points AS home_ot3,
        ls.first_half_points AS home_first_half,
        ls.second_half_points AS home_second_half,
        ls.regulation_points AS home_regulation_points,
        ls.overtime_points AS home_overtime_points,
        ls.max_quarter_score AS home_max_quarter,
        ls.min_quarter_score AS home_min_quarter,
        ls.best_quarter AS home_best_quarter,
        ls.q2_momentum AS home_q2_momentum,
        ls.q3_momentum AS home_q3_momentum,
        ls.q4_momentum AS home_q4_momentum
    FROM {{ ref('stg_line_scores') }} ls
    INNER JOIN game_base g ON ls.game_id = g.game_id AND ls.team = g.home_team
),

visitor_line_scores AS (
    SELECT
        ls.game_id,
        ls.q1_points AS visitor_q1,
        ls.q2_points AS visitor_q2,
        ls.q3_points AS visitor_q3,
        ls.q4_points AS visitor_q4,
        ls.ot1_points AS visitor_ot1,
        ls.ot2_points AS visitor_ot2,
        ls.ot3_points AS visitor_ot3,
        ls.first_half_points AS visitor_first_half,
        ls.second_half_points AS visitor_second_half,
        ls.regulation_points AS visitor_regulation_points,
        ls.overtime_points AS visitor_overtime_points,
        ls.max_quarter_score AS visitor_max_quarter,
        ls.min_quarter_score AS visitor_min_quarter,
        ls.best_quarter AS visitor_best_quarter,
        ls.q2_momentum AS visitor_q2_momentum,
        ls.q3_momentum AS visitor_q3_momentum,
        ls.q4_momentum AS visitor_q4_momentum
    FROM {{ ref('stg_line_scores') }} ls
    INNER JOIN game_base g ON ls.game_id = g.game_id AND ls.team = g.visitor_team
),

games_with_quarters AS (
    SELECT
        g.*,
        -- Home team quarters
        h.home_q1,
        h.home_q2,
        h.home_q3,
        h.home_q4,
        h.home_ot1,
        h.home_ot2,
        h.home_ot3,
        h.home_first_half,
        h.home_second_half,
        h.home_regulation_points,
        h.home_overtime_points,
        h.home_max_quarter,
        h.home_min_quarter,
        h.home_best_quarter,
        
        -- Visitor team quarters
        v.visitor_q1,
        v.visitor_q2,
        v.visitor_q3,
        v.visitor_q4,
        v.visitor_ot1,
        v.visitor_ot2,
        v.visitor_ot3,
        v.visitor_first_half,
        v.visitor_second_half,
        v.visitor_regulation_points,
        v.visitor_overtime_points,
        v.visitor_max_quarter,
        v.visitor_min_quarter,
        v.visitor_best_quarter,
        
        -- Quarter differentials
        h.home_q1 - v.visitor_q1 AS q1_differential,
        h.home_q2 - v.visitor_q2 AS q2_differential,
        h.home_q3 - v.visitor_q3 AS q3_differential,
        h.home_q4 - v.visitor_q4 AS q4_differential,
        
        -- Half differentials
        h.home_first_half - v.visitor_first_half AS first_half_differential,
        h.home_second_half - v.visitor_second_half AS second_half_differential,
        
        -- Lead changes and momentum
        CASE 
            WHEN h.home_q1 > v.visitor_q1 THEN 'home'
            WHEN v.visitor_q1 > h.home_q1 THEN 'visitor'
            ELSE 'tied'
        END AS q1_leader,
        
        CASE 
            WHEN h.home_first_half > v.visitor_first_half THEN 'home'
            WHEN v.visitor_first_half > h.home_first_half THEN 'visitor'
            ELSE 'tied'
        END AS halftime_leader,
        
        -- Comeback indicators
        CASE 
            WHEN h.home_first_half < v.visitor_first_half 
                 AND g.winner_location = 'HOME' THEN TRUE
            WHEN v.visitor_first_half < h.home_first_half 
                 AND g.winner_location = 'AWAY' THEN TRUE
            ELSE FALSE
        END AS is_comeback_win,
        
        -- Largest lead calculations
        GREATEST(
            ABS(h.home_q1 - v.visitor_q1),
            ABS((h.home_q1 + h.home_q2) - (v.visitor_q1 + v.visitor_q2)),
            ABS((h.home_q1 + h.home_q2 + h.home_q3) - (v.visitor_q1 + v.visitor_q2 + v.visitor_q3)),
            ABS(h.home_regulation_points - v.visitor_regulation_points)
        ) AS largest_lead,
        
        -- Close game indicators
        CASE 
            WHEN g.point_differential <= 5 THEN 'Very Close'
            WHEN g.point_differential <= 10 THEN 'Close'
            WHEN g.point_differential <= 20 THEN 'Competitive'
            ELSE 'Blowout'
        END AS game_competitiveness,
        
        -- Scoring pace indicators
        CASE 
            WHEN g.total_points >= 240 THEN 'Very High Scoring'
            WHEN g.total_points >= 220 THEN 'High Scoring'
            WHEN g.total_points >= 200 THEN 'Average Scoring'
            WHEN g.total_points >= 180 THEN 'Low Scoring'
            ELSE 'Very Low Scoring'
        END AS scoring_pace_category,
        
        -- Momentum swings
        h.home_q2_momentum + h.home_q3_momentum + h.home_q4_momentum AS home_total_momentum,
        v.visitor_q2_momentum + v.visitor_q3_momentum + v.visitor_q4_momentum AS visitor_total_momentum,
        
        -- Clutch time indicator (4th quarter within 5 points)
        CASE 
            WHEN ABS((h.home_q1 + h.home_q2 + h.home_q3) - 
                    (v.visitor_q1 + v.visitor_q2 + v.visitor_q3)) <= 5 THEN TRUE
            ELSE FALSE
        END AS was_clutch_game,
        
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM game_base g
    LEFT JOIN home_line_scores h ON g.game_id = h.game_id
    LEFT JOIN visitor_line_scores v ON g.game_id = v.game_id
)

SELECT * FROM games_with_quarters
