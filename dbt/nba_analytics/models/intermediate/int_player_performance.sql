{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['game_id', 'player_id'], 'unique': True},
            {'columns': ['player_id']},
            {'columns': ['team']},
            {'columns': ['game_date']}
        ]
    )
}}

WITH game_info AS (
    SELECT
        game_id,
        game_date,
        season_start_year,
        is_playoff,
        winning_team,
        point_differential,
        is_overtime
    FROM {{ ref('int_games_enriched') }}
),

player_combined AS (
    SELECT
        -- Keys
        b.game_id,
        b.player_id,
        b.team,
        
        -- Game context
        g.game_date,
        g.season_start_year,
        g.is_playoff,
        CASE 
            WHEN b.team = g.winning_team THEN TRUE 
            ELSE FALSE 
        END AS is_win,
        g.point_differential,
        g.is_overtime,
        
        -- Player info
        b.player_name,
        b.player_status,
        b.did_play,
        
        -- Playing time
        b.minutes_played,
        b.likely_starter,
        
        -- Basic stats
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
        b.game_score,
        b.plus_minus,
        b.points_per_shot,
        b.is_double_double,
        b.is_triple_double,
        
        -- Advanced stats
        a.true_shooting_pct,
        a.effective_fg_pct,
        a.three_point_attempt_rate,
        a.free_throw_rate,
        a.offensive_rebound_pct,
        a.defensive_rebound_pct,
        a.total_rebound_pct,
        a.assist_pct,
        a.steal_pct,
        a.block_pct,
        a.turnover_pct,
        a.usage_pct,
        a.offensive_rating,
        a.defensive_rating,
        a.net_rating,
        a.box_plus_minus,
        
        -- Performance categories
        a.shooting_efficiency_tier,
        a.usage_tier,
        a.impact_tier,
        a.is_versatile,
        a.is_defensive_specialist,
        a.is_three_and_d,
        
        -- Fantasy points (standard scoring)
        b.points 
        + (b.total_rebounds * 1.2) 
        + (b.assists * 1.5) 
        + (b.steals * 3) 
        + (b.blocks * 3) 
        - (b.turnovers * 1) AS fantasy_points,
        
        -- Per-36 minute stats (for players with 10+ minutes)
        CASE 
            WHEN b.minutes_played >= 10 THEN b.points * 36.0 / b.minutes_played
            ELSE NULL
        END AS points_per_36,
        
        CASE 
            WHEN b.minutes_played >= 10 THEN b.total_rebounds * 36.0 / b.minutes_played
            ELSE NULL
        END AS rebounds_per_36,
        
        CASE 
            WHEN b.minutes_played >= 10 THEN b.assists * 36.0 / b.minutes_played
            ELSE NULL
        END AS assists_per_36,
        
        -- Efficiency metrics
        CASE 
            WHEN b.minutes_played >= 10 THEN 
                (b.points + b.total_rebounds + b.assists + b.steals + b.blocks - 
                (b.field_goals_attempted - b.field_goals_made) - 
                (b.free_throws_attempted - b.free_throws_made) - b.turnovers) / b.minutes_played
            ELSE NULL
        END AS efficiency_per_minute,
        
        -- Role classification
        CASE 
            WHEN b.minutes_played >= 32 AND a.usage_pct >= 28 THEN 'Star'
            WHEN b.minutes_played >= 28 AND a.usage_pct >= 23 THEN 'Key Player'
            WHEN b.minutes_played >= 20 THEN 'Rotation Player'
            WHEN b.minutes_played >= 10 THEN 'Bench Player'
            WHEN b.minutes_played > 0 THEN 'Garbage Time'
            ELSE 'DNP'
        END AS player_role,
        
        -- Performance rating (composite score)
        CASE 
            WHEN b.minutes_played >= 10 THEN
                (
                    -- Normalize each component to 0-10 scale
                    LEAST(b.game_score / 40.0 * 10, 10) * 0.3 +
                    LEAST((a.net_rating + 20) / 40.0 * 10, 10) * 0.3 +
                    LEAST((a.box_plus_minus + 10) / 20.0 * 10, 10) * 0.2 +
                    LEAST(a.true_shooting_pct * 10, 10) * 0.2
                )
            ELSE NULL
        END AS performance_rating,
        
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM {{ ref('stg_player_game_basic_stats') }} b
    LEFT JOIN {{ ref('stg_player_game_adv_stats') }} a
        ON b.game_id = a.game_id 
        AND b.player_id = a.player_id
    LEFT JOIN game_info g
        ON b.game_id = g.game_id
)

SELECT * FROM player_combined
WHERE did_play = TRUE  -- Only include players who actually played
