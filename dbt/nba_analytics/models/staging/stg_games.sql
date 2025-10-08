{{
    config(
        materialized='view',
        schema='staging',
        alias='stg_games'
    )
}}

WITH source_data AS (
    SELECT * FROM {{ source('raw_nba', 'games') }}
    WHERE deleted_at IS NULL  -- Exclude soft-deleted records
),

cleaned AS (
    SELECT
        -- Primary Key
        game_id,
        
        -- Foreign Keys (will be created in intermediate layer)
        home_team,
        visitor_team,
        
        -- Game Information
        date AS game_date,
        EXTRACT(YEAR FROM date) AS season_year,
        CASE 
            WHEN EXTRACT(MONTH FROM date) >= 10 THEN EXTRACT(YEAR FROM date)
            ELSE EXTRACT(YEAR FROM date) - 1
        END AS season_start_year,
        EXTRACT(MONTH FROM date) AS game_month,
        EXTRACT(DAY FROM date) AS game_day,
        TO_CHAR(date, 'Day') AS game_day_of_week,
        is_playoff,
        
        -- Time Information
        start_time_et,
        CASE 
            WHEN start_time_et LIKE '%12:%p%' THEN 'Afternoon'
            WHEN start_time_et LIKE '%1:%p%' OR start_time_et LIKE '%2:%p%' OR start_time_et LIKE '%3:%p%' THEN 'Afternoon'
            WHEN start_time_et LIKE '%7:%p%' OR start_time_et LIKE '%8:%p%' THEN 'Prime Time'
            ELSE 'Late Night'
        END AS game_time_slot,
        
        -- Location
        arena,
        
        -- Scores
        home_pts AS home_points,
        visitor_pts AS visitor_points,
        
        -- Derived Fields
        CASE 
            WHEN home_pts > visitor_pts THEN home_team
            WHEN visitor_pts > home_pts THEN visitor_team
            ELSE NULL
        END AS winning_team,
        
        CASE 
            WHEN home_pts < visitor_pts THEN home_team
            WHEN visitor_pts < home_pts THEN visitor_team
            ELSE NULL
        END AS losing_team,
        
        CASE 
            WHEN home_pts > visitor_pts THEN 'HOME'
            WHEN visitor_pts > home_pts THEN 'AWAY'
            ELSE NULL
        END AS winner_location,
        
        ABS(home_pts - visitor_pts) AS point_differential,
        home_pts + visitor_pts AS total_points,
        
        -- Game Duration
        game_duration,
        CASE 
            WHEN game_duration LIKE '%OT%' THEN TRUE
            ELSE FALSE
        END AS is_overtime,
        
        -- URL
        box_score_url,
        
        -- Metadata
        created_at,
        updated_at,
        CURRENT_TIMESTAMP AS dbt_loaded_at
        
    FROM source_data
    WHERE 
        -- Data quality filters
        game_id IS NOT NULL
        AND date IS NOT NULL
        AND home_team IS NOT NULL
        AND visitor_team IS NOT NULL
)

SELECT * FROM cleaned