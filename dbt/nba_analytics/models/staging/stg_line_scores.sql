{{
    config(
        materialized='view',
        schema='staging',
        alias='stg_line_scores'
    )
}}

WITH source_data AS (
    SELECT * FROM {{ source('raw_nba', 'line_scores') }}
    WHERE deleted_at IS NULL
),

cleaned AS (
    SELECT
        -- Keys
        game_id,
        team,
        
        -- Quarter Scores
        COALESCE(q1, 0) AS q1_points,
        COALESCE(q2, 0) AS q2_points,
        COALESCE(q3, 0) AS q3_points,
        COALESCE(q4, 0) AS q4_points,
        
        -- Overtime Scores
        COALESCE(ot1, 0) AS ot1_points,
        COALESCE(ot2, 0) AS ot2_points,
        COALESCE(ot3, 0) AS ot3_points,
        
        -- Total
        total AS total_points,
        
        -- Derived Fields - Half scores
        COALESCE(q1, 0) + COALESCE(q2, 0) AS first_half_points,
        COALESCE(q3, 0) + COALESCE(q4, 0) AS second_half_points,
        
        -- Regulation vs Overtime
        COALESCE(q1, 0) + COALESCE(q2, 0) + COALESCE(q3, 0) + COALESCE(q4, 0) AS regulation_points,
        COALESCE(ot1, 0) + COALESCE(ot2, 0) + COALESCE(ot3, 0) AS overtime_points,
        
        -- Overtime Indicators
        CASE WHEN ot1 > 0 OR ot1 IS NOT NULL THEN TRUE ELSE FALSE END AS had_ot1,
        CASE WHEN ot2 > 0 OR ot2 IS NOT NULL THEN TRUE ELSE FALSE END AS had_ot2,
        CASE WHEN ot3 > 0 OR ot3 IS NOT NULL THEN TRUE ELSE FALSE END AS had_ot3,
        
        -- Quarter-by-quarter momentum
        COALESCE(q2, 0) - COALESCE(q1, 0) AS q2_momentum,
        COALESCE(q3, 0) - COALESCE(q2, 0) AS q3_momentum,
        COALESCE(q4, 0) - COALESCE(q3, 0) AS q4_momentum,
        
        -- Scoring consistency (will calculate std dev in intermediate layer)
        GREATEST(COALESCE(q1, 0), COALESCE(q2, 0), COALESCE(q3, 0), COALESCE(q4, 0)) AS max_quarter_score,
        LEAST(COALESCE(q1, 0), COALESCE(q2, 0), COALESCE(q3, 0), COALESCE(q4, 0)) AS min_quarter_score,
        
        -- Best/Worst quarters
        CASE 
            WHEN GREATEST(COALESCE(q1, 0), COALESCE(q2, 0), COALESCE(q3, 0), COALESCE(q4, 0)) = COALESCE(q1, 0) THEN 'Q1'
            WHEN GREATEST(COALESCE(q1, 0), COALESCE(q2, 0), COALESCE(q3, 0), COALESCE(q4, 0)) = COALESCE(q2, 0) THEN 'Q2'
            WHEN GREATEST(COALESCE(q1, 0), COALESCE(q2, 0), COALESCE(q3, 0), COALESCE(q4, 0)) = COALESCE(q3, 0) THEN 'Q3'
            WHEN GREATEST(COALESCE(q1, 0), COALESCE(q2, 0), COALESCE(q3, 0), COALESCE(q4, 0)) = COALESCE(q4, 0) THEN 'Q4'
        END AS best_quarter,
        
        -- Metadata
        created_at,
        updated_at,
        CURRENT_TIMESTAMP AS dbt_loaded_at
        
    FROM source_data
    WHERE 
        game_id IS NOT NULL
        AND team IS NOT NULL
)

SELECT * FROM cleaned