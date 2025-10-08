{{
    config(
        materialized='view',
        schema='staging',
        alias='stg_line_scores'
    )
}}

WITH source_data AS (
    -- Select and filter the raw source data
    SELECT *
    FROM {{ source('raw_nba', 'line_scores') }}
    WHERE deleted_at IS NULL
),

cleaned_and_transformed AS (
    SELECT
        -- Keys
        game_id,
        team,

        -- Quarter Scores (coalesce NULLs to 0)
        COALESCE(q1, 0) AS q1_points,
        COALESCE(q2, 0) AS q2_points,
        COALESCE(q3, 0) AS q3_points,
        COALESCE(q4, 0) AS q4_points,

        -- Overtime Scores (coalesce NULLs to 0)
        COALESCE(ot1, 0) AS ot1_points,
        COALESCE(ot2, 0) AS ot2_points,
        COALESCE(ot3, 0) AS ot3_points,

        -- Total Points
        total AS total_points,

        -- Derived Fields: Half scores
        COALESCE(q1, 0) + COALESCE(q2, 0) AS first_half_points,
        COALESCE(q3, 0) + COALESCE(q4, 0) AS second_half_points,

        -- Derived Fields: Regulation vs Overtime points
        COALESCE(q1, 0) + COALESCE(q2, 0) + COALESCE(q3, 0) + COALESCE(q4, 0) AS regulation_points,
        COALESCE(ot1, 0) + COALESCE(ot2, 0) + COALESCE(ot3, 0) AS overtime_points,

        -- CORRECTED LOGIC: A game had overtime only if points were scored in that period.
        -- The previous logic `(ot1 > 0 OR ot1 IS NOT NULL)` was incorrect because
        -- for non-overtime games, ot1 is 0, which is not null, resulting in TRUE.
        COALESCE(ot1, 0) > 0 AS had_ot1,
        COALESCE(ot2, 0) > 0 AS had_ot2,
        COALESCE(ot3, 0) > 0 AS had_ot3,

        -- Derived Fields: Quarter-by-quarter momentum
        COALESCE(q2, 0) - COALESCE(q1, 0) AS q2_momentum,
        COALESCE(q3, 0) - COALESCE(q2, 0) AS q3_momentum,
        COALESCE(q4, 0) - COALESCE(q3, 0) AS q4_momentum,

        -- Derived Fields: Scoring consistency metrics
        GREATEST(COALESCE(q1, 0), COALESCE(q2, 0), COALESCE(q3, 0), COALESCE(q4, 0)) AS max_quarter_score,
        LEAST(COALESCE(q1, 0), COALESCE(q2, 0), COALESCE(q3, 0), COALESCE(q4, 0)) AS min_quarter_score,

        -- Derived Fields: Identify the best scoring quarter.
        -- Note: In case of a tie, this will return the first quarter that matches the max score.
        CASE
            WHEN GREATEST(COALESCE(q1, 0), COALESCE(q2, 0), COALESCE(q3, 0), COALESCE(q4, 0)) = COALESCE(q1, 0) THEN 'Q1'
            WHEN GREATEST(COALESCE(q1, 0), COALESCE(q2, 0), COALESCE(q3, 0), COALESCE(q4, 0)) = COALESCE(q2, 0) THEN 'Q2'
            WHEN GREATEST(COALESCE(q1, 0), COALESCE(q2, 0), COALESCE(q3, 0), COALESCE(q4, 0)) = COALESCE(q3, 0) THEN 'Q3'
            ELSE 'Q4'
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

SELECT * FROM cleaned_and_transformed

