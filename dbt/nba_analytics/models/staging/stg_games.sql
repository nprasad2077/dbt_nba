{{
    config(
        materialized='view',
        schema='staging',
        alias='stg_games'
    )
}}

-- *** FIX: CTE to reliably identify games that had overtime scoring ***
WITH overtime_games AS (
    SELECT
        game_id,
        MAX(CASE WHEN overtime_points > 0 THEN 1 ELSE 0 END) AS had_overtime
    FROM {{ ref('stg_line_scores') }}
    GROUP BY 1
),

source_data AS (
    SELECT * FROM {{ source('raw_nba', 'games') }}
    WHERE deleted_at IS NULL  -- Exclude soft-deleted records
),

cleaned AS (
    SELECT
        -- Primary Key
        g.game_id,

        -- Foreign Keys (will be created in intermediate layer)
        g.home_team,
        g.visitor_team,

        -- Game Information
        g.date AS game_date,
        EXTRACT(YEAR FROM g.date) AS season_year,
        CASE
            WHEN EXTRACT(MONTH FROM g.date) >= 10 THEN EXTRACT(YEAR FROM g.date)
            ELSE EXTRACT(YEAR FROM g.date) - 1
        END AS season_start_year,
        EXTRACT(MONTH FROM g.date) AS game_month,
        EXTRACT(DAY FROM g.date) AS game_day,
        TO_CHAR(g.date, 'Day') AS game_day_of_week,
        g.is_playoff,

        -- Time Information
        g.start_time_et,
        CASE
            WHEN g.start_time_et LIKE '%12:%p%' THEN 'Afternoon'
            WHEN g.start_time_et LIKE ANY (ARRAY['%1:%p%', '%2:%p%', '%3:%p%']) THEN 'Afternoon'
            WHEN g.start_time_et LIKE ANY (ARRAY['%7:%p%', '%8:%p%']) THEN 'Prime Time'
            ELSE 'Late Night'
        END AS game_time_slot,

        -- Location
        g.arena,

        -- Scores
        g.home_pts AS home_points,
        g.visitor_pts AS visitor_points,

        -- Derived Fields
        CASE
            WHEN g.home_pts > g.visitor_pts THEN g.home_team
            WHEN g.visitor_pts > g.home_pts THEN g.visitor_team
            ELSE NULL
        END AS winning_team,

        CASE
            WHEN g.home_pts < g.visitor_pts THEN g.home_team
            WHEN g.visitor_pts < g.home_pts THEN g.visitor_team
            ELSE NULL
        END AS losing_team,

        CASE
            WHEN g.home_pts > g.visitor_pts THEN 'HOME'
            WHEN g.visitor_pts > g.home_pts THEN 'AWAY'
            ELSE NULL
        END AS winner_location,

        ABS(g.home_pts - g.visitor_pts) AS point_differential,
        g.home_pts + g.visitor_pts AS total_points,

        -- Game Duration
        g.game_duration,

        -- *** FIX: Replaced fragile string matching with a reliable check against scoring data ***
        CASE
            WHEN ot.had_overtime = 1 THEN TRUE
            ELSE FALSE
        END AS is_overtime,

        -- URL
        g.box_score_url,

        -- Metadata
        g.created_at,
        g.updated_at,
        CURRENT_TIMESTAMP AS dbt_loaded_at

    FROM source_data AS g

    -- *** FIX: Join to the overtime data to get the reliable flag ***
    LEFT JOIN overtime_games AS ot
        ON g.game_id = ot.game_id

    WHERE
        -- Data quality filters
        g.game_id IS NOT NULL
        AND g.date IS NOT NULL
        AND g.home_team IS NOT NULL
        AND g.visitor_team IS NOT NULL
)

SELECT * FROM cleaned
