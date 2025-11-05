{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH all_seasons AS (
    -- Get a unique list of all seasons from the enriched games model
    SELECT DISTINCT 
        season_start_year
    FROM {{ ref('int_games_enriched') }}
)

SELECT
    -- Surrogate Key: A durable, unique key for the season
    {{ dbt_utils.generate_surrogate_key(['season_start_year']) }} AS season_key,

    -- Season Attributes
    season_start_year,
    
    -- Create a user-friendly display name, e.g., '2022-23'
    season_start_year || '-' || SUBSTR(CAST(season_start_year + 1 AS VARCHAR), 3, 2) AS season_display

FROM all_seasons
ORDER BY season_start_year
