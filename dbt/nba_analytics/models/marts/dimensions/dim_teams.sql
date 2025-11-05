{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH team_mappings AS (
    SELECT 
        team_abbr,
        full_name
    FROM {{ ref('team_mappings') }}
)

SELECT
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['team_abbr']) }} AS team_key,

    -- Team Identifiers
    team_abbr,
    full_name AS team_full_name

    -- We can add more dimensional attributes here in the future
    -- e.g., conference, division, city, etc.

FROM team_mappings
ORDER BY team_abbr
