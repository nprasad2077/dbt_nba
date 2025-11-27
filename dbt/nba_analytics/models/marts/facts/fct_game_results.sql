{{
    config(
        materialized='incremental',
        schema='marts',
        unique_key='game_key',
        tags=["facts"],
        indexes=[
            {'columns': ['date_key']},
            {'columns': ['home_team_key']},
            {'columns': ['visitor_team_key']},
            {'columns': ['winning_team_key']}
        ]
    )
}}

WITH games_enriched AS (
    SELECT *
    FROM {{ ref('int_games_enriched') }}
    {% if is_incremental() %}
    WHERE game_date >= (SELECT MAX(game_date) FROM {{ this }}) - INTERVAL '30 days'
    {% endif %}
)

SELECT
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['ge.game_id']) }} AS game_key,

    -- Foreign Keys from Dimensions
    d.date_key,
    s.season_key,
    a.arena_key,
    home_team.team_key AS home_team_key,
    visitor_team.team_key AS visitor_team_key,
    winning_team.team_key AS winning_team_key,

    -- Degenerate Dimension
    ge.game_id,

    -- Core Outcome Measures & Context
    ge.home_points,
    ge.visitor_points,
    ge.point_differential,
    ge.total_points,
    ge.is_playoff,
    ge.is_overtime,
    CASE WHEN ge.home_points > ge.visitor_points THEN TRUE ELSE FALSE END AS is_home_team_winner,

    -- Matchup Performance Measures
    ge.home_net_rating,
    ge.visitor_net_rating,
    ge.matchup_pace,

    -- Derived Analytical Measures
    CASE
        WHEN ge.point_differential <= 5 THEN 'Clutch Game'
        WHEN ge.point_differential <= 10 THEN 'Competitive'
        WHEN ge.point_differential <= 20 THEN 'Decisive'
        ELSE 'Blowout'
    END AS game_competitiveness_tier,

    -- *** FIX: REMOVED is_potential_upset ***
    -- The upstream logic was flawed and always produced FALSE.
    -- This metric should be revisited if pre-game ranking/odds data becomes available.

    -- Game Date for partitioning / incremental logic
    CAST(ge.game_date AS DATE) AS game_date

FROM games_enriched AS ge

LEFT JOIN {{ ref('dim_dates') }} AS d
    ON CAST(ge.game_date AS DATE) = d.full_date
LEFT JOIN {{ ref('dim_seasons') }} AS s
    ON ge.season_start_year = s.season_start_year
LEFT JOIN {{ ref('dim_arenas') }} AS a
    ON ge.arena = a.arena_name AND ge.arena_city = a.arena_city
LEFT JOIN {{ ref('dim_teams') }} AS home_team
    ON ge.home_team = home_team.team_abbr
LEFT JOIN {{ ref('dim_teams') }} AS visitor_team
    ON ge.visitor_team = visitor_team.team_abbr
LEFT JOIN {{ ref('dim_teams') }} AS winning_team
    ON ge.winning_team = winning_team.team_abbr
