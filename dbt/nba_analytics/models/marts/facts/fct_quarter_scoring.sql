{{
    config(
        materialized='incremental',
        schema='marts',
        unique_key='quarter_scoring_key',
        tags=["facts"],
        indexes=[
            {'columns': ['date_key']},
            {'columns': ['team_key']},
            {'columns': ['opponent_key']}
        ]
    )
}}

WITH games AS (
    SELECT
        game_id,
        game_date,
        season_start_year
    FROM {{ ref('int_games_enriched') }}
    {% if is_incremental() %}
    WHERE game_date >= (SELECT MAX(game_date) FROM {{ this }}) - INTERVAL '30 days'
    {% endif %}
),

unpivoted_scores AS (
    -- This CTE unpivots the raw line scores data. The team abbreviations are still unconformed here.
    {{ dbt_utils.unpivot(
        relation=ref('stg_line_scores'),
        cast_to='INTEGER',
        exclude=['game_id', 'team', 'total_points', 'first_half_points', 'second_half_points', 'regulation_points', 'overtime_points', 'had_ot1', 'had_ot2', 'had_ot3', 'q2_momentum', 'q3_momentum', 'q4_momentum', 'max_quarter_score', 'min_quarter_score', 'best_quarter', 'created_at', 'updated_at', 'dbt_loaded_at'],
        field_name='period_name',
        value_name='points_scored'
    ) }}
    WHERE game_id IN (SELECT game_id FROM games) -- Pre-filter before the self-join for incremental performance
),

final AS (
    SELECT
        -- Surrogate Key using the conformed team abbreviation
        {{ dbt_utils.generate_surrogate_key(['team_scores.game_id', 'team_map.team_abbr', 'team_scores.period_name']) }} AS quarter_scoring_key,
        
        -- Foreign Keys from Dimensions
        d.date_key,
        s.season_key,
        t.team_key,
        opp.team_key AS opponent_key,

        -- Degenerate Dimensions
        team_scores.game_id,
        REPLACE(UPPER(team_scores.period_name), '_POINTS', '') AS period,
        
        -- Measures
        team_scores.points_scored,
        opponent_scores.points_scored AS opponent_points_scored,
        (team_scores.points_scored - opponent_scores.points_scored) AS period_point_differential,
        
        -- Game Date for partitioning
        CAST(g.game_date AS DATE) AS game_date
        
    FROM unpivoted_scores AS team_scores
    
    -- Perform the self-join to find the opponent's score for the same period
    INNER JOIN unpivoted_scores AS opponent_scores
        ON team_scores.game_id = opponent_scores.game_id
        AND team_scores.period_name = opponent_scores.period_name
        AND team_scores.team != opponent_scores.team
        
    -- Join to games table to get date/season info
    INNER JOIN games AS g
        ON team_scores.game_id = g.game_id
        
    -- *** FIX: CONFORM TEAM ABBREVIATIONS BEFORE JOINING TO DIMENSIONS ***
    -- Map the raw source abbreviation for the primary team to the conformed abbreviation
    LEFT JOIN {{ ref('team_maps') }} AS team_map
        ON team_scores.team = team_map.team_abbr

    -- Map the raw source abbreviation for the opponent team to the conformed abbreviation
    LEFT JOIN {{ ref('team_maps') }} AS opponent_map
        ON opponent_scores.team = opponent_map.team_abbr

    -- Now, join to dim_teams using the CONFORMED abbreviation
    LEFT JOIN {{ ref('dim_teams') }} AS t
        ON team_map.team_abbr = t.team_abbr
        
    -- And join to dim_teams again for the opponent using their CONFORMED abbreviation
    LEFT JOIN {{ ref('dim_teams') }} AS opp
        ON opponent_map.team_abbr = opp.team_abbr

    LEFT JOIN {{ ref('dim_dates') }} AS d
        ON CAST(g.game_date AS DATE) = d.full_date
        
    LEFT JOIN {{ ref('dim_seasons') }} AS s
        ON g.season_start_year = s.season_start_year

    -- Filter out rows for OT periods where no points were scored (i.e., the OT period didn't happen)
    WHERE team_scores.points_scored IS NOT NULL
)

SELECT * FROM final
