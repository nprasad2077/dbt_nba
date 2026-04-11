{{
    config(
        materialized='incremental',
        schema='marts',
        unique_key='shot_key',
        tags=["facts"],
        indexes=[
            {'columns': ['player_key']},
            {'columns': ['team_key']},
            {'columns': ['date_key']},
            {'columns': ['game_id']},
            {'columns': ['shot_source']}
        ]
    )
}}

{#
    Grain: One row per individual shot attempt (field goals + free throws).

    This fact table contains every shot from the shot chart dataset that
    successfully matched to a game_id, PLUS free throw attempts derived
    from box score data for those same player-games.

    Free throw rows have:
      - quarter_number = 0 (timing unknown)
      - shot_source = 'box_score_ft'
      - No x/y coordinates or score context

    Field goal rows have:
      - shot_source = 'shot_chart'
      - Full location and score context
#}

WITH shots_enriched AS (
    SELECT *
    FROM {{ ref('int_player_shots_enriched') }}
    WHERE has_game_match = TRUE
    {% if is_incremental() %}
      AND game_date >= (SELECT MAX(game_date) FROM {{ this }}) - INTERVAL '30 days'
    {% endif %}
),

final AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['se.shot_id', 'se.shot_source']) }} AS shot_key,

        -- Foreign Keys to Dimensions
        p.player_key,
        t.team_key,
        opp_t.team_key AS opponent_key,
        d.date_key,
        s.season_key,

        -- Degenerate Dimensions
        se.game_id,
        se.shot_id,

        -- Source Tracking
        se.shot_source,

        -- Game Context
        se.is_playoff,
        se.team_location,
        se.game_result,

        -- Quarter & Time Measures
        se.quarter_number,
        se.is_overtime_shot,
        se.seconds_remaining_in_quarter,

        -- Shot Location Measures (NULL for FTs)
        se.shot_x_coordinate,
        se.shot_y_coordinate,

        -- Shot Outcome Measures
        se.is_made,
        se.shot_made_flag,
        se.shot_missed_flag,

        -- Shot Type Measures
        se.shot_type_raw AS shot_type,
        se.shot_point_value,
        se.is_three_pointer,
        se.is_free_throw,
        se.distance_ft,
        se.shot_distance_zone,
        se.points_generated,

        -- Score Context Measures (NULL for FTs)
        se.team_had_lead,
        se.team_score_at_shot,
        se.opponent_score_at_shot,
        se.score_margin_at_shot,
        se.is_clutch_shot,

        -- Game Date for incremental logic
        CAST(se.game_date AS DATE) AS game_date

    FROM shots_enriched AS se

    LEFT JOIN {{ ref('dim_players') }} AS p
        ON se.player_id = p.player_id
    LEFT JOIN {{ ref('dim_teams') }} AS t
        ON se.team = t.team_abbr
    LEFT JOIN {{ ref('dim_teams') }} AS opp_t
        ON se.opponent = opp_t.team_abbr
    LEFT JOIN {{ ref('dim_dates') }} AS d
        ON CAST(se.game_date AS DATE) = d.full_date
    LEFT JOIN {{ ref('dim_seasons') }} AS s
        ON se.season_start_year = s.season_start_year
)

SELECT * FROM final