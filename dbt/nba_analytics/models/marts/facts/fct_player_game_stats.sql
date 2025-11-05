{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH base_player_stats AS (
    SELECT
        game_id,
        player_id,
        team AS team_abbreviation,
        minutes_played,
        field_goals_made,
        field_goals_attempted,
        three_pointers_made,
        three_pointers_attempted,
        free_throws_made,
        free_throws_attempted,
        offensive_rebounds,
        defensive_rebounds,
        total_rebounds,
        assists,
        steals,
        blocks,
        turnovers,
        personal_fouls,
        points,
        plus_minus
    FROM {{ ref('stg_player_game_basic_stats') }}
    WHERE did_play = true
),

adv_player_stats AS (
    SELECT
        game_id,
        player_id,
        team AS team_abbreviation,
        true_shooting_pct,
        effective_fg_pct,
        three_point_attempt_rate,
        free_throw_rate,
        offensive_rebound_pct,
        defensive_rebound_pct,
        total_rebound_pct,
        assist_pct,
        steal_pct,
        block_pct,
        turnover_pct,
        usage_pct,
        offensive_rating,
        defensive_rating,
        box_plus_minus
    FROM {{ ref('stg_player_game_adv_stats') }}
),

game_details AS (
    SELECT
        game_id,
        game_date,
        season_start_year,
        arena AS arena_name
    FROM {{ ref('stg_games') }}
),

final AS (
    SELECT
        -- Surrogate Key for the fact table
        {{ dbt_utils.generate_surrogate_key(['base.game_id', 'base.player_id']) }} AS player_game_key,

        -- Foreign Keys from Dimensions
        p.player_key,
        t.team_key,
        d.date_key,
        s.season_key,
        a.arena_key,

        -- Degenerate Dimension
        base.game_id,

        -- Measures from Base Stats
        base.minutes_played,
        base.points,
        base.assists,
        base.total_rebounds,
        base.steals,
        base.blocks,
        base.turnovers,
        base.offensive_rebounds,
        base.defensive_rebounds,
        base.field_goals_made,
        base.field_goals_attempted,
        base.three_pointers_made,
        base.three_pointers_attempted,
        base.free_throws_made,
        base.free_throws_attempted,
        base.personal_fouls,
        base.plus_minus,

        -- Measures from Advanced Stats
        adv.true_shooting_pct,
        adv.effective_fg_pct,
        adv.three_point_attempt_rate,
        adv.free_throw_rate,
        adv.offensive_rebound_pct,
        adv.defensive_rebound_pct,
        adv.total_rebound_pct,
        adv.assist_pct,
        adv.steal_pct,
        adv.block_pct,
        adv.turnover_pct,
        adv.usage_pct,
        adv.offensive_rating,
        adv.defensive_rating,
        adv.box_plus_minus,

        -- Game Details for Partitioning
        CAST(gd.game_date AS DATE) AS game_date

    FROM base_player_stats AS base
    INNER JOIN adv_player_stats AS adv
        ON base.game_id = adv.game_id
        AND base.player_id = adv.player_id
    LEFT JOIN game_details AS gd
        ON base.game_id = gd.game_id
    LEFT JOIN {{ ref('dim_players') }} AS p
        ON base.player_id = p.player_id
    LEFT JOIN {{ ref('dim_teams') }} AS t
        ON base.team_abbreviation = t.team_abbr -- <<< THE FINAL, CORRECT JOIN CONDITION
    LEFT JOIN {{ ref('dim_dates') }} AS d
        ON CAST(gd.game_date AS DATE) = d.full_date
    LEFT JOIN {{ ref('dim_seasons') }} AS s
        ON gd.season_start_year = s.season_start_year
    LEFT JOIN {{ ref('dim_arenas') }} AS a
        ON gd.arena_name = a.arena_name
)

SELECT * FROM final
