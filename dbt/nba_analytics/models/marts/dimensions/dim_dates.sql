{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- Get the min and max game dates from our source data
{% set start_date_query %}
select min(game_date)::date from {{ ref('stg_games') }}
{% endset %}
{% set start_date = dbt_utils.get_single_value(start_date_query) %}

{% set end_date_query %}
select max(game_date)::date from {{ ref('stg_games') }}
{% endset %}
{% set end_date = dbt_utils.get_single_value(end_date_query) %}

WITH date_spine AS (
    SELECT 
        generate_series::date as date_day
    FROM generate_series(
        '{{ start_date }}'::date,
        '{{ end_date }}'::date,
        '1 day'::interval
    )
)

SELECT
    -- Surrogate Key: An integer key in YYYYMMDD format
    CAST(to_char(date_day, 'YYYYMMDD') AS INTEGER) AS date_key,
    
    -- Date Attributes
    date_day AS full_date,
    EXTRACT(YEAR FROM date_day)::int AS year,
    EXTRACT(QUARTER FROM date_day)::int AS quarter_of_year,
    EXTRACT(MONTH FROM date_day)::int AS month_of_year,
    TRIM(to_char(date_day, 'Month')) AS month_name,
    EXTRACT(DAY FROM date_day)::int AS day_of_month,
    EXTRACT(ISODOW FROM date_day)::int AS day_of_week,
    TRIM(to_char(date_day, 'Day')) AS day_of_week_name,
    EXTRACT(DOY FROM date_day)::int AS day_of_year,
    EXTRACT(WEEK FROM date_day)::int AS week_of_year,
    CASE 
        WHEN EXTRACT(ISODOW FROM date_day) IN (6, 7) THEN true
        ELSE false 
    END AS is_weekend

FROM date_spine
ORDER BY full_date
