{{
    config(
        materialized='table',
        schema='marts',
        tags=["dimension"]
    )
}}

{#
    Dimension table for shot distance zones.
    Provides a static reference for shot zone analysis and visualization.
#}

WITH zones AS (
    SELECT * FROM (
        VALUES
            ('At Rim (0-3 ft)',           0,  3,  1, 'Paint',     'Interior'),
            ('Short Range (4-10 ft)',     4,  10, 2, 'Paint',     'Interior'),
            ('Mid Range (11-16 ft)',      11, 16, 3, 'Mid Range', 'Mid Range'),
            ('Long Mid Range (17-23 ft)', 17, 23, 4, 'Mid Range', 'Mid Range'),
            ('Three Point (24-27 ft)',    24, 27, 5, 'Perimeter', 'Three Point'),
            ('Deep Three (28+ ft)',       28, 50, 6, 'Perimeter', 'Three Point')
    ) AS t(shot_distance_zone, min_distance_ft, max_distance_ft, zone_order, zone_group, zone_category)
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['shot_distance_zone']) }} AS shot_zone_key,
    shot_distance_zone,
    min_distance_ft,
    max_distance_ft,
    zone_order,
    zone_group,
    zone_category
FROM zones
ORDER BY zone_order