{{ config(materialized='table') }}

SELECT
    event_id,
    time_utc,
    DATE(time_utc) AS event_date,
    magnitude,
    depth_km,
    latitude,
    longitude,
    place,
    mag_type,
    tsunami,
    alert,
    detail_url,               -- ✅ FIXED: include detail_url

    CASE
        WHEN magnitude < 2 THEN 'micro'
        WHEN magnitude < 4 THEN 'minor'
        WHEN magnitude < 6 THEN 'moderate'
        ELSE 'major'
    END AS magnitude_bin,

    CASE
        WHEN depth_km < 70 THEN 'shallow'
        WHEN depth_km < 300 THEN 'intermediate'
        ELSE 'deep'
    END AS depth_bin

FROM {{ ref('stg_earthquakes_raw') }}
WHERE magnitude IS NOT NULL

