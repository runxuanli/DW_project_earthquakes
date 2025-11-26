{{ config(materialized='table') }}

SELECT
    event_id,
    time_utc,
    DATE(time_utc) AS event_date,
    latitude,
    longitude,
    magnitude,
    depth_km,
    place,
    mag_type,
    tsunami,
    alert,
    detail_url
FROM {{ ref('fct_quakes') }}

WHERE magnitude IS NOT NULL
