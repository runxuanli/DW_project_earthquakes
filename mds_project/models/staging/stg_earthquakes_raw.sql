{{ config(materialized='view') }}

SELECT
    event_id,
    time_utc,               -- already a TIMESTAMP_TZ, no conversion needed
    time_ms,
    latitude,
    longitude,
    depth_km,
    magnitude,
    mag_type,
    place,
    type,
    status,
    tsunami,
    alert,
    updated_ms,
    detail_url
FROM {{ source('earthquakes_source', 'EARTHQUAKES') }}

