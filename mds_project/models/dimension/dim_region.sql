{{ config(materialized='table') }}

SELECT
    event_id,
    CASE
        WHEN latitude BETWEEN 32 AND 42
         AND longitude BETWEEN -125 AND -114 THEN 'California'
        WHEN latitude BETWEEN 19 AND 23
         AND longitude BETWEEN -156 AND -154 THEN 'Hawaii'
        ELSE 'Other'
    END AS region
FROM {{ ref('stg_earthquakes_raw') }}
