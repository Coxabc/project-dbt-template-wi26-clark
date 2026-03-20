{{ config(materialized='table') }}

SELECT
    d.date_sk,
    w.city,
    w.latitude,
    w.longitude
FROM {{ source('sba', 'WEATHER_COX_A') }} w
JOIN {{ ref('dim_dates_cox_a') }} d
    ON d.calendar_date = w."DATE"