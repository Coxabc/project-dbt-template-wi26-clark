{{ config(materialized='table') }}

SELECT
    s.dim_stocks_sk,
    dw.date_sk      AS dime_weather_sk,
    d.date_sk,
    r.open,
    r.high,
    r.low,
    r.close,
    r.volume,
    ny.max_temp     AS ny_max_temp,
    ny.min_temp     AS ny_min_temp,
    ny.percip       AS ny_percip,
    ny.max_wind     AS ny_max_wind,
    ny.radiation    AS ny_radiation,
    ny.evap         AS ny_evap,
    tok.max_temp    AS tokyo_max_temp,
    tok.min_temp    AS tokyo_min_temp,
    tok.percip      AS tokyo_percip,
    tok.max_wind    AS tokyo_max_wind,
    tok.radiation   AS tokyo_radiation,
    tok.evap        AS tokyo_evap,
    lon.max_temp    AS london_max_temp,
    lon.min_temp    AS london_min_temp,
    lon.percip      AS london_percip,
    lon.max_wind    AS london_max_wind,
    lon.radiation   AS london_radiation,
    lon.evap        AS london_evap
FROM {{ ref('dim_stocks_cox_a') }} s
JOIN {{ ref('dim_dates_cox_a') }} d
    ON d.date_sk = s.date_sk
JOIN {{ source('sba', 'STOCK_API_COX_A') }} r
    ON r.id = s.dim_stocks_sk
JOIN {{ ref('dim_weather_cox_a') }} dw
    ON dw.date_sk = s.date_sk
JOIN {{ source('sba', 'WEATHER_COX_A') }} ny
    ON ny."DATE" = d.calendar_date
    AND ny.city  = 'New York'
JOIN {{ source('sba', 'WEATHER_COX_A') }} tok
    ON tok."DATE" = d.calendar_date
    AND tok.city  = 'Tokyo'
JOIN {{ source('sba', 'WEATHER_COX_A') }} lon
    ON lon."DATE" = d.calendar_date
    AND lon.city  = 'London'