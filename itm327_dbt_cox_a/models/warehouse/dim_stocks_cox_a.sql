{{ config(materialized='table') }}

SELECT
    s.id       AS dim_stocks_sk,
    s.symbol,
    d.date_sk
FROM {{ source('sba', 'STOCK_API_COX_A') }} s
JOIN {{ ref('dim_dates_cox_a') }} d
    ON d.calendar_date = s.datetime::DATE