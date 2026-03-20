{{ config(materialized='table') }}

WITH date_range AS (
    SELECT DATEADD(DAY, SEQ4(), '2025-01-01'::DATE) AS calendar_date
    FROM TABLE(GENERATOR(ROWCOUNT => 462))
)
SELECT
    ROW_NUMBER() OVER (ORDER BY calendar_date)                              AS date_sk,
    calendar_date,
    DAY(calendar_date)                                                      AS day_of_month,
    DAYOFWEEK(calendar_date)                                                AS day_of_week,
    DAYNAME(calendar_date)                                                  AS day_name,
    WEEKOFYEAR(calendar_date)                                               AS week_number,
    MONTH(calendar_date)                                                    AS month_number,
    MONTHNAME(calendar_date)                                                AS month_name,
    QUARTER(calendar_date)                                                  AS quarter,
    YEAR(calendar_date)                                                     AS year,
    MONTH(calendar_date)                                                    AS fiscal_month,
    QUARTER(calendar_date)                                                  AS fiscal_quarter,
    YEAR(calendar_date)                                                     AS fiscal_year,
    CASE WHEN DAYOFWEEK(calendar_date) IN (0, 6) THEN 1 ELSE 0 END        AS is_weekend,
    CASE WHEN DAYOFWEEK(calendar_date) IN (0, 6) THEN 0 ELSE 1 END        AS is_weekday,
    CASE WHEN DAY(calendar_date) = 1 THEN 1 ELSE 0 END                    AS month_start_flag,
    CASE WHEN calendar_date = LAST_DAY(calendar_date) THEN 1 ELSE 0 END   AS month_end_flag,
    CASE WHEN calendar_date = DATE_TRUNC('QUARTER', calendar_date)
         THEN 1 ELSE 0 END                                                  AS quarter_start_flag,
    CASE WHEN calendar_date = LAST_DAY(calendar_date, 'QUARTER')
         THEN 1 ELSE 0 END                                                  AS quarter_end_flag,
    CASE WHEN MONTH(calendar_date) = 1  AND DAY(calendar_date) = 1
         THEN 1 ELSE 0 END                                                  AS year_start_flag,
    CASE WHEN MONTH(calendar_date) = 12 AND DAY(calendar_date) = 31
         THEN 1 ELSE 0 END                                                  AS year_end_flag
FROM date_range
ORDER BY calendar_date