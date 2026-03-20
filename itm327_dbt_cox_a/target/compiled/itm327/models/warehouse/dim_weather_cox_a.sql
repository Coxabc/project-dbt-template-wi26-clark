

SELECT
    d.date_sk,
    w.city,
    w.latitude,
    w.longitude
FROM SNOWBEARAIR_DB.RAW.WEATHER_COX_A w
JOIN SNOWBEARAIR_DB.RAW.dim_dates_cox_a d
    ON d.calendar_date = w."DATE"