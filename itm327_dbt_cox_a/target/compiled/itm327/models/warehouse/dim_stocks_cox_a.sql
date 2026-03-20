

SELECT
    s.id       AS dim_stocks_sk,
    s.symbol,
    d.date_sk
FROM SNOWBEARAIR_DB.RAW.STOCK_API_COX_A s
JOIN SNOWBEARAIR_DB.RAW.dim_dates_cox_a d
    ON d.calendar_date = s.datetime::DATE