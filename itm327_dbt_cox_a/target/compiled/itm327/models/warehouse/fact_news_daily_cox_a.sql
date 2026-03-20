

SELECT
    n.date_sk,
    n.id         AS dim_news_sk,
    s.dim_stocks_sk
FROM SNOWBEARAIR_DB.RAW.dim_news_cox_a n
JOIN SNOWBEARAIR_DB.RAW.dim_stocks_cox_a s
    ON  n.date_sk = s.date_sk
    AND n.related = s.symbol