

SELECT
    n.category,
    d.date_sk,
    n.headline,
    n.id,
    n.image,
    n.related,
    n.source,
    n.summary,
    n.url
FROM SNOWBEARAIR_DB.RAW.NEWS_COX_A n
JOIN SNOWBEARAIR_DB.RAW.dim_dates_cox_a d
    ON d.calendar_date = n.datetime::DATE