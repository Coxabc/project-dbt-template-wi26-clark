

WITH stock_weather AS (
    SELECT
        d.calendar_date,
        d.day_name,
        d.week_number,
        d.month_name,
        d.year,
        s.symbol,

        -- Stock prices
        ml.open,
        ml.high,
        ml.low,
        ml.close,
        ml.volume,

        -- Daily price change
        ROUND(ml.close - ml.open, 4)                                         AS price_change,
        ROUND(((ml.close - ml.open) / NULLIF(ml.open, 0)) * 100, 2)         AS price_change_pct,

        -- Price volatility
        ROUND(ml.high - ml.low, 4)                                           AS daily_volatility,

        -- New York weather
        ml.ny_max_temp,
        ml.ny_min_temp,
        ml.ny_percip,
        ml.ny_max_wind,

        -- Tokyo weather
        ml.tokyo_max_temp,
        ml.tokyo_min_temp,
        ml.tokyo_percip,
        ml.tokyo_max_wind,

        -- London weather
        ml.london_max_temp,
        ml.london_min_temp,
        ml.london_percip,
        ml.london_max_wind

    FROM SNOWBEARAIR_DB.RAW.ml_analysis_fact_cox_a ml
    JOIN SNOWBEARAIR_DB.RAW.dim_stocks_cox_a s
        ON s.dim_stocks_sk = ml.dim_stocks_sk
    JOIN SNOWBEARAIR_DB.RAW.dim_dates_cox_a d
        ON d.date_sk = ml.date_sk
),

news_with_sentiment AS (
    SELECT
        d.calendar_date,
        s.symbol,
        n.headline,
        n.summary,
        n.category,

        -- Snowflake Cortex AI: sentiment score on headline (-1 negative → 1 positive)
        SNOWFLAKE.CORTEX.SENTIMENT(n.headline)                               AS headline_sentiment,

        -- Snowflake Cortex AI: sentiment score on full summary
        SNOWFLAKE.CORTEX.SENTIMENT(n.summary)                                AS summary_sentiment

    FROM SNOWBEARAIR_DB.RAW.fact_news_daily_cox_a fn
    JOIN SNOWBEARAIR_DB.RAW.dim_news_cox_a n
        ON n.id = fn.dim_news_sk
    JOIN SNOWBEARAIR_DB.RAW.dim_stocks_cox_a s
        ON s.dim_stocks_sk = fn.dim_stocks_sk
    JOIN SNOWBEARAIR_DB.RAW.dim_dates_cox_a d
        ON d.date_sk = fn.date_sk
),

news_counts AS (
    SELECT
        calendar_date,
        symbol,
        COUNT(*)                                                             AS news_count,
        COUNT(CASE WHEN category = 'business' THEN 1 END)                   AS business_news_count,
        COUNT(CASE WHEN category = 'technology' THEN 1 END)                 AS tech_news_count,

        -- Cortex AI: average sentiment across all headlines for that stock that day
        ROUND(AVG(headline_sentiment), 4)                                    AS avg_headline_sentiment,

        -- Cortex AI: average sentiment across all summaries
        ROUND(AVG(summary_sentiment), 4)                                     AS avg_summary_sentiment,

        -- Cortex AI: most positive and most negative headline of the day
        MAX(headline_sentiment)                                              AS max_headline_sentiment,
        MIN(headline_sentiment)                                              AS min_headline_sentiment,

        -- Cortex AI: sentiment signal (bullish / bearish / neutral)
        CASE
            WHEN AVG(headline_sentiment) >= 0.2  THEN 'bullish'
            WHEN AVG(headline_sentiment) <= -0.2 THEN 'bearish'
            ELSE 'neutral'
        END                                                                  AS sentiment_signal

    FROM news_with_sentiment
    GROUP BY calendar_date, symbol
)

SELECT
    sw.*,
    COALESCE(nc.news_count, 0)               AS total_news_count,
    COALESCE(nc.business_news_count, 0)      AS business_news_count,
    COALESCE(nc.tech_news_count, 0)          AS tech_news_count,
    COALESCE(nc.avg_headline_sentiment, 0)   AS avg_headline_sentiment,
    COALESCE(nc.avg_summary_sentiment, 0)    AS avg_summary_sentiment,
    COALESCE(nc.max_headline_sentiment, 0)   AS max_headline_sentiment,
    COALESCE(nc.min_headline_sentiment, 0)   AS min_headline_sentiment,
    COALESCE(nc.sentiment_signal, 'neutral') AS sentiment_signal
FROM stock_weather sw
LEFT JOIN news_counts nc
    ON nc.calendar_date = sw.calendar_date
    AND nc.symbol       = sw.symbol
ORDER BY sw.calendar_date, sw.symbol