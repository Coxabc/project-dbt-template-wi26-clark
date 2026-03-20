
  
    

        create or replace transient table SNOWBEARAIR_DB.RAW.dw
         as
        (CREATE TABLE IF NOT EXISTS DIM_NEWS_COX_A (
              dim_news_sk NUMBER AUTOINCREMENT PRIMARY KEY,
              category VARCHAR,
              date_sk NUMBER,
              headline VARCHAR,
              id INT,
              image VARCHAR,
              related VARCHAR,
              source VARCHAR,
              summary VARCHAR,
              url VARCHAR  
             );
             
CREATE IF NOT EXISTS TABLE FACT_NEWS_DAILY_COX_A (
    fact_news_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    date_sk NUMBER,
    dim_news_sk NUMBER,
    dim_stock_sk NUMBER
);

CREATE TABLE IF NOT EXISTS DIM_WEATHER_COX_A (
                dime_weather_sk NUMBER AUTOINCREMENT PRIMARY KEY,
                DATE_SK NUMBER,
                CITY VARCHAR,
                LATITUDE FLOAT,
                LONGITUDE FLOAT 
             );

CREATE TABLE IF NOT EXISTS DIM_STOCKS_COX_A (
    DIM_STOCKS_SK NUMBER,
    SYMBOL VARCHAR,
    DATE_SK NUMBER
);



CREATE TABLE IF NOT EXISTS DIM_DATES_COX_A (
    date_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    calendar_date DATE,
    day_of_month INT,
    day_of_week INT,
    day_name VARCHAR(45),
    week_number INT,
    month_number INT,
    month_name VARCHAR(45),
    quarter INT,
    year INT,
    fiscal_month INT,
    fiscal_quarter INT,
    fiscal_year INT,
    is_weekend TINYINT,
    is_weekday TINYINT,
    month_start_flag TINYINT,
    month_end_flag TINYINT,
    quarter_start_flag TINYINT,
    quarter_end_flag TINYINT,
    year_start_flag TINYINT,
    year_end_flag TINYINT
);


CREATE TABLE IF NOT EXISTS ML_ANALYSIS_FACT_TABLE_COX_A (
    ml_fact_table_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    dim_stock_sk NUMBER NOT NULL,
    dime_weather_sk NUMBER NOT NULL,
    date_sk NUMBER NOT NULL,
    OPEN FLOAT,
    HIGH FLOAT,
    LOW FLOAT,
    CLOSE FLOAT,
    VOLUME NUMBER,
    ny_max_temp FLOAT,
    ny_min_temp FLOAT,
    ny_percip FLOAT,
    ny_max_wind FLOAT,
    ny_radiation FLOAT,
    ny_evap FLOAT,
    tokyo_max_temp FLOAT,
    tokyo_min_temp FLOAT,
    tokyo_percip FLOAT,
    tokyo_max_wind FLOAT,
    tokyo_radiation FLOAT,
    tokyo_evap FLOAT,
    london_max_temp FLOAT,
    london_min_temp FLOAT,
    london_percip FLOAT,
    london_max_wind FLOAT,
    london_radiation FLOAT,
    london_evap FLOAT
    );



-- date insert
MERGE INTO DIM_DATES_COX_A AS target
USING (
    WITH date_range AS (
        SELECT DATEADD(DAY, SEQ4(), '2025-01-01'::DATE) AS calendar_date
        FROM TABLE(GENERATOR(ROWCOUNT => 462))
    )
    SELECT
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
        CASE WHEN DAYOFWEEK(calendar_date) IN (0,6) THEN 1 ELSE 0 END         AS is_weekend,
        CASE WHEN DAYOFWEEK(calendar_date) IN (0,6) THEN 0 ELSE 1 END         AS is_weekday,
        CASE WHEN DAY(calendar_date) = 1 THEN 1 ELSE 0 END                    AS month_start_flag,
        CASE WHEN calendar_date = LAST_DAY(calendar_date) THEN 1 ELSE 0 END   AS month_end_flag,
        CASE WHEN calendar_date = DATE_TRUNC('QUARTER', calendar_date) THEN 1 ELSE 0 END AS quarter_start_flag,
        CASE WHEN calendar_date = LAST_DAY(calendar_date, 'QUARTER') THEN 1 ELSE 0 END  AS quarter_end_flag,
        CASE WHEN MONTH(calendar_date) = 1  AND DAY(calendar_date) = 1  THEN 1 ELSE 0 END AS year_start_flag,
        CASE WHEN MONTH(calendar_date) = 12 AND DAY(calendar_date) = 31 THEN 1 ELSE 0 END AS year_end_flag
    FROM date_range
) AS source
    ON target.calendar_date = source.calendar_date
WHEN NOT MATCHED THEN INSERT (
    calendar_date, day_of_month, day_of_week, day_name, week_number,
    month_number, month_name, quarter, year,
    fiscal_month, fiscal_quarter, fiscal_year,
    is_weekend, is_weekday,
    month_start_flag, month_end_flag,
    quarter_start_flag, quarter_end_flag,
    year_start_flag, year_end_flag
) VALUES (
    source.calendar_date, source.day_of_month, source.day_of_week, source.day_name, source.week_number,
    source.month_number, source.month_name, source.quarter, source.year,
    source.fiscal_month, source.fiscal_quarter, source.fiscal_year,
    source.is_weekend, source.is_weekday,
    source.month_start_flag, source.month_end_flag,
    source.quarter_start_flag, source.quarter_end_flag,
    source.year_start_flag, source.year_end_flag
);

-- news insert
MERGE INTO DIM_NEWS_COX_A AS target
USING (
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
    FROM SNOWBEARAIR_DB.RAW.raw_news n
    JOIN DIM_DATES_COX_A d
        ON d.calendar_date = n.datetime::DATE
) AS source
    ON target.id = source.id
WHEN NOT MATCHED THEN INSERT (
    category, date_sk, headline, id, image, related, source, summary, url
) VALUES (
    source.category, source.date_sk, source.headline, source.id,
    source.image, source.related, source.source, source.summary, source.url
);

-- weather insert 
MERGE INTO DIM_WEATHER_COX_A AS target
USING (
    SELECT
        d.date_sk,
        w.city,
        w.latitude,
        w.longitude
    FROM SNOWBEARAIR_DB.RAW.raw_weather w
    JOIN DIM_DATES_COX_A d
        ON d.calendar_date = w."DATE"
) AS source
    ON target.date_sk = source.date_sk
   AND target.city    = source.city
WHEN NOT MATCHED THEN INSERT (
    date_sk, city, latitude, longitude
) VALUES (
    source.date_sk, source.city, source.latitude, source.longitude
);

-- stocks insert
MERGE INTO DIM_STOCKS_COX_A AS target
USING (
    SELECT
        s.id AS dim_stocks_sk,
        s.symbol,
        d.date_sk
    FROM SNOWBEARAIR_DB.RAW.raw_stocks s
    JOIN DIM_DATES_COX_A d
        ON d.calendar_date = s.datetime::DATE
) AS source
    ON target.dim_stocks_sk = source.dim_stocks_sk
WHEN NOT MATCHED THEN INSERT (
    dim_stocks_sk, symbol, date_sk
) VALUES (
    source.dim_stocks_sk, source.symbol, source.date_sk
);

-- fact news insert 
MERGE INTO FACT_NEWS_DAILY_COX_A AS target
USING (
    SELECT
        n.date_sk,
        n.dim_news_sk,
        s.dim_stocks_sk
    FROM DIM_NEWS_COX_A n
    JOIN DIM_STOCKS_COX_A s
        ON n.date_sk = s.date_sk
       AND n.related = s.symbol
) AS source
    ON target.dim_news_sk  = source.dim_news_sk
   AND target.dim_stock_sk = source.dim_stocks_sk
WHEN NOT MATCHED THEN INSERT (
    date_sk, dim_news_sk, dim_stock_sk
) VALUES (
    source.date_sk, source.dim_news_sk, source.dim_stocks_sk
);

-- ml analysis inert
MERGE INTO ML_ANALYSIS_FACT_TABLE_COX_A AS target
USING (
    SELECT
        s.dim_stocks_sk,
        dw.dime_weather_sk,
        d.date_sk,
        r.open, r.high, r.low, r.close, r.volume,
        ny.max_temp     AS ny_max_temp,     ny.min_temp  AS ny_min_temp,
        ny.percip       AS ny_percip,       ny.max_wind  AS ny_max_wind,
        ny.radiation    AS ny_radiation,    ny.evap      AS ny_evap,
        tok.max_temp    AS tokyo_max_temp,  tok.min_temp AS tokyo_min_temp,
        tok.percip      AS tokyo_percip,    tok.max_wind AS tokyo_max_wind,
        tok.radiation   AS tokyo_radiation, tok.evap     AS tokyo_evap,
        lon.max_temp    AS london_max_temp, lon.min_temp AS london_min_temp,
        lon.percip      AS london_percip,   lon.max_wind AS london_max_wind,
        lon.radiation   AS london_radiation,lon.evap     AS london_evap
    FROM DIM_STOCKS_COX_A s
    JOIN DIM_DATES_COX_A d
        ON d.date_sk = s.date_sk
    JOIN SNOWBEARAIR_DB.RAW.raw_stocks r
        ON r.id = s.dim_stocks_sk
    JOIN DIM_WEATHER_COX_A dw
        ON dw.date_sk = s.date_sk
    JOIN SNOWBEARAIR_DB.RAW.raw_weather ny
        ON ny."DATE" = d.calendar_date AND ny.city = 'New York'
    JOIN SNOWBEARAIR_DB.RAW.raw_weather tok
        ON tok."DATE" = d.calendar_date AND tok.city = 'Tokyo'
    JOIN SNOWBEARAIR_DB.RAW.raw_weather lon
        ON lon."DATE" = d.calendar_date AND lon.city = 'London'
) AS source
    ON target.dim_stock_sk = source.dim_stocks_sk
   AND target.date_sk      = source.date_sk
WHEN NOT MATCHED THEN INSERT (
    dim_stock_sk, dime_weather_sk, date_sk,
    open, high, low, close, volume,
    ny_max_temp, ny_min_temp, ny_percip, ny_max_wind, ny_radiation, ny_evap,
    tokyo_max_temp, tokyo_min_temp, tokyo_percip, tokyo_max_wind, tokyo_radiation, tokyo_evap,
    london_max_temp, london_min_temp, london_percip, london_max_wind, london_radiation, london_evap
) VALUES (
    source.dim_stocks_sk, source.dime_weather_sk, source.date_sk,
    source.open, source.high, source.low, source.close, source.volume,
    source.ny_max_temp, source.ny_min_temp, source.ny_percip, source.ny_max_wind, source.ny_radiation, source.ny_evap,
    source.tokyo_max_temp, source.tokyo_min_temp, source.tokyo_percip, source.tokyo_max_wind, source.tokyo_radiation, source.tokyo_evap,
    source.london_max_temp, source.london_min_temp, source.london_percip, source.london_max_wind, source.london_radiation, source.london_evap
);
        );
      
  