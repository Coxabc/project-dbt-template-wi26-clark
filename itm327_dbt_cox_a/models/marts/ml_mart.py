import pandas as pd

def model(dbt, session):

    dbt.config(materialized="table")

    base_df = dbt.ref("mart_stock_weather_daily_cox_a").to_pandas()

    base_df = base_df.sort_values(["SYMBOL", "CALENDAR_DATE"]).reset_index(drop=True)

    base_df["ROLLING_7D_AVG_CLOSE"] = (
        base_df.groupby("SYMBOL")["CLOSE"]
        .transform(lambda x: x.rolling(window=7, min_periods=1).mean())
        .round(4)
    )

    base_df["ROLLING_7D_AVG_VOLUME"] = (
        base_df.groupby("SYMBOL")["VOLUME"]
        .transform(lambda x: x.rolling(window=7, min_periods=1).mean())
        .round(0)
    )

    base_df["ROLLING_7D_AVG_VOLATILITY"] = (
        base_df.groupby("SYMBOL")["DAILY_VOLATILITY"]
        .transform(lambda x: x.rolling(window=7, min_periods=1).mean())
        .round(4)
    )

    base_df["PREV_DAY_CLOSE"] = (
        base_df.groupby("SYMBOL")["CLOSE"]
        .shift(1)
    )

    base_df["PREV_DAY_VOLUME"] = (
        base_df.groupby("SYMBOL")["VOLUME"]
        .shift(1)
    )

    base_df["PREV_DAY_PRICE_CHANGE_PCT"] = (
        base_df.groupby("SYMBOL")["PRICE_CHANGE_PCT"]
        .shift(1)
    )

    base_df["PRICE_MOMENTUM"] = (
        (base_df["CLOSE"] - base_df["ROLLING_7D_AVG_CLOSE"])
        / base_df["ROLLING_7D_AVG_CLOSE"] * 100
    ).round(4)

    base_df["AVG_GLOBAL_MAX_TEMP"] = (
        (base_df["NY_MAX_TEMP"] + base_df["TOKYO_MAX_TEMP"] + base_df["LONDON_MAX_TEMP"]) / 3
    ).round(2)

    base_df["AVG_GLOBAL_PERCIP"] = (
        (base_df["NY_PERCIP"] + base_df["TOKYO_PERCIP"] + base_df["LONDON_PERCIP"]) / 3
    ).round(4)

    base_df["ROLLING_7D_NEWS_COUNT"] = (
        base_df.groupby("SYMBOL")["TOTAL_NEWS_COUNT"]
        .transform(lambda x: x.rolling(window=7, min_periods=1).sum())
        .round(0)
    )

    base_df["NEXT_DAY_CLOSE"] = (
        base_df.groupby("SYMBOL")["CLOSE"]
        .shift(-1)
    )

    base_df["TARGET_PRICE_UP"] = (
        (base_df["NEXT_DAY_CLOSE"] > base_df["CLOSE"]).astype(int)
    )

    base_df = base_df.drop(columns=["NEXT_DAY_CLOSE"])

    return base_df