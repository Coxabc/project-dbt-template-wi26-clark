import pandas as pd

def model(dbt, session):

    dbt.config(materialized="table")

    base_df = dbt.ref("Mart_stock_weather_daily").to_pandas()

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


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {"Mart_stock_weather_daily": "SNOWBEARAIR_DB.RAW.Mart_stock_weather_daily"}
    key = '.'.join(args)
    version = kwargs.get("v") or kwargs.get("version")
    if version:
        key += f".v{version}"
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])


config_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = "SNOWBEARAIR_DB"
    schema = "RAW"
    identifier = "ml_mart"
    
    def __repr__(self):
        return 'SNOWBEARAIR_DB.RAW.ml_mart'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------


