
  create or replace   view SNOWBEARAIR_DB.RAW.raw_weather
  
   as (
    select *
from SNOWBEARAIR_DB.RAW.WEATHER_COX_A
  );

