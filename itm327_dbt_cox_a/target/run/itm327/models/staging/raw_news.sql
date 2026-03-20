
  create or replace   view SNOWBEARAIR_DB.RAW.raw_news
  
   as (
    select *
from SNOWBEARAIR_DB.RAW.NEWS_COX_A
  );

