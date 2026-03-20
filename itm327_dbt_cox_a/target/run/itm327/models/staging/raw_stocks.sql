
  create or replace   view SNOWBEARAIR_DB.RAW.raw_stocks
  
   as (
    select *
from SNOWBEARAIR_DB.RAW.STOCK_API_COX_A
  );

