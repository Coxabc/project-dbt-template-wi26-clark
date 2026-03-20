select *
from {{ source('sba', 'WEATHER_COX_A') }}
