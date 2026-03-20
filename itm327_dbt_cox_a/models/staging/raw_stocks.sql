select *
from {{ source('sba', 'STOCK_API_COX_A') }}
