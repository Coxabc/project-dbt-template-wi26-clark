select *
from {{ source('sba', 'NEWS_COX_A') }}
