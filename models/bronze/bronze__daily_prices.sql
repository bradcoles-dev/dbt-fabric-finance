select
    ticker,
    date_,
    open_,
    high_,
    low_,
    close_,
    volume_,
    ingested_at,
    source
from {{ source('bronze', 'raw_daily_prices') }}
