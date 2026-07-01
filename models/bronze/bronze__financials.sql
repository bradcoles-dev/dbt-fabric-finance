select
    ticker,
    period_end,
    statement_type,
    metric_name,
    value,
    ingested_at,
    source
from {{ source('bronze', 'raw_financials') }}
