with base as (

    select
        ticker,
        date_   as date,
        open_   as open,
        high_   as high,
        low_    as low,
        close_  as close,
        volume_ as volume,
        ingested_at,
        source
    from {{ ref('bronze__daily_prices') }}

),

with_prev as (

    select
        *,
        lag(close) over (partition by ticker order by date) as prev_close
    from base

)

select
    ticker,
    date,
    open,
    high,
    low,
    close,
    volume,
    prev_close,
    case
        when prev_close is null or prev_close = 0 then null
        else round((close - prev_close) / prev_close, 6)
    end as daily_return,
    case
        when prev_close is null or prev_close <= 0 then null
        else round(log(close / prev_close), 6)
    end as log_return,
    ingested_at,
    source
from with_prev
