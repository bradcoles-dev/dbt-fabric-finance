with company as (

    select *
    from {{ ref('silver__company_info') }}
    where ticker is not null

),

sp100 as (

    select ticker, exchange, 'S&P 100' as market_index
    from {{ ref('sp100_tickers') }}

),

asx200 as (

    select ticker, exchange, 'ASX 200' as market_index
    from {{ ref('asx200_tickers') }}

),

index_map as (

    select ticker, exchange, market_index from sp100
    union all
    select ticker, exchange, market_index from asx200

)

select
    c.ticker,
    c.company_name,
    c.sector,
    c.industry,
    c.country,
    c.currency,
    i.exchange,
    i.market_index
from company c
left join index_map i on c.ticker = i.ticker
