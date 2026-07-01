with prices as (

    select ticker, date, close
    from {{ ref('silver__daily_prices') }}
    where close is not null

),

with_peak as (

    select
        ticker,
        date,
        close,
        max(close) over (
            partition by ticker
            order by date
            rows between unbounded preceding and current row
        ) as running_peak
    from prices

),

drawdown_agg as (

    select
        ticker,
        min((close - running_peak) / running_peak) as max_drawdown
    from with_peak
    where running_peak > 0
    group by ticker

),

returns_agg as (

    select
        ticker,
        count(*)                        as trading_days,
        min(date)                       as date_from,
        max(date)                       as date_to,
        avg(log_return) * 252           as annualised_return,
        stddev(log_return) * sqrt(252)  as annualised_volatility
    from {{ ref('silver__daily_prices') }}
    where log_return is not null
    group by ticker

)

select
    r.ticker,
    r.trading_days,
    r.date_from,
    r.date_to,
    round(r.annualised_return, 4)                                      as annualised_return,
    round(r.annualised_volatility, 4)                                  as annualised_volatility,
    case
        when r.annualised_volatility is null or r.annualised_volatility = 0 then null
        else round(r.annualised_return / r.annualised_volatility, 4)
    end                                                                as sharpe_ratio,
    round(d.max_drawdown, 4)                                           as max_drawdown,
    c.beta,
    c.sector,
    c.market_cap
from returns_agg r
left join drawdown_agg d          on r.ticker = d.ticker
left join {{ ref('silver__company_info') }} c on r.ticker = c.ticker
