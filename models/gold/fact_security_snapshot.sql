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

),

latest_financials as (

    select *
    from (
        select
            *,
            row_number() over (partition by ticker order by period_end desc) as rn
        from {{ ref('silver__financials') }}
    )
    where rn = 1

)

select
    r.ticker,

    -- price history window
    r.trading_days,
    r.date_from,
    r.date_to,

    -- risk metrics (derived from full price history)
    round(r.annualised_return, 4)                                          as annualised_return,
    round(r.annualised_volatility, 4)                                      as annualised_volatility,
    case
        when r.annualised_volatility is null or r.annualised_volatility = 0 then null
        else round(r.annualised_return / r.annualised_volatility, 4)
    end                                                                    as sharpe_ratio,
    round(d.max_drawdown, 4)                                               as max_drawdown,

    -- valuation snapshot (Yahoo Finance point-in-time)
    c.market_cap,
    c.beta,
    c.pe_ratio_ttm,
    c.pe_ratio_forward,
    c.price_to_book,
    c.dividend_yield,
    c.eps_ttm,
    c.return_on_equity,
    c.return_on_assets,
    c.operating_margin,
    c.profit_margin,
    c.gross_margin,
    c.revenue_growth_yoy,
    c.earnings_growth_yoy,
    c.debt_to_equity,
    c.current_ratio,

    -- enterprise value: current market_cap + latest annual net_debt
    case
        when c.market_cap is not null and lf.net_debt is not null
        then c.market_cap + lf.net_debt
    end                                                                    as enterprise_value,

    case
        when c.market_cap is not null
         and lf.net_debt is not null
         and lf.ebitda   is not null
         and lf.ebitda   > 0
        then round((c.market_cap + lf.net_debt) / lf.ebitda, 2)
    end                                                                    as ev_ebitda

from returns_agg r
left join drawdown_agg                      d  on r.ticker = d.ticker
left join {{ ref('silver__company_info') }} c  on r.ticker = c.ticker
left join latest_financials                 lf on r.ticker = lf.ticker
