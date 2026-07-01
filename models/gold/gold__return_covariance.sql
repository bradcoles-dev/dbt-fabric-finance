-- Pairwise return correlation and covariance matrix for MPT portfolio construction.
-- Lookback defaults to 252 trading days (1 year). Override with:
--   dbt run --vars "covariance_lookback_days: 756"   -- 3-year window

{% set lookback_days = var('covariance_lookback_days', 252) %}

with recent_returns as (

    select ticker, date, log_return
    from {{ ref('silver__daily_prices') }}
    where log_return is not null
      and date >= date_sub(current_date(), {{ lookback_days }})

),

pairs as (

    select
        a.ticker                                               as ticker_a,
        b.ticker                                               as ticker_b,
        round(corr(a.log_return, b.log_return), 6)            as correlation,
        round(covar_samp(a.log_return, b.log_return), 10)     as covariance_daily,
        round(covar_samp(a.log_return, b.log_return) * 252, 6) as covariance_annual,
        count(*)                                               as common_trading_days
    from recent_returns a
    join recent_returns b on a.date = b.date
    group by a.ticker, b.ticker

)

select
    p.ticker_a,
    p.ticker_b,
    p.correlation,
    p.covariance_daily,
    p.covariance_annual,
    p.common_trading_days,
    a.sector as sector_a,
    b.sector as sector_b
from pairs p
left join {{ ref('silver__company_info') }} a on p.ticker_a = a.ticker
left join {{ ref('silver__company_info') }} b on p.ticker_b = b.ticker
