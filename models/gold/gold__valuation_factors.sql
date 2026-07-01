with latest_financials as (

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
    ci.ticker,
    ci.company_name,
    ci.sector,
    ci.industry,
    ci.country,
    ci.currency,
    ci.market_cap,

    -- valuation multiples (from Yahoo Finance snapshot)
    ci.beta,
    ci.pe_ratio_ttm,
    ci.pe_ratio_forward,
    ci.price_to_book,
    ci.dividend_yield,
    ci.eps_ttm,

    -- profitability ratios
    ci.return_on_equity,
    ci.return_on_assets,
    ci.operating_margin,
    ci.profit_margin,
    ci.gross_margin,

    -- growth
    ci.revenue_growth_yoy,
    ci.earnings_growth_yoy,

    -- leverage / liquidity
    ci.debt_to_equity,
    ci.current_ratio,

    -- latest annual financials
    lf.fiscal_year             as latest_fiscal_year,
    lf.period_end              as latest_period_end,
    lf.revenue,
    lf.gross_profit,
    lf.ebitda,
    lf.net_income,
    lf.eps_diluted,
    lf.total_assets,
    lf.equity                  as book_equity,
    lf.total_debt,
    lf.cash,
    lf.net_debt,
    lf.free_cash_flow,

    -- derived: enterprise value (market cap + net debt)
    case
        when ci.market_cap is not null and lf.net_debt is not null
        then ci.market_cap + lf.net_debt
    end                        as enterprise_value,

    -- derived: EV / EBITDA
    case
        when ci.market_cap is not null
         and lf.net_debt    is not null
         and lf.ebitda      is not null
         and lf.ebitda > 0
        then round((ci.market_cap + lf.net_debt) / lf.ebitda, 2)
    end                        as ev_ebitda

from {{ ref('silver__company_info') }} ci
left join latest_financials lf on ci.ticker = lf.ticker
