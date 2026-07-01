select
    ticker,
    fiscal_year,
    period_end,

    -- income statement
    revenue,
    gross_profit,
    operating_income,
    ebit,
    ebitda,
    net_income,
    eps_basic,
    eps_diluted,

    -- balance sheet
    total_assets,
    equity              as book_equity,
    total_debt,
    cash,
    current_assets,
    current_liabilities,

    -- cash flow
    operating_cash_flow,
    capex,
    free_cash_flow,

    -- derived
    net_debt

from {{ ref('silver__financials') }}
where ticker is not null
