-- Pivots the EAV bronze table into a wide table: one row per (ticker, fiscal year).
-- Metric names are as returned by yfinance. Coverage varies by ticker and exchange;
-- ASX tickers tend to have fewer fields populated than US ones.

with pivoted as (

    select
        ticker,
        period_end,

        -- income statement
        max(case when statement_type = 'income_statement' and metric_name = 'Total Revenue'    then value end) as revenue,
        max(case when statement_type = 'income_statement' and metric_name = 'Gross Profit'     then value end) as gross_profit,
        max(case when statement_type = 'income_statement' and metric_name = 'Operating Income' then value end) as operating_income,
        max(case when statement_type = 'income_statement' and metric_name = 'EBIT'             then value end) as ebit,
        max(case when statement_type = 'income_statement' and metric_name = 'EBITDA'           then value end) as ebitda,
        max(case when statement_type = 'income_statement' and metric_name = 'Net Income'       then value end) as net_income,
        max(case when statement_type = 'income_statement' and metric_name = 'Basic EPS'        then value end) as eps_basic,
        max(case when statement_type = 'income_statement' and metric_name = 'Diluted EPS'      then value end) as eps_diluted,

        -- balance sheet
        max(case when statement_type = 'balance_sheet'    and metric_name = 'Total Assets'               then value end) as total_assets,
        max(case when statement_type = 'balance_sheet'    and metric_name = 'Stockholders Equity'        then value end) as equity,
        max(case when statement_type = 'balance_sheet'    and metric_name = 'Total Debt'                 then value end) as total_debt,
        max(case when statement_type = 'balance_sheet'    and metric_name = 'Cash And Cash Equivalents'  then value end) as cash,
        max(case when statement_type = 'balance_sheet'    and metric_name = 'Current Assets'             then value end) as current_assets,
        max(case when statement_type = 'balance_sheet'    and metric_name = 'Current Liabilities'        then value end) as current_liabilities,

        -- cash flow
        max(case when statement_type = 'cash_flow'        and metric_name = 'Operating Cash Flow'  then value end) as operating_cash_flow,
        max(case when statement_type = 'cash_flow'        and metric_name = 'Capital Expenditure'  then value end) as capex,
        max(case when statement_type = 'cash_flow'        and metric_name = 'Free Cash Flow'       then value end) as free_cash_flow,

        max(ingested_at) as ingested_at,
        max(source)      as source

    from {{ ref('bronze__financials') }}
    group by ticker, period_end

)

select
    ticker,
    period_end,
    year(period_end)                                         as fiscal_year,

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
    equity,
    total_debt,
    cash,
    current_assets,
    current_liabilities,

    -- cash flow
    operating_cash_flow,
    capex,
    free_cash_flow,

    -- derived
    case
        when total_debt is not null and cash is not null then total_debt - cash
    end as net_debt,

    ingested_at,
    source
from pivoted
