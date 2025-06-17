
-- Example SQL Queries for YFinance Database
-- These queries demonstrate how to analyze the loaded data

-- 1. Company Overview - Get basic information about all companies
SELECT 
    symbol,
    long_name,
    sector,
    industry,
    country,
    market_cap,
    trailing_pe,
    dividend_yield,
    last_close_price,
    last_price_date
FROM company_overview
ORDER BY market_cap DESC NULLS LAST;

-- 2. Price Performance - Last 30 days price change
SELECT 
    c.symbol,
    c.long_name,
    current_price.close_price as current_price,
    month_ago_price.close_price as price_30_days_ago,
    ROUND(((current_price.close_price - month_ago_price.close_price) / month_ago_price.close_price * 100)::numeric, 2) as change_percent_30d
FROM companies c
JOIN price_history current_price ON c.id = current_price.company_id
JOIN price_history month_ago_price ON c.id = month_ago_price.company_id
WHERE current_price.date = (
    SELECT MAX(date) FROM price_history WHERE company_id = c.id
)
AND month_ago_price.date = (
    SELECT MAX(date) 
    FROM price_history 
    WHERE company_id = c.id 
    AND date <= (SELECT MAX(date) FROM price_history WHERE company_id = c.id) - INTERVAL '30 days'
)
ORDER BY change_percent_30d DESC;

-- 3. Financial Health - Companies with strong financials
SELECT 
    c.symbol,
    c.long_name,
    i.total_revenue,
    i.net_income,
    ROUND((i.net_income::numeric / i.total_revenue * 100), 2) as profit_margin_percent,
    b.total_debt,
    b.total_assets,
    ROUND((b.total_debt::numeric / b.total_assets * 100), 2) as debt_to_assets_percent,
    cf.free_cash_flow,
    i.period_ending
FROM companies c
JOIN income_statements i ON c.id = i.company_id
JOIN balance_sheets b ON c.id = b.company_id AND b.period_ending = i.period_ending AND b.period_type = i.period_type
JOIN cash_flow_statements cf ON c.id = cf.company_id AND cf.period_ending = i.period_ending AND cf.period_type = i.period_type
WHERE i.period_type = 'annual'
AND i.period_ending = (
    SELECT MAX(period_ending) 
    FROM income_statements 
    WHERE company_id = c.id AND period_type = 'annual'
)
AND i.total_revenue > 0
AND i.net_income > 0
ORDER BY profit_margin_percent DESC;

-- 4. Dividend Analysis
SELECT 
    c.symbol,
    c.long_name,
    cm.dividend_yield,
    cm.dividend_rate,
    COUNT(ca.id) as dividend_payments_count,
    AVG(ca.amount) as avg_dividend_amount,
    MAX(ca.action_date) as last_dividend_date
FROM companies c
LEFT JOIN company_metrics cm ON c.id = cm.company_id
LEFT JOIN corporate_actions ca ON c.id = ca.company_id AND ca.action_type = 'dividend'
WHERE cm.dividend_yield > 0
GROUP BY c.id, c.symbol, c.long_name, cm.dividend_yield, cm.dividend_rate
ORDER BY cm.dividend_yield DESC;

-- 5. Valuation Metrics Comparison
SELECT 
    c.symbol,
    c.long_name,
    c.sector,
    cm.trailing_pe,
    cm.forward_pe,
    cm.price_to_book,
    cm.market_cap,
    -- Compare to sector averages
    AVG(cm2.trailing_pe) OVER (PARTITION BY c.sector) as sector_avg_pe,
    AVG(cm2.price_to_book) OVER (PARTITION BY c.sector) as sector_avg_pb
FROM companies c
JOIN company_metrics cm ON c.id = cm.company_id
JOIN company_metrics cm2 ON cm2.company_id IN (
    SELECT id FROM companies WHERE sector = c.sector
)
WHERE cm.trailing_pe > 0 AND cm.price_to_book > 0
ORDER BY c.sector, cm.trailing_pe;

-- 6. Revenue Growth Analysis
WITH revenue_growth AS (
    SELECT 
        c.symbol,
        c.long_name,
        i.period_ending,
        i.total_revenue,
        LAG(i.total_revenue) OVER (PARTITION BY c.id ORDER BY i.period_ending) as prev_revenue,
        CASE 
            WHEN LAG(i.total_revenue) OVER (PARTITION BY c.id ORDER BY i.period_ending) > 0 
            THEN ROUND(((i.total_revenue - LAG(i.total_revenue) OVER (PARTITION BY c.id ORDER BY i.period_ending))::numeric 
                       / LAG(i.total_revenue) OVER (PARTITION BY c.id ORDER BY i.period_ending) * 100), 2)
            ELSE NULL
        END as revenue_growth_percent
    FROM companies c
    JOIN income_statements i ON c.id = i.company_id
    WHERE i.period_type = 'annual'
    AND i.total_revenue > 0
)
SELECT *
FROM revenue_growth
WHERE revenue_growth_percent IS NOT NULL
ORDER BY symbol, period_ending DESC;

-- 7. Cash Flow Analysis
SELECT 
    c.symbol,
    c.long_name,
    cf.period_ending,
    cf.operating_cash_flow,
    cf.free_cash_flow,
    cf.capital_expenditure,
    i.net_income,
    CASE 
        WHEN i.net_income > 0 
        THEN ROUND((cf.operating_cash_flow::numeric / i.net_income), 2)
        ELSE NULL
    END as operating_cf_to_net_income_ratio
FROM companies c
JOIN cash_flow_statements cf ON c.id = cf.company_id
JOIN income_statements i ON c.id = i.company_id AND i.period_ending = cf.period_ending AND i.period_type = cf.period_type
WHERE cf.period_type = 'annual'
AND cf.period_ending = (
    SELECT MAX(period_ending) 
    FROM cash_flow_statements 
    WHERE company_id = c.id AND period_type = 'annual'
)
ORDER BY operating_cf_to_net_income_ratio DESC NULLS LAST;

-- 8. Price Volatility Analysis
WITH daily_returns AS (
    SELECT 
        company_id,
        date,
        close_price,
        LAG(close_price) OVER (PARTITION BY company_id ORDER BY date) as prev_close,
        CASE 
            WHEN LAG(close_price) OVER (PARTITION BY company_id ORDER BY date) > 0
            THEN (close_price - LAG(close_price) OVER (PARTITION BY company_id ORDER BY date)) 
                 / LAG(close_price) OVER (PARTITION BY company_id ORDER BY date)
            ELSE NULL
        END as daily_return
    FROM price_history
    WHERE date >= CURRENT_DATE - INTERVAL '1 year'
)
SELECT 
    c.symbol,
    c.long_name,
    COUNT(dr.daily_return) as trading_days,
    ROUND(AVG(dr.daily_return)::numeric * 100, 4) as avg_daily_return_percent,
    ROUND(STDDEV(dr.daily_return)::numeric * 100, 4) as volatility_percent,
    ROUND((AVG(dr.daily_return) / STDDEV(dr.daily_return))::numeric, 4) as sharpe_ratio_proxy
FROM companies c
JOIN daily_returns dr ON c.id = dr.company_id
WHERE dr.daily_return IS NOT NULL
GROUP BY c.id, c.symbol, c.long_name
HAVING COUNT(dr.daily_return) >= 100  -- At least 100 trading days of data
ORDER BY volatility_percent;

-- 9. Balance Sheet Strength
SELECT 
    c.symbol,
    c.long_name,
    b.period_ending,
    b.total_assets,
    b.total_liabilities,
    b.stockholders_equity,
    ROUND((b.stockholders_equity::numeric / b.total_assets * 100), 2) as equity_ratio_percent,
    ROUND((b.total_debt::numeric / b.stockholders_equity * 100), 2) as debt_to_equity_percent,
    b.cash_and_cash_equivalents,
    CASE 
        WHEN b.current_liabilities > 0 
        THEN ROUND((b.current_assets::numeric / b.current_liabilities), 2)
        ELSE NULL
    END as current_ratio
FROM companies c
JOIN balance_sheets b ON c.id = b.company_id
WHERE b.period_type = 'annual'
AND b.period_ending = (
    SELECT MAX(period_ending) 
    FROM balance_sheets 
    WHERE company_id = c.id AND period_type = 'annual'
)
AND b.total_assets > 0
ORDER BY equity_ratio_percent DESC;

-- 10. Data Quality Check
SELECT 
    'Companies' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT symbol) as unique_symbols,
    COUNT(*) - COUNT(long_name) as missing_names
FROM companies

UNION ALL

SELECT 
    'Price History' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT company_id) as unique_companies,
    COUNT(*) - COUNT(close_price) as missing_prices
FROM price_history

UNION ALL

SELECT 
    'Income Statements' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT company_id) as unique_companies,
    COUNT(*) - COUNT(total_revenue) as missing_revenue
FROM income_statements

UNION ALL

SELECT 
    'Balance Sheets' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT company_id) as unique_companies,
    COUNT(*) - COUNT(total_assets) as missing_assets
FROM balance_sheets

UNION ALL

SELECT 
    'Cash Flow Statements' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT company_id) as unique_companies,
    COUNT(*) - COUNT(operating_cash_flow) as missing_operating_cf
FROM cash_flow_statements;
