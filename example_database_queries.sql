
-- Example Queries for YFinance Database Analysis

-- 1. Get latest stock prices with key metrics
SELECT 
    c.symbol,
    c.long_name,
    c.sector,
    ph.date as latest_date,
    ph.close_price,
    ph.volume,
    cm.market_cap,
    cm.trailing_pe,
    cm.dividend_yield,
    cm.beta
FROM companies c
JOIN price_history ph ON c.id = ph.company_id
JOIN company_metrics cm ON c.id = cm.company_id
WHERE ph.date = (
    SELECT MAX(date) FROM price_history ph2 WHERE ph2.company_id = ph.company_id
)
ORDER BY cm.market_cap DESC;

-- 2. Revenue growth analysis (Year-over-Year)
WITH revenue_growth AS (
    SELECT 
        c.symbol,
        c.long_name,
        i.period_ending,
        i.total_revenue,
        LAG(i.total_revenue) OVER (PARTITION BY c.id ORDER BY i.period_ending) as prev_year_revenue,
        ROUND(
            ((i.total_revenue - LAG(i.total_revenue) OVER (PARTITION BY c.id ORDER BY i.period_ending)) * 100.0 / 
             LAG(i.total_revenue) OVER (PARTITION BY c.id ORDER BY i.period_ending)), 2
        ) as revenue_growth_percent
    FROM companies c
    JOIN income_statements i ON c.id = i.company_id
    WHERE i.period_type = 'annual'
)
SELECT * FROM revenue_growth 
WHERE revenue_growth_percent IS NOT NULL
ORDER BY period_ending DESC, revenue_growth_percent DESC;

-- 3. Find undervalued stocks (low P/E ratio)
SELECT 
    c.symbol,
    c.long_name,
    c.sector,
    cm.trailing_pe,
    cm.price_to_book,
    cm.dividend_yield,
    cm.market_cap
FROM companies c
JOIN company_metrics cm ON c.id = cm.company_id
WHERE cm.trailing_pe IS NOT NULL 
  AND cm.trailing_pe > 0 
  AND cm.trailing_pe < 15
ORDER BY cm.trailing_pe ASC;

-- 4. Dividend analysis
SELECT 
    c.symbol,
    c.long_name,
    COUNT(ca.id) as dividend_payments_count,
    SUM(ca.amount) as total_dividends,
    AVG(ca.amount) as avg_dividend,
    MAX(ca.action_date) as last_dividend_date,
    cm.dividend_yield
FROM companies c
JOIN corporate_actions ca ON c.id = ca.company_id
JOIN company_metrics cm ON c.id = cm.company_id
WHERE ca.action_type = 'dividend'
GROUP BY c.id, c.symbol, c.long_name, cm.dividend_yield
ORDER BY total_dividends DESC;

-- 5. Stock price performance over time
SELECT 
    c.symbol,
    EXTRACT(YEAR FROM ph.date) as year,
    MIN(ph.low_price) as year_low,
    MAX(ph.high_price) as year_high,
    FIRST_VALUE(ph.close_price) OVER (
        PARTITION BY c.symbol, EXTRACT(YEAR FROM ph.date) 
        ORDER BY ph.date ASC 
        ROWS UNBOUNDED PRECEDING
    ) as year_open,
    LAST_VALUE(ph.close_price) OVER (
        PARTITION BY c.symbol, EXTRACT(YEAR FROM ph.date) 
        ORDER BY ph.date ASC 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as year_close,
    SUM(ph.volume) as total_volume
FROM companies c
JOIN price_history ph ON c.id = ph.company_id
WHERE ph.date >= '2020-01-01'
GROUP BY c.symbol, EXTRACT(YEAR FROM ph.date), ph.date, ph.close_price
ORDER BY c.symbol, year;

-- 6. Financial health indicators
SELECT 
    c.symbol,
    c.long_name,
    b.period_ending,
    ROUND(b.current_assets::numeric / NULLIF(b.current_liabilities, 0), 2) as current_ratio,
    ROUND(b.total_debt::numeric / NULLIF(b.stockholders_equity, 0), 2) as debt_to_equity,
    ROUND(cf.free_cash_flow::numeric / 1000000, 2) as free_cashflow_millions,
    ROUND(i.net_income::numeric / NULLIF(b.total_assets, 0) * 100, 2) as roa_percent
FROM companies c
JOIN balance_sheets b ON c.id = b.company_id AND b.period_type = 'annual'
JOIN cash_flow_statements cf ON c.id = cf.company_id AND cf.period_ending = b.period_ending AND cf.period_type = 'annual'
JOIN income_statements i ON c.id = i.company_id AND i.period_ending = b.period_ending AND i.period_type = 'annual'
WHERE b.period_ending = (
    SELECT MAX(period_ending) FROM balance_sheets b2 
    WHERE b2.company_id = b.company_id AND b2.period_type = 'annual'
)
ORDER BY free_cashflow_millions DESC;

-- 7. Sector-wise comparison
SELECT 
    c.sector,
    COUNT(*) as company_count,
    AVG(cm.trailing_pe) as avg_pe,
    AVG(cm.dividend_yield) as avg_dividend_yield,
    AVG(cm.beta) as avg_beta,
    SUM(cm.market_cap) as total_market_cap
FROM companies c
JOIN company_metrics cm ON c.id = cm.company_id
WHERE c.sector IS NOT NULL
GROUP BY c.sector
ORDER BY total_market_cap DESC;

-- 8. Monthly trading volume analysis
SELECT 
    c.symbol,
    EXTRACT(YEAR FROM ph.date) as year,
    EXTRACT(MONTH FROM ph.date) as month,
    AVG(ph.close_price) as avg_close_price,
    SUM(ph.volume) as total_volume,
    COUNT(*) as trading_days
FROM companies c
JOIN price_history ph ON c.id = ph.company_id
WHERE ph.date >= CURRENT_DATE - INTERVAL '2 years'
GROUP BY c.symbol, EXTRACT(YEAR FROM ph.date), EXTRACT(MONTH FROM ph.date)
ORDER BY c.symbol, year DESC, month DESC;

-- 9. Find stocks with recent price momentum
WITH price_momentum AS (
    SELECT 
        c.symbol,
        c.long_name,
        ph.close_price as current_price,
        ph.date as current_date,
        LAG(ph.close_price, 20) OVER (PARTITION BY c.id ORDER BY ph.date) as price_20_days_ago,
        LAG(ph.close_price, 50) OVER (PARTITION BY c.id ORDER BY ph.date) as price_50_days_ago
    FROM companies c
    JOIN price_history ph ON c.id = ph.company_id
    WHERE ph.date >= CURRENT_DATE - INTERVAL '60 days'
)
SELECT 
    symbol,
    long_name,
    current_price,
    current_date,
    ROUND(((current_price - price_20_days_ago) / price_20_days_ago * 100), 2) as return_20_days,
    ROUND(((current_price - price_50_days_ago) / price_50_days_ago * 100), 2) as return_50_days
FROM price_momentum
WHERE price_20_days_ago IS NOT NULL 
  AND price_50_days_ago IS NOT NULL
  AND current_date = (SELECT MAX(current_date) FROM price_momentum)
ORDER BY return_20_days DESC;

-- 10. Earnings surprise analysis
SELECT 
    c.symbol,
    c.long_name,
    e.earnings_date,
    e.eps_estimate,
    e.reported_eps,
    e.surprise_percent,
    CASE 
        WHEN e.surprise_percent > 5 THEN 'Positive Surprise'
        WHEN e.surprise_percent < -5 THEN 'Negative Surprise'
        ELSE 'In Line'
    END as surprise_category
FROM companies c
JOIN earnings e ON c.id = e.company_id
WHERE e.earnings_date >= CURRENT_DATE - INTERVAL '1 year'
ORDER BY e.earnings_date DESC, e.surprise_percent DESC;
