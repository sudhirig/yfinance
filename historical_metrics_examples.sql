
-- Historical Company Metrics Analysis Examples

-- 1. Get quarterly metrics trend for a specific company
SELECT 
    metric_date,
    trailing_pe,
    return_on_equity,
    debt_to_equity,
    revenue_growth_yoy
FROM historical_company_metrics
WHERE company_id = (SELECT id FROM companies WHERE symbol = 'RELIANCE.NS')
AND period_type = 'quarterly'
ORDER BY metric_date DESC
LIMIT 8;

-- 2. Compare PE ratios across companies over time
SELECT 
    c.symbol,
    hcm.metric_date,
    hcm.trailing_pe
FROM companies c
JOIN historical_company_metrics hcm ON c.id = hcm.company_id
WHERE c.symbol IN ('RELIANCE.NS', 'TCS.NS', 'INFY.NS')
AND hcm.period_type = 'quarterly'
AND hcm.metric_date >= '2023-01-01'
ORDER BY hcm.metric_date DESC, c.symbol;

-- 3. Find companies with improving ROE trend
WITH roe_trend AS (
    SELECT 
        company_id,
        metric_date,
        return_on_equity,
        LAG(return_on_equity) OVER (PARTITION BY company_id ORDER BY metric_date) as prev_roe
    FROM historical_company_metrics
    WHERE period_type = 'quarterly'
    AND metric_date >= '2023-01-01'
    AND return_on_equity IS NOT NULL
)
SELECT 
    c.symbol,
    c.long_name,
    rt.metric_date,
    rt.return_on_equity,
    rt.prev_roe,
    (rt.return_on_equity - rt.prev_roe) as roe_change
FROM companies c
JOIN roe_trend rt ON c.id = rt.company_id
WHERE rt.prev_roe IS NOT NULL
AND (rt.return_on_equity - rt.prev_roe) > 0.02  -- ROE improved by more than 2%
ORDER BY roe_change DESC;

-- 4. Sector-wise metrics comparison for latest quarter
WITH latest_metrics AS (
    SELECT 
        company_id,
        MAX(metric_date) as latest_date
    FROM historical_company_metrics
    WHERE period_type = 'quarterly'
    GROUP BY company_id
)
SELECT 
    c.sector,
    COUNT(*) as companies_count,
    AVG(hcm.trailing_pe) as avg_pe,
    AVG(hcm.return_on_equity) as avg_roe,
    AVG(hcm.debt_to_equity) as avg_debt_equity,
    AVG(hcm.revenue_growth_yoy) as avg_revenue_growth
FROM companies c
JOIN latest_metrics lm ON c.id = lm.company_id
JOIN historical_company_metrics hcm ON c.id = hcm.company_id AND hcm.metric_date = lm.latest_date
WHERE c.sector IS NOT NULL
AND hcm.period_type = 'quarterly'
GROUP BY c.sector
HAVING COUNT(*) >= 3  -- Only sectors with at least 3 companies
ORDER BY avg_roe DESC;

-- 5. Metrics volatility analysis
SELECT 
    c.symbol,
    c.long_name,
    COUNT(hcm.trailing_pe) as data_points,
    AVG(hcm.trailing_pe) as avg_pe,
    STDDEV(hcm.trailing_pe) as pe_volatility,
    MIN(hcm.trailing_pe) as min_pe,
    MAX(hcm.trailing_pe) as max_pe,
    (MAX(hcm.trailing_pe) - MIN(hcm.trailing_pe)) / AVG(hcm.trailing_pe) as pe_range_ratio
FROM companies c
JOIN historical_company_metrics hcm ON c.id = hcm.company_id
WHERE hcm.period_type = 'quarterly'
AND hcm.trailing_pe IS NOT NULL
AND hcm.metric_date >= '2022-01-01'
GROUP BY c.id, c.symbol, c.long_name
HAVING COUNT(hcm.trailing_pe) >= 4  -- At least 4 quarters of data
ORDER BY pe_volatility DESC;

-- 6. Growth momentum analysis
WITH growth_metrics AS (
    SELECT 
        company_id,
        metric_date,
        revenue_growth_yoy,
        earnings_growth_yoy,
        return_on_equity,
        LAG(revenue_growth_yoy, 1) OVER (PARTITION BY company_id ORDER BY metric_date) as prev_revenue_growth,
        LAG(return_on_equity, 1) OVER (PARTITION BY company_id ORDER BY metric_date) as prev_roe
    FROM historical_company_metrics
    WHERE period_type = 'quarterly'
    AND metric_date >= '2023-01-01'
)
SELECT 
    c.symbol,
    c.long_name,
    gm.metric_date,
    gm.revenue_growth_yoy,
    gm.prev_revenue_growth,
    gm.return_on_equity,
    gm.prev_roe,
    CASE 
        WHEN gm.revenue_growth_yoy > gm.prev_revenue_growth AND gm.return_on_equity > gm.prev_roe THEN 'Accelerating'
        WHEN gm.revenue_growth_yoy > gm.prev_revenue_growth OR gm.return_on_equity > gm.prev_roe THEN 'Improving'
        WHEN gm.revenue_growth_yoy < gm.prev_revenue_growth AND gm.return_on_equity < gm.prev_roe THEN 'Declining'
        ELSE 'Mixed'
    END as momentum
FROM companies c
JOIN growth_metrics gm ON c.id = gm.company_id
WHERE gm.prev_revenue_growth IS NOT NULL 
AND gm.prev_roe IS NOT NULL
ORDER BY c.symbol, gm.metric_date DESC;

-- 7. Historical valuation levels
SELECT 
    c.symbol,
    hcm.metric_date,
    hcm.trailing_pe,
    -- Calculate percentile of current PE vs historical range
    PERCENT_RANK() OVER (
        PARTITION BY hcm.company_id 
        ORDER BY hcm.trailing_pe
    ) as pe_percentile,
    
    -- Compare to 1-year average
    AVG(hcm.trailing_pe) OVER (
        PARTITION BY hcm.company_id 
        ORDER BY hcm.metric_date 
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) as four_quarter_avg_pe
    
FROM companies c
JOIN historical_company_metrics hcm ON c.id = hcm.company_id
WHERE hcm.period_type = 'quarterly'
AND hcm.trailing_pe IS NOT NULL
AND c.symbol IN ('RELIANCE.NS', 'TCS.NS', 'INFY.NS')
ORDER BY c.symbol, hcm.metric_date DESC;
