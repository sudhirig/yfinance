
-- Historical Company Metrics Schema
-- This allows tracking company metrics over time (monthly, quarterly, annually)

-- 1. Historical Company Metrics Table
CREATE TABLE historical_company_metrics (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    metric_date DATE NOT NULL,
    period_type VARCHAR(20) NOT NULL, -- 'daily', 'monthly', 'quarterly', 'annual'
    
    -- Market Data
    market_cap BIGINT,
    shares_outstanding BIGINT,
    float_shares BIGINT,
    current_price DECIMAL(15,6),
    
    -- Valuation Metrics
    trailing_pe NUMERIC(12, 6),
    forward_pe NUMERIC(12, 6),
    price_to_book NUMERIC(12, 6),
    price_to_sales NUMERIC(12, 6),
    ev_to_ebitda NUMERIC(12, 6),
    
    -- Profitability Metrics
    gross_margin DECIMAL(8,6),
    operating_margin DECIMAL(8,6),
    profit_margin DECIMAL(8,6),
    return_on_assets DECIMAL(8,6),
    return_on_equity DECIMAL(8,6),
    
    -- Growth Metrics
    revenue_growth_yoy DECIMAL(8,4),
    earnings_growth_yoy DECIMAL(8,4),
    
    -- Financial Health
    debt_to_equity DECIMAL(8,4),
    current_ratio DECIMAL(8,4),
    quick_ratio DECIMAL(8,4),
    
    -- Cash Flow Metrics
    operating_cashflow BIGINT,
    free_cashflow BIGINT,
    fcf_per_share DECIMAL(10,4),
    
    -- Dividend Metrics
    dividend_yield NUMERIC(10, 8),
    dividend_rate NUMERIC(12, 2),
    payout_ratio DECIMAL(8,6),
    
    -- Other Key Metrics
    book_value_per_share DECIMAL(15,6),
    tangible_book_value_per_share DECIMAL(15,6),
    beta DECIMAL(8,4),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure unique combination of company, date, and period type
    UNIQUE(company_id, metric_date, period_type)
);

-- 2. Indexes for efficient querying
CREATE INDEX idx_historical_metrics_company_date ON historical_company_metrics(company_id, metric_date DESC);
CREATE INDEX idx_historical_metrics_period_type ON historical_company_metrics(period_type);
CREATE INDEX idx_historical_metrics_date ON historical_company_metrics(metric_date DESC);

-- 3. View for latest metrics (maintains compatibility with existing queries)
CREATE OR REPLACE VIEW latest_company_metrics AS
SELECT DISTINCT ON (company_id)
    company_id,
    metric_date,
    market_cap,
    trailing_pe,
    forward_pe,
    price_to_book,
    return_on_equity,
    debt_to_equity,
    dividend_yield,
    beta
FROM historical_company_metrics
ORDER BY company_id, metric_date DESC;

-- 4. View for quarterly metrics trends
CREATE OR REPLACE VIEW quarterly_metrics_trends AS
SELECT 
    c.symbol,
    c.long_name,
    hcm.metric_date,
    hcm.market_cap,
    hcm.trailing_pe,
    hcm.revenue_growth_yoy,
    hcm.earnings_growth_yoy,
    hcm.return_on_equity,
    hcm.debt_to_equity,
    
    -- Calculate quarter-over-quarter changes
    LAG(hcm.market_cap) OVER (PARTITION BY hcm.company_id ORDER BY hcm.metric_date) as prev_market_cap,
    LAG(hcm.return_on_equity) OVER (PARTITION BY hcm.company_id ORDER BY hcm.metric_date) as prev_roe
FROM companies c
JOIN historical_company_metrics hcm ON c.id = hcm.company_id
WHERE hcm.period_type = 'quarterly'
ORDER BY c.symbol, hcm.metric_date DESC;

-- 5. View for annual metrics comparison
CREATE OR REPLACE VIEW annual_metrics_comparison AS
SELECT 
    c.symbol,
    c.long_name,
    hcm.metric_date,
    hcm.trailing_pe,
    hcm.return_on_equity,
    hcm.debt_to_equity,
    hcm.revenue_growth_yoy,
    
    -- Industry averages for comparison
    AVG(hcm2.trailing_pe) OVER (PARTITION BY c.sector, hcm.metric_date) as sector_avg_pe,
    AVG(hcm2.return_on_equity) OVER (PARTITION BY c.sector, hcm.metric_date) as sector_avg_roe
FROM companies c
JOIN historical_company_metrics hcm ON c.id = hcm.company_id
JOIN historical_company_metrics hcm2 ON hcm2.metric_date = hcm.metric_date
JOIN companies c2 ON hcm2.company_id = c2.id AND c2.sector = c.sector
WHERE hcm.period_type = 'annual'
ORDER BY c.symbol, hcm.metric_date DESC;
