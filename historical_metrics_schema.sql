
-- Historical Company Metrics Table for Time-Series Analysis
-- This extends the current company_metrics table to support historical data points

CREATE TABLE historical_company_metrics (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    metric_date DATE NOT NULL,
    period_type VARCHAR(10) NOT NULL, -- 'daily', 'monthly', 'quarterly', 'annual'
    
    -- Market Data
    market_cap BIGINT,
    shares_outstanding BIGINT,
    regular_market_price DECIMAL(15,6),
    fifty_two_week_low DECIMAL(15,6),
    fifty_two_week_high DECIMAL(15,6),
    fifty_day_average DECIMAL(15,6),
    two_hundred_day_average DECIMAL(15,6),
    
    -- Valuation Metrics
    trailing_pe NUMERIC(12, 6),
    forward_pe NUMERIC(12, 6),
    price_to_book NUMERIC(12, 6),
    price_to_sales DECIMAL(10,4),
    enterprise_value BIGINT,
    enterprise_to_revenue DECIMAL(10,4),
    enterprise_to_ebitda DECIMAL(10,4),
    
    -- Dividend Metrics
    dividend_yield NUMERIC(10, 8),
    dividend_rate NUMERIC(12, 2),
    payout_ratio DECIMAL(8,6),
    
    -- Financial Health Metrics
    beta DECIMAL(8,4),
    book_value DECIMAL(15,6),
    debt_to_equity DECIMAL(10,4),
    return_on_assets DECIMAL(8,4),
    return_on_equity DECIMAL(8,4),
    
    -- Profitability Metrics
    profit_margins DECIMAL(8,4),
    operating_margins DECIMAL(8,4),
    gross_margins DECIMAL(8,4),
    
    -- Growth Metrics
    earnings_growth DECIMAL(8,4),
    revenue_growth DECIMAL(8,4),
    
    -- Per Share Metrics
    eps_trailing_twelve_months DECIMAL(10,4),
    eps_forward DECIMAL(10,4),
    revenue_per_share DECIMAL(10,4),
    book_value_per_share DECIMAL(10,4),
    
    -- Cash Flow Metrics
    operating_cashflow_per_share DECIMAL(10,4),
    free_cashflow_per_share DECIMAL(10,4),
    
    -- Analyst Data
    analyst_target_price DECIMAL(15,6),
    recommendation_mean DECIMAL(4,2),
    number_of_analyst_opinions INTEGER,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(company_id, metric_date, period_type)
);

-- Indexes for efficient querying
CREATE INDEX idx_historical_metrics_company_date ON historical_company_metrics(company_id, metric_date DESC);
CREATE INDEX idx_historical_metrics_period_type ON historical_company_metrics(period_type, metric_date DESC);
CREATE INDEX idx_historical_metrics_date ON historical_company_metrics(metric_date DESC);

-- View for latest metrics by period
CREATE OR REPLACE VIEW latest_historical_metrics AS
WITH ranked_metrics AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY company_id, period_type ORDER BY metric_date DESC) as rn
    FROM historical_company_metrics
)
SELECT 
    c.symbol,
    c.long_name,
    hm.period_type,
    hm.metric_date,
    hm.market_cap,
    hm.trailing_pe,
    hm.price_to_book,
    hm.dividend_yield,
    hm.return_on_equity,
    hm.profit_margins,
    hm.earnings_growth,
    hm.revenue_growth
FROM ranked_metrics hm
JOIN companies c ON hm.company_id = c.id
WHERE hm.rn = 1;

-- View for metric trends (quarterly)
CREATE OR REPLACE VIEW quarterly_metric_trends AS
SELECT 
    c.symbol,
    c.long_name,
    hm.metric_date,
    hm.trailing_pe,
    hm.price_to_book,
    hm.return_on_equity,
    hm.profit_margins,
    LAG(hm.trailing_pe) OVER (PARTITION BY c.id ORDER BY hm.metric_date) as prev_pe,
    LAG(hm.return_on_equity) OVER (PARTITION BY c.id ORDER BY hm.metric_date) as prev_roe,
    LAG(hm.profit_margins) OVER (PARTITION BY c.id ORDER BY hm.metric_date) as prev_margins
FROM historical_company_metrics hm
JOIN companies c ON hm.company_id = c.id
WHERE hm.period_type = 'quarterly'
ORDER BY c.symbol, hm.metric_date DESC;
