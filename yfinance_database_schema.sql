
-- Database Schema for YFinance Data Storage
-- This schema supports all data types from yfinance downloads

-- 1. Companies Master Table
CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL UNIQUE,
    short_name VARCHAR(100),
    long_name VARCHAR(200),
    exchange VARCHAR(10),
    country VARCHAR(50),
    sector VARCHAR(100),
    industry VARCHAR(200),
    website VARCHAR(200),
    phone VARCHAR(50),
    address TEXT,
    city VARCHAR(100),
    long_business_summary TEXT,
    full_time_employees INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Historical Price Data
CREATE TABLE price_history (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    date DATE NOT NULL,
    open_price DECIMAL(12,4),
    high_price DECIMAL(12,4),
    low_price DECIMAL(12,4),
    close_price DECIMAL(12,4),
    adj_close_price DECIMAL(12,4),
    volume BIGINT,
    dividends DECIMAL(10,6) DEFAULT 0,
    stock_splits DECIMAL(10,6) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_id, date)
);

-- 3. Company Financial Metrics (Current snapshot)
CREATE TABLE company_metrics (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    market_cap BIGINT,
    shares_outstanding BIGINT,
    float_shares BIGINT,
    previous_close DECIMAL(10,4),
    regular_market_price DECIMAL(10,4),
    fifty_two_week_low DECIMAL(10,4),
    fifty_two_week_high DECIMAL(10,4),
    fifty_day_average DECIMAL(10,4),
    two_hundred_day_average DECIMAL(10,4),
    trailing_pe DECIMAL(8,4),
    forward_pe DECIMAL(8,4),
    price_to_book DECIMAL(8,4),
    dividend_yield DECIMAL(6,4),
    dividend_rate DECIMAL(6,4),
    beta DECIMAL(6,4),
    book_value DECIMAL(10,4),
    eps_trailing_twelve_months DECIMAL(8,4),
    eps_forward DECIMAL(8,4),
    revenue_per_share DECIMAL(8,4),
    total_revenue BIGINT,
    gross_profits BIGINT,
    ebitda BIGINT,
    operating_cashflow BIGINT,
    free_cashflow BIGINT,
    total_cash BIGINT,
    total_debt BIGINT,
    debt_to_equity DECIMAL(8,4),
    return_on_assets DECIMAL(6,4),
    return_on_equity DECIMAL(6,4),
    profit_margins DECIMAL(6,4),
    operating_margins DECIMAL(6,4),
    gross_margins DECIMAL(6,4),
    analyst_target_price DECIMAL(10,4),
    recommendation_mean DECIMAL(4,2),
    recommendation_key VARCHAR(20),
    number_of_analyst_opinions INTEGER,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_id)
);

-- 4. Income Statement (Annual & Quarterly)
CREATE TABLE income_statements (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    period_ending DATE NOT NULL,
    period_type VARCHAR(10) NOT NULL, -- 'annual' or 'quarterly'
    total_revenue BIGINT,
    cost_of_revenue BIGINT,
    gross_profit BIGINT,
    operating_income BIGINT,
    ebit BIGINT,
    ebitda BIGINT,
    net_income BIGINT,
    net_income_common_stockholders BIGINT,
    diluted_eps DECIMAL(8,4),
    basic_eps DECIMAL(8,4),
    operating_expense BIGINT,
    interest_expense BIGINT,
    tax_provision BIGINT,
    normalized_income BIGINT,
    total_expenses BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_id, period_ending, period_type)
);

-- 5. Balance Sheet (Annual & Quarterly)
CREATE TABLE balance_sheets (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    period_ending DATE NOT NULL,
    period_type VARCHAR(10) NOT NULL, -- 'annual' or 'quarterly'
    total_assets BIGINT,
    total_liabilities BIGINT,
    stockholders_equity BIGINT,
    total_debt BIGINT,
    cash_and_cash_equivalents BIGINT,
    current_assets BIGINT,
    current_liabilities BIGINT,
    working_capital BIGINT,
    retained_earnings BIGINT,
    common_stock BIGINT,
    inventory BIGINT,
    accounts_receivable BIGINT,
    accounts_payable BIGINT,
    net_ppe BIGINT,
    goodwill BIGINT,
    total_equity_gross_minority_interest BIGINT,
    minority_interest BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_id, period_ending, period_type)
);

-- 6. Cash Flow Statement (Annual & Quarterly)
CREATE TABLE cash_flow_statements (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    period_ending DATE NOT NULL,
    period_type VARCHAR(10) NOT NULL, -- 'annual' or 'quarterly'
    operating_cash_flow BIGINT,
    investing_cash_flow BIGINT,
    financing_cash_flow BIGINT,
    free_cash_flow BIGINT,
    capital_expenditure BIGINT,
    net_income_from_continuing_operations BIGINT,
    depreciation_and_amortization BIGINT,
    change_in_working_capital BIGINT,
    issuance_of_debt BIGINT,
    repayment_of_debt BIGINT,
    cash_dividends_paid BIGINT,
    end_cash_position BIGINT,
    beginning_cash_position BIGINT,
    changes_in_cash BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_id, period_ending, period_type)
);

-- 7. Corporate Actions (Dividends & Stock Splits)
CREATE TABLE corporate_actions (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    action_date DATE NOT NULL,
    action_type VARCHAR(20) NOT NULL, -- 'dividend' or 'stock_split'
    amount DECIMAL(10,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_id, action_date, action_type)
);

-- 8. Holders Information
CREATE TABLE holders (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    holder_type VARCHAR(20) NOT NULL, -- 'institutional' or 'major'
    holder_name VARCHAR(200),
    shares BIGINT,
    percentage DECIMAL(6,4),
    value BIGINT,
    date_reported DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 9. Major Holders Summary
CREATE TABLE major_holders_summary (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    insiders_percent_held DECIMAL(6,4),
    institutions_percent_held DECIMAL(6,4),
    institutions_float_percent_held DECIMAL(6,4),
    institutions_count INTEGER,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_id)
);

-- 10. Earnings Data
CREATE TABLE earnings (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    earnings_date TIMESTAMP,
    eps_estimate DECIMAL(8,4),
    reported_eps DECIMAL(8,4),
    surprise_percent DECIMAL(6,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 11. Company News
CREATE TABLE company_news (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    title VARCHAR(500),
    link VARCHAR(1000),
    published_date TIMESTAMP,
    source VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 12. Data Update Log
CREATE TABLE data_updates (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    table_name VARCHAR(50),
    update_type VARCHAR(20), -- 'insert', 'update', 'delete'
    records_affected INTEGER,
    update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(50) DEFAULT 'yfinance'
);

-- Indexes for better query performance
CREATE INDEX idx_price_history_company_date ON price_history(company_id, date DESC);
CREATE INDEX idx_companies_symbol ON companies(symbol);
CREATE INDEX idx_companies_sector ON companies(sector);
CREATE INDEX idx_companies_industry ON companies(industry);
CREATE INDEX idx_income_statements_company_period ON income_statements(company_id, period_ending DESC);
CREATE INDEX idx_balance_sheets_company_period ON balance_sheets(company_id, period_ending DESC);
CREATE INDEX idx_cash_flow_company_period ON cash_flow_statements(company_id, period_ending DESC);
CREATE INDEX idx_corporate_actions_company_date ON corporate_actions(company_id, action_date DESC);
CREATE INDEX idx_earnings_company_date ON earnings(company_id, earnings_date DESC);

-- Views for commonly used queries
CREATE VIEW latest_price_data AS
SELECT 
    c.symbol,
    c.long_name,
    ph.date,
    ph.close_price,
    ph.volume,
    cm.market_cap,
    cm.trailing_pe,
    cm.dividend_yield
FROM companies c
JOIN price_history ph ON c.id = ph.company_id
JOIN company_metrics cm ON c.id = cm.company_id
WHERE ph.date = (
    SELECT MAX(date) 
    FROM price_history ph2 
    WHERE ph2.company_id = ph.company_id
);

CREATE VIEW annual_financials AS
SELECT 
    c.symbol,
    c.long_name,
    i.period_ending,
    i.total_revenue,
    i.gross_profit,
    i.operating_income,
    i.net_income,
    i.diluted_eps,
    b.total_assets,
    b.stockholders_equity,
    cf.operating_cash_flow,
    cf.free_cash_flow
FROM companies c
JOIN income_statements i ON c.id = i.company_id AND i.period_type = 'annual'
JOIN balance_sheets b ON c.id = b.company_id AND b.period_ending = i.period_ending AND b.period_type = 'annual'
JOIN cash_flow_statements cf ON c.id = cf.company_id AND cf.period_ending = i.period_ending AND cf.period_type = 'annual'
ORDER BY c.symbol, i.period_ending DESC;
