
-- Enhanced Database Schema for YFinance Data Storage
-- This schema supports all data types from yfinance downloads including CSV structures

-- Drop existing tables if they exist (for clean setup)
DROP TABLE IF EXISTS data_updates CASCADE;
DROP TABLE IF EXISTS company_news CASCADE;
DROP TABLE IF EXISTS earnings CASCADE;
DROP TABLE IF EXISTS major_holders_summary CASCADE;
DROP TABLE IF EXISTS holders CASCADE;
DROP TABLE IF EXISTS corporate_actions CASCADE;
DROP TABLE IF EXISTS cash_flow_statements CASCADE;
DROP TABLE IF EXISTS balance_sheets CASCADE;
DROP TABLE IF EXISTS income_statements CASCADE;
DROP TABLE IF EXISTS company_metrics CASCADE;
DROP TABLE IF EXISTS price_history CASCADE;
DROP TABLE IF EXISTS companies CASCADE;

-- 1. Companies Master Table
CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL UNIQUE,
    short_name VARCHAR(100),
    long_name VARCHAR(200),
    display_name VARCHAR(200),
    exchange VARCHAR(20),
    exchange_timezone_name VARCHAR(50),
    exchange_timezone_short_name VARCHAR(10),
    full_exchange_name VARCHAR(100),
    market VARCHAR(20),
    quote_type VARCHAR(20),
    region VARCHAR(20),
    language VARCHAR(10),
    country VARCHAR(50),
    sector VARCHAR(100),
    industry VARCHAR(200),
    industry_key VARCHAR(50),
    industry_disp VARCHAR(200),
    sector_key VARCHAR(50),
    sector_disp VARCHAR(100),
    website VARCHAR(200),
    phone VARCHAR(50),
    fax VARCHAR(50),
    address1 VARCHAR(200),
    address2 VARCHAR(200),
    city VARCHAR(100),
    zip VARCHAR(20),
    long_business_summary TEXT,
    full_time_employees INTEGER,
    audit_risk INTEGER,
    board_risk INTEGER,
    compensation_risk INTEGER,
    shareholder_rights_risk INTEGER,
    overall_risk INTEGER,
    governance_epoch_date BIGINT,
    compensation_as_of_epoch_date BIGINT,
    ir_website VARCHAR(200),
    max_age INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Historical Price Data (Enhanced for all history)
CREATE TABLE price_history (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    open_price DECIMAL(15,6),
    high_price DECIMAL(15,6),
    low_price DECIMAL(15,6),
    close_price DECIMAL(15,6),
    adj_close_price DECIMAL(15,6),
    volume BIGINT,
    dividends DECIMAL(10,6) DEFAULT 0,
    stock_splits DECIMAL(10,6) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_id, date)
);

-- 3. Company Financial Metrics (Current snapshot)
CREATE TABLE company_metrics (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    market_cap BIGINT,
    shares_outstanding BIGINT,
    float_shares BIGINT,
    implied_shares_outstanding BIGINT,
    previous_close DECIMAL(15,6),
    open_price DECIMAL(15,6),
    regular_market_open DECIMAL(15,6),
    regular_market_price DECIMAL(15,6),
    regular_market_high DECIMAL(15,6),
    regular_market_low DECIMAL(15,6),
    regular_market_volume BIGINT,
    regular_market_previous_close DECIMAL(15,6),
    regular_market_day_high DECIMAL(15,6),
    regular_market_day_low DECIMAL(15,6),
    fifty_two_week_low DECIMAL(15,6),
    fifty_two_week_high DECIMAL(15,6),
    fifty_day_average DECIMAL(15,6),
    two_hundred_day_average DECIMAL(15,6),
    trailing_pe DECIMAL(15,6),
    forward_pe DECIMAL(15,6),
    price_to_book DECIMAL(10,4),
    dividend_yield DECIMAL(12,8),
    dividend_rate DECIMAL(12,8),
    ex_dividend_date DATE,
    payout_ratio DECIMAL(12,8),
    five_year_avg_dividend_yield DECIMAL(12,8),
    beta DECIMAL(12,6),
    book_value DECIMAL(15,6),
    eps_trailing_twelve_months DECIMAL(10,4),
    eps_forward DECIMAL(10,4),
    earnings_growth DECIMAL(8,4),
    revenue_growth DECIMAL(8,4),
    revenue_per_share DECIMAL(10,4),
    total_revenue BIGINT,
    gross_profits BIGINT,
    ebitda BIGINT,
    operating_cashflow BIGINT,
    free_cashflow BIGINT,
    total_cash BIGINT,
    total_cash_per_share DECIMAL(10,4),
    total_debt BIGINT,
    debt_to_equity DECIMAL(10,4),
    return_on_assets DECIMAL(8,4),
    return_on_equity DECIMAL(8,4),
    profit_margins DECIMAL(8,4),
    operating_margins DECIMAL(8,4),
    gross_margins DECIMAL(8,4),
    analyst_target_price DECIMAL(15,6),
    recommendation_mean DECIMAL(4,2),
    recommendation_key VARCHAR(20),
    number_of_analyst_opinions INTEGER,
    enterprise_value BIGINT,
    price_to_sales_trailing_12months DECIMAL(10,4),
    enterprise_to_revenue DECIMAL(10,4),
    enterprise_to_ebitda DECIMAL(10,4),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_id)
);

-- 4. Income Statement (Annual & Quarterly)
CREATE TABLE income_statements (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    period_ending DATE NOT NULL,
    period_type VARCHAR(10) NOT NULL, -- 'annual' or 'quarterly'
    total_revenue BIGINT,
    cost_of_revenue BIGINT,
    gross_profit BIGINT,
    research_development BIGINT,
    selling_general_administrative BIGINT,
    total_operating_expenses BIGINT,
    operating_income BIGINT,
    ebit BIGINT,
    ebitda BIGINT,
    interest_income BIGINT,
    interest_expense BIGINT,
    other_income_expense_net BIGINT,
    income_before_tax BIGINT,
    tax_provision BIGINT,
    net_income BIGINT,
    net_income_common_stockholders BIGINT,
    diluted_eps DECIMAL(10,4),
    basic_eps DECIMAL(10,4),
    diluted_average_shares BIGINT,
    basic_average_shares BIGINT,
    operating_expense BIGINT,
    normalized_income BIGINT,
    total_expenses BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_id, period_ending, period_type)
);

-- 5. Balance Sheet (Annual & Quarterly)
CREATE TABLE balance_sheets (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    period_ending DATE NOT NULL,
    period_type VARCHAR(10) NOT NULL, -- 'annual' or 'quarterly'
    total_assets BIGINT,
    current_assets BIGINT,
    cash_and_cash_equivalents BIGINT,
    cash_cash_equivalents_and_short_term_investments BIGINT,
    other_short_term_investments BIGINT,
    accounts_receivable BIGINT,
    inventory BIGINT,
    prepaid_assets BIGINT,
    other_current_assets BIGINT,
    non_current_assets BIGINT,
    net_ppe BIGINT,
    goodwill BIGINT,
    other_intangible_assets BIGINT,
    investments_and_advances BIGINT,
    other_non_current_assets BIGINT,
    total_liabilities BIGINT,
    current_liabilities BIGINT,
    accounts_payable BIGINT,
    accrued_liabilities BIGINT,
    short_term_debt BIGINT,
    current_debt_and_capital_lease_obligation BIGINT,
    other_current_liabilities BIGINT,
    non_current_liabilities BIGINT,
    long_term_debt BIGINT,
    long_term_debt_and_capital_lease_obligation BIGINT,
    other_non_current_liabilities BIGINT,
    total_debt BIGINT,
    stockholders_equity BIGINT,
    retained_earnings BIGINT,
    common_stock BIGINT,
    capital_stock BIGINT,
    additional_paid_in_capital BIGINT,
    treasury_shares_number BIGINT,
    treasury_stock BIGINT,
    accumulated_other_comprehensive_income BIGINT,
    working_capital BIGINT,
    total_equity_gross_minority_interest BIGINT,
    minority_interest BIGINT,
    total_capitalization BIGINT,
    common_stock_equity BIGINT,
    net_tangible_assets BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_id, period_ending, period_type)
);

-- 6. Cash Flow Statement (Annual & Quarterly)
CREATE TABLE cash_flow_statements (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    period_ending DATE NOT NULL,
    period_type VARCHAR(10) NOT NULL, -- 'annual' or 'quarterly'
    operating_cash_flow BIGINT,
    investing_cash_flow BIGINT,
    financing_cash_flow BIGINT,
    free_cash_flow BIGINT,
    capital_expenditure BIGINT,
    net_income BIGINT,
    net_income_from_continuing_operations BIGINT,
    depreciation_depletion_and_amortization BIGINT,
    depreciation_and_amortization BIGINT,
    deferred_income_tax BIGINT,
    stock_based_compensation BIGINT,
    change_in_working_capital BIGINT,
    change_in_accounts_receivable BIGINT,
    change_in_inventory BIGINT,
    change_in_accounts_payable BIGINT,
    change_in_other_working_capital BIGINT,
    other_non_cash_items BIGINT,
    investments_in_property_plant_and_equipment BIGINT,
    acquisitions_net BIGINT,
    purchases_of_investments BIGINT,
    sales_maturities_of_investments BIGINT,
    other_investing_activities BIGINT,
    issuance_of_debt BIGINT,
    repayment_of_debt BIGINT,
    repurchase_of_capital_stock BIGINT,
    cash_dividends_paid BIGINT,
    other_financing_activities BIGINT,
    effect_of_exchange_rate_changes BIGINT,
    end_cash_position BIGINT,
    beginning_cash_position BIGINT,
    changes_in_cash BIGINT,
    financing_cash_flow_net BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_id, period_ending, period_type)
);

-- 7. Corporate Actions (Dividends & Stock Splits)
CREATE TABLE corporate_actions (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    action_date DATE NOT NULL,
    action_type VARCHAR(20) NOT NULL, -- 'dividend' or 'stock_split'
    amount DECIMAL(15,8),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_id, action_date, action_type)
);

-- 8. Holders Information
CREATE TABLE holders (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    holder_type VARCHAR(20) NOT NULL, -- 'institutional' or 'major'
    holder_name VARCHAR(300),
    shares BIGINT,
    percentage DECIMAL(8,4),
    value BIGINT,
    date_reported DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 9. Major Holders Summary
CREATE TABLE major_holders_summary (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    insiders_percent_held DECIMAL(8,4),
    institutions_percent_held DECIMAL(8,4),
    institutions_float_percent_held DECIMAL(8,4),
    institutions_count INTEGER,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_id)
);

-- 10. Earnings Data
CREATE TABLE earnings (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    earnings_date TIMESTAMP,
    ex_dividend_date DATE,
    earnings_high DECIMAL(10,4),
    earnings_low DECIMAL(10,4),
    earnings_average DECIMAL(10,4),
    revenue_high BIGINT,
    revenue_low BIGINT,
    revenue_average BIGINT,
    eps_estimate DECIMAL(10,4),
    reported_eps DECIMAL(10,4),
    surprise_percent DECIMAL(8,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 11. SEC Filings (may be empty for non-US stocks)
CREATE TABLE sec_filings (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    filing_date DATE,
    filing_type VARCHAR(50),
    description TEXT,
    url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 12. Company News
CREATE TABLE company_news (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    title VARCHAR(500),
    link VARCHAR(1000),
    published_date TIMESTAMP,
    source VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 13. Data Update Log
CREATE TABLE data_updates (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    table_name VARCHAR(50),
    update_type VARCHAR(20), -- 'insert', 'update', 'delete', 'full_load'
    records_affected INTEGER,
    file_source VARCHAR(200),
    update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(50) DEFAULT 'yfinance_csv'
);

-- Enhanced Indexes for better query performance
CREATE INDEX idx_price_history_company_date ON price_history(company_id, date DESC);
CREATE INDEX idx_price_history_date ON price_history(date DESC);
CREATE INDEX idx_companies_symbol ON companies(symbol);
CREATE INDEX idx_companies_sector ON companies(sector);
CREATE INDEX idx_companies_industry ON companies(industry);
CREATE INDEX idx_companies_exchange ON companies(exchange);
CREATE INDEX idx_income_statements_company_period ON income_statements(company_id, period_ending DESC, period_type);
CREATE INDEX idx_balance_sheets_company_period ON balance_sheets(company_id, period_ending DESC, period_type);
CREATE INDEX idx_cash_flow_company_period ON cash_flow_statements(company_id, period_ending DESC, period_type);
CREATE INDEX idx_corporate_actions_company_date ON corporate_actions(company_id, action_date DESC);
CREATE INDEX idx_earnings_company_date ON earnings(company_id, earnings_date DESC);
CREATE INDEX idx_holders_company_type ON holders(company_id, holder_type);

-- Enhanced Views for commonly used queries
CREATE OR REPLACE VIEW latest_price_data AS
SELECT 
    c.symbol,
    c.long_name,
    c.sector,
    c.industry,
    ph.date,
    ph.open_price,
    ph.high_price,
    ph.low_price,
    ph.close_price,
    ph.volume,
    ph.dividends,
    ph.stock_splits,
    cm.market_cap,
    cm.trailing_pe,
    cm.dividend_yield,
    cm.beta
FROM companies c
LEFT JOIN price_history ph ON c.id = ph.company_id
LEFT JOIN company_metrics cm ON c.id = cm.company_id
WHERE ph.date = (
    SELECT MAX(date) 
    FROM price_history ph2 
    WHERE ph2.company_id = ph.company_id
) OR ph.date IS NULL;

CREATE OR REPLACE VIEW annual_financials AS
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
    b.total_debt,
    cf.operating_cash_flow,
    cf.free_cash_flow,
    cf.capital_expenditure
FROM companies c
LEFT JOIN income_statements i ON c.id = i.company_id AND i.period_type = 'annual'
LEFT JOIN balance_sheets b ON c.id = b.company_id AND b.period_ending = i.period_ending AND b.period_type = 'annual'
LEFT JOIN cash_flow_statements cf ON c.id = cf.company_id AND cf.period_ending = i.period_ending AND cf.period_type = 'annual'
ORDER BY c.symbol, i.period_ending DESC;

CREATE OR REPLACE VIEW quarterly_financials AS
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
    b.total_debt,
    cf.operating_cash_flow,
    cf.free_cash_flow,
    cf.capital_expenditure
FROM companies c
LEFT JOIN income_statements i ON c.id = i.company_id AND i.period_type = 'quarterly'
LEFT JOIN balance_sheets b ON c.id = b.company_id AND b.period_ending = i.period_ending AND b.period_type = 'quarterly'
LEFT JOIN cash_flow_statements cf ON c.id = cf.company_id AND cf.period_ending = i.period_ending AND cf.period_type = 'quarterly'
ORDER BY c.symbol, i.period_ending DESC;

-- Company overview view
CREATE OR REPLACE VIEW company_overview AS
SELECT 
    c.symbol,
    c.long_name,
    c.sector,
    c.industry,
    c.country,
    c.exchange,
    c.website,
    c.full_time_employees,
    cm.market_cap,
    cm.trailing_pe,
    cm.forward_pe,
    cm.dividend_yield,
    cm.beta,
    cm.fifty_two_week_low,
    cm.fifty_two_week_high,
    cm.regular_market_price,
    ph.date as last_price_date,
    ph.close_price as last_close_price,
    ph.volume as last_volume
FROM companies c
LEFT JOIN company_metrics cm ON c.id = cm.company_id
LEFT JOIN price_history ph ON c.id = ph.company_id
WHERE ph.date = (
    SELECT MAX(date) 
    FROM price_history ph2 
    WHERE ph2.company_id = ph.company_id
) OR ph.date IS NULL;
