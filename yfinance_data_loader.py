
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import yfinance as yf
import json
from datetime import datetime
import logging

class YFinanceDataLoader:
    def __init__(self, db_config):
        """
        Initialize the data loader with database configuration
        db_config should contain: host, database, user, password, port
        """
        self.db_config = db_config
        self.conn = None
        self.setup_logging()
    
    def setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def connect_db(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.logger.info("Database connection established")
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            raise
    
    def close_db(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.logger.info("Database connection closed")
    
    def insert_or_update_company(self, symbol):
        """Insert or update company basic information"""
        ticker = yf.Ticker(symbol)
        info = ticker.info
        
        cursor = self.conn.cursor()
        
        # Insert or update company
        company_sql = """
        INSERT INTO companies (symbol, short_name, long_name, exchange, country, sector, 
                              industry, website, phone, address, city, long_business_summary, 
                              full_time_employees, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol) 
        DO UPDATE SET 
            short_name = EXCLUDED.short_name,
            long_name = EXCLUDED.long_name,
            exchange = EXCLUDED.exchange,
            country = EXCLUDED.country,
            sector = EXCLUDED.sector,
            industry = EXCLUDED.industry,
            website = EXCLUDED.website,
            phone = EXCLUDED.phone,
            address = EXCLUDED.address,
            city = EXCLUDED.city,
            long_business_summary = EXCLUDED.long_business_summary,
            full_time_employees = EXCLUDED.full_time_employees,
            updated_at = EXCLUDED.updated_at
        RETURNING id;
        """
        
        address = f"{info.get('address1', '')}, {info.get('address2', '')}".strip(', ')
        
        cursor.execute(company_sql, (
            symbol,
            info.get('shortName'),
            info.get('longName'),
            info.get('exchange'),
            info.get('country'),
            info.get('sector'),
            info.get('industry'),
            info.get('website'),
            info.get('phone'),
            address,
            info.get('city'),
            info.get('longBusinessSummary'),
            info.get('fullTimeEmployees'),
            datetime.now()
        ))
        
        company_id = cursor.fetchone()[0]
        self.conn.commit()
        cursor.close()
        
        self.logger.info(f"Company {symbol} inserted/updated with ID: {company_id}")
        return company_id
    
    def load_price_history(self, company_id, symbol, period="max"):
        """Load historical price data"""
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=period)
        
        if hist.empty:
            self.logger.warning(f"No historical data found for {symbol}")
            return
        
        cursor = self.conn.cursor()
        
        # Clear existing data for this company
        cursor.execute("DELETE FROM price_history WHERE company_id = %s", (company_id,))
        
        # Prepare data for bulk insert
        price_data = []
        for date, row in hist.iterrows():
            price_data.append((
                company_id,
                date.date(),
                float(row['Open']) if pd.notna(row['Open']) else None,
                float(row['High']) if pd.notna(row['High']) else None,
                float(row['Low']) if pd.notna(row['Low']) else None,
                float(row['Close']) if pd.notna(row['Close']) else None,
                float(row['Close']) if pd.notna(row['Close']) else None,  # Using Close as Adj Close
                int(row['Volume']) if pd.notna(row['Volume']) else None,
                float(row['Dividends']) if pd.notna(row['Dividends']) else 0,
                float(row['Stock Splits']) if pd.notna(row['Stock Splits']) else 0
            ))
        
        # Bulk insert
        execute_values(
            cursor,
            """INSERT INTO price_history 
               (company_id, date, open_price, high_price, low_price, close_price, 
                adj_close_price, volume, dividends, stock_splits) VALUES %s""",
            price_data
        )
        
        self.conn.commit()
        cursor.close()
        
        self.logger.info(f"Loaded {len(price_data)} price history records for {symbol}")
    
    def load_company_metrics(self, company_id, symbol):
        """Load current company metrics"""
        ticker = yf.Ticker(symbol)
        info = ticker.info
        
        cursor = self.conn.cursor()
        
        metrics_sql = """
        INSERT INTO company_metrics (
            company_id, market_cap, shares_outstanding, float_shares, previous_close,
            regular_market_price, fifty_two_week_low, fifty_two_week_high, fifty_day_average,
            two_hundred_day_average, trailing_pe, forward_pe, price_to_book, dividend_yield,
            dividend_rate, beta, book_value, eps_trailing_twelve_months, eps_forward,
            revenue_per_share, total_revenue, gross_profits, ebitda, operating_cashflow,
            free_cashflow, total_cash, total_debt, debt_to_equity, return_on_assets,
            return_on_equity, profit_margins, operating_margins, gross_margins,
            analyst_target_price, recommendation_mean, recommendation_key,
            number_of_analyst_opinions, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (company_id) 
        DO UPDATE SET 
            market_cap = EXCLUDED.market_cap,
            shares_outstanding = EXCLUDED.shares_outstanding,
            float_shares = EXCLUDED.float_shares,
            previous_close = EXCLUDED.previous_close,
            regular_market_price = EXCLUDED.regular_market_price,
            fifty_two_week_low = EXCLUDED.fifty_two_week_low,
            fifty_two_week_high = EXCLUDED.fifty_two_week_high,
            fifty_day_average = EXCLUDED.fifty_day_average,
            two_hundred_day_average = EXCLUDED.two_hundred_day_average,
            trailing_pe = EXCLUDED.trailing_pe,
            forward_pe = EXCLUDED.forward_pe,
            price_to_book = EXCLUDED.price_to_book,
            dividend_yield = EXCLUDED.dividend_yield,
            dividend_rate = EXCLUDED.dividend_rate,
            beta = EXCLUDED.beta,
            book_value = EXCLUDED.book_value,
            eps_trailing_twelve_months = EXCLUDED.eps_trailing_twelve_months,
            eps_forward = EXCLUDED.eps_forward,
            revenue_per_share = EXCLUDED.revenue_per_share,
            total_revenue = EXCLUDED.total_revenue,
            gross_profits = EXCLUDED.gross_profits,
            ebitda = EXCLUDED.ebitda,
            operating_cashflow = EXCLUDED.operating_cashflow,
            free_cashflow = EXCLUDED.free_cashflow,
            total_cash = EXCLUDED.total_cash,
            total_debt = EXCLUDED.total_debt,
            debt_to_equity = EXCLUDED.debt_to_equity,
            return_on_assets = EXCLUDED.return_on_assets,
            return_on_equity = EXCLUDED.return_on_equity,
            profit_margins = EXCLUDED.profit_margins,
            operating_margins = EXCLUDED.operating_margins,
            gross_margins = EXCLUDED.gross_margins,
            analyst_target_price = EXCLUDED.analyst_target_price,
            recommendation_mean = EXCLUDED.recommendation_mean,
            recommendation_key = EXCLUDED.recommendation_key,
            number_of_analyst_opinions = EXCLUDED.number_of_analyst_opinions,
            updated_at = EXCLUDED.updated_at;
        """
        
        cursor.execute(metrics_sql, (
            company_id,
            info.get('marketCap'),
            info.get('sharesOutstanding'),
            info.get('floatShares'),
            info.get('previousClose'),
            info.get('regularMarketPrice'),
            info.get('fiftyTwoWeekLow'),
            info.get('fiftyTwoWeekHigh'),
            info.get('fiftyDayAverage'),
            info.get('twoHundredDayAverage'),
            info.get('trailingPE'),
            info.get('forwardPE'),
            info.get('priceToBook'),
            info.get('dividendYield'),
            info.get('dividendRate'),
            info.get('beta'),
            info.get('bookValue'),
            info.get('epsTrailingTwelveMonths'),
            info.get('forwardEps'),
            info.get('revenuePerShare'),
            info.get('totalRevenue'),
            info.get('grossProfits'),
            info.get('ebitda'),
            info.get('operatingCashflow'),
            info.get('freeCashflow'),
            info.get('totalCash'),
            info.get('totalDebt'),
            info.get('debtToEquity'),
            info.get('returnOnAssets'),
            info.get('returnOnEquity'),
            info.get('profitMargins'),
            info.get('operatingMargins'),
            info.get('grossMargins'),
            info.get('targetMeanPrice'),
            info.get('recommendationMean'),
            info.get('recommendationKey'),
            info.get('numberOfAnalystOpinions'),
            datetime.now()
        ))
        
        self.conn.commit()
        cursor.close()
        
        self.logger.info(f"Company metrics loaded for {symbol}")
    
    def load_financial_statements(self, company_id, symbol):
        """Load income statements, balance sheets, and cash flow statements"""
        ticker = yf.Ticker(symbol)
        
        # Load annual and quarterly data
        for period_type in ['annual', 'quarterly']:
            if period_type == 'annual':
                income_stmt = ticker.financials.T if not ticker.financials.empty else pd.DataFrame()
                balance_sheet = ticker.balance_sheet.T if not ticker.balance_sheet.empty else pd.DataFrame()
                cashflow = ticker.cashflow.T if not ticker.cashflow.empty else pd.DataFrame()
            else:
                income_stmt = ticker.quarterly_financials.T if not ticker.quarterly_financials.empty else pd.DataFrame()
                balance_sheet = ticker.quarterly_balance_sheet.T if not ticker.quarterly_balance_sheet.empty else pd.DataFrame()
                cashflow = ticker.quarterly_cashflow.T if not ticker.quarterly_cashflow.empty else pd.DataFrame()
            
            self._load_income_statements(company_id, income_stmt, period_type)
            self._load_balance_sheets(company_id, balance_sheet, period_type)
            self._load_cashflow_statements(company_id, cashflow, period_type)
    
    def _load_income_statements(self, company_id, df, period_type):
        """Load income statement data"""
        if df.empty:
            return
        
        cursor = self.conn.cursor()
        
        # Clear existing data
        cursor.execute(
            "DELETE FROM income_statements WHERE company_id = %s AND period_type = %s",
            (company_id, period_type)
        )
        
        for date, row in df.iterrows():
            cursor.execute("""
                INSERT INTO income_statements (
                    company_id, period_ending, period_type, total_revenue, cost_of_revenue,
                    gross_profit, operating_income, ebit, ebitda, net_income,
                    net_income_common_stockholders, diluted_eps, basic_eps, operating_expense,
                    interest_expense, tax_provision, normalized_income, total_expenses
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                company_id, date.date(), period_type,
                self._safe_int(row.get('Total Revenue')),
                self._safe_int(row.get('Cost Of Revenue')),
                self._safe_int(row.get('Gross Profit')),
                self._safe_int(row.get('Operating Income')),
                self._safe_int(row.get('EBIT')),
                self._safe_int(row.get('EBITDA')),
                self._safe_int(row.get('Net Income')),
                self._safe_int(row.get('Net Income Common Stockholders')),
                self._safe_float(row.get('Diluted EPS')),
                self._safe_float(row.get('Basic EPS')),
                self._safe_int(row.get('Operating Expense')),
                self._safe_int(row.get('Interest Expense')),
                self._safe_int(row.get('Tax Provision')),
                self._safe_int(row.get('Normalized Income')),
                self._safe_int(row.get('Total Expenses'))
            ))
        
        self.conn.commit()
        cursor.close()
    
    def _load_balance_sheets(self, company_id, df, period_type):
        """Load balance sheet data"""
        if df.empty:
            return
        
        cursor = self.conn.cursor()
        
        # Clear existing data
        cursor.execute(
            "DELETE FROM balance_sheets WHERE company_id = %s AND period_type = %s",
            (company_id, period_type)
        )
        
        for date, row in df.iterrows():
            cursor.execute("""
                INSERT INTO balance_sheets (
                    company_id, period_ending, period_type, total_assets, total_liabilities,
                    stockholders_equity, total_debt, cash_and_cash_equivalents, current_assets,
                    current_liabilities, working_capital, retained_earnings, common_stock,
                    inventory, accounts_receivable, accounts_payable, net_ppe, goodwill,
                    total_equity_gross_minority_interest, minority_interest
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                company_id, date.date(), period_type,
                self._safe_int(row.get('Total Assets')),
                self._safe_int(row.get('Total Liabilities Net Minority Interest')),
                self._safe_int(row.get('Stockholders Equity')),
                self._safe_int(row.get('Total Debt')),
                self._safe_int(row.get('Cash And Cash Equivalents')),
                self._safe_int(row.get('Current Assets')),
                self._safe_int(row.get('Current Liabilities')),
                self._safe_int(row.get('Working Capital')),
                self._safe_int(row.get('Retained Earnings')),
                self._safe_int(row.get('Common Stock')),
                self._safe_int(row.get('Inventory')),
                self._safe_int(row.get('Accounts Receivable')),
                self._safe_int(row.get('Accounts Payable')),
                self._safe_int(row.get('Net PPE')),
                self._safe_int(row.get('Goodwill')),
                self._safe_int(row.get('Total Equity Gross Minority Interest')),
                self._safe_int(row.get('Minority Interest'))
            ))
        
        self.conn.commit()
        cursor.close()
    
    def _load_cashflow_statements(self, company_id, df, period_type):
        """Load cash flow statement data"""
        if df.empty:
            return
        
        cursor = self.conn.cursor()
        
        # Clear existing data
        cursor.execute(
            "DELETE FROM cash_flow_statements WHERE company_id = %s AND period_type = %s",
            (company_id, period_type)
        )
        
        for date, row in df.iterrows():
            cursor.execute("""
                INSERT INTO cash_flow_statements (
                    company_id, period_ending, period_type, operating_cash_flow, investing_cash_flow,
                    financing_cash_flow, free_cash_flow, capital_expenditure,
                    net_income_from_continuing_operations, depreciation_and_amortization,
                    change_in_working_capital, issuance_of_debt, repayment_of_debt,
                    cash_dividends_paid, end_cash_position, beginning_cash_position, changes_in_cash
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                company_id, date.date(), period_type,
                self._safe_int(row.get('Operating Cash Flow')),
                self._safe_int(row.get('Investing Cash Flow')),
                self._safe_int(row.get('Financing Cash Flow')),
                self._safe_int(row.get('Free Cash Flow')),
                self._safe_int(row.get('Capital Expenditure')),
                self._safe_int(row.get('Net Income From Continuing Operations')),
                self._safe_int(row.get('Depreciation And Amortization')),
                self._safe_int(row.get('Change In Working Capital')),
                self._safe_int(row.get('Issuance Of Debt')),
                self._safe_int(row.get('Repayment Of Debt')),
                self._safe_int(row.get('Cash Dividends Paid')),
                self._safe_int(row.get('End Cash Position')),
                self._safe_int(row.get('Beginning Cash Position')),
                self._safe_int(row.get('Changes In Cash'))
            ))
        
        self.conn.commit()
        cursor.close()
    
    def load_corporate_actions(self, company_id, symbol):
        """Load dividends and stock splits"""
        ticker = yf.Ticker(symbol)
        
        cursor = self.conn.cursor()
        
        # Clear existing data
        cursor.execute("DELETE FROM corporate_actions WHERE company_id = %s", (company_id,))
        
        # Load dividends
        dividends = ticker.dividends
        for date, amount in dividends.items():
            cursor.execute("""
                INSERT INTO corporate_actions (company_id, action_date, action_type, amount)
                VALUES (%s, %s, %s, %s)
            """, (company_id, date.date(), 'dividend', float(amount)))
        
        # Load stock splits
        splits = ticker.splits
        for date, ratio in splits.items():
            cursor.execute("""
                INSERT INTO corporate_actions (company_id, action_date, action_type, amount)
                VALUES (%s, %s, %s, %s)
            """, (company_id, date.date(), 'stock_split', float(ratio)))
        
        self.conn.commit()
        cursor.close()
        
        self.logger.info(f"Corporate actions loaded for {symbol}")
    
    def load_complete_data(self, symbol):
        """Load all available data for a symbol"""
        try:
            self.connect_db()
            
            self.logger.info(f"Starting data load for {symbol}")
            
            # Insert/update company
            company_id = self.insert_or_update_company(symbol)
            
            # Load all data types
            self.load_price_history(company_id, symbol)
            self.load_company_metrics(company_id, symbol)
            self.load_financial_statements(company_id, symbol)
            self.load_corporate_actions(company_id, symbol)
            
            # Log the update
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO data_updates (company_id, table_name, update_type, records_affected)
                VALUES (%s, %s, %s, %s)
            """, (company_id, 'all_tables', 'full_load', 1))
            self.conn.commit()
            cursor.close()
            
            self.logger.info(f"Data load completed for {symbol}")
            
        except Exception as e:
            self.logger.error(f"Error loading data for {symbol}: {e}")
            if self.conn:
                self.conn.rollback()
            raise
        finally:
            self.close_db()
    
    def _safe_int(self, value):
        """Safely convert to int, return None if not possible"""
        try:
            return int(value) if pd.notna(value) else None
        except (ValueError, TypeError):
            return None
    
    def _safe_float(self, value):
        """Safely convert to float, return None if not possible"""
        try:
            return float(value) if pd.notna(value) else None
        except (ValueError, TypeError):
            return None

# Example usage
if __name__ == "__main__":
    # Database configuration
    db_config = {
        'host': 'localhost',
        'database': 'yfinance_db',
        'user': 'your_username',
        'password': 'your_password',
        'port': 5432
    }
    
    # Initialize loader
    loader = YFinanceDataLoader(db_config)
    
    # Load data for Indian stocks
    indian_stocks = ['RELIANCE.NS', 'TCS.NS', 'INFY.NS', 'HDFCBANK.NS', 'ICICIBANK.NS']
    
    for symbol in indian_stocks:
        try:
            loader.load_complete_data(symbol)
            print(f"✓ Successfully loaded data for {symbol}")
        except Exception as e:
            print(f"✗ Failed to load data for {symbol}: {e}")
