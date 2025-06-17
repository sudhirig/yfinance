
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
import os
import json
from datetime import datetime, date
import logging
import glob
from typing import Dict, List, Optional, Any
from database_config import get_database_config

class CSVToDatabaseLoader:
    def __init__(self):
        """
        Initialize the CSV to database loader
        """
        self.db_config = get_database_config()
        self.conn = None
        self.setup_logging()
    
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def connect_db(self):
        """Establish database connection"""
        try:
            # Try to use DATABASE_URL first (for Replit)
            database_url = os.getenv('DATABASE_URL')
            if database_url:
                self.conn = psycopg2.connect(database_url)
            else:
                self.conn = psycopg2.connect(**self.db_config)
            
            self.conn.autocommit = False
            self.logger.info("Database connection established")
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            raise
    
    def close_db(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.logger.info("Database connection closed")
    
    def safe_convert_to_float(self, value):
        """Safely convert value to float"""
        if pd.isna(value) or value == '' or value == 'N/A':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def safe_convert_to_int(self, value):
        """Safely convert value to int"""
        if pd.isna(value) or value == '' or value == 'N/A':
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
    
    def safe_convert_to_date(self, value):
        """Safely convert value to date"""
        if pd.isna(value) or value == '' or value == 'N/A':
            return None
        try:
            if isinstance(value, str):
                # Try different date formats
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S']:
                    try:
                        return datetime.strptime(value, fmt).date()
                    except ValueError:
                        continue
            elif isinstance(value, (datetime, date)):
                return value if isinstance(value, date) else value.date()
            return None
        except Exception:
            return None
    
    def get_or_create_company(self, symbol: str, company_data: Dict = None) -> int:
        """Get existing company ID or create new company record"""
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
        # Check if company exists
        cursor.execute("SELECT id FROM companies WHERE symbol = %s", (symbol,))
        result = cursor.fetchone()
        
        if result:
            company_id = result['id']
            self.logger.info(f"Found existing company {symbol} with ID: {company_id}")
        else:
            # Create new company record
            if company_data:
                insert_sql = """
                INSERT INTO companies (
                    symbol, short_name, long_name, display_name, exchange, 
                    exchange_timezone_name, exchange_timezone_short_name, full_exchange_name,
                    market, quote_type, region, language, country, sector, industry,
                    industry_key, industry_disp, sector_key, sector_disp, website,
                    phone, fax, address1, address2, city, zip, long_business_summary,
                    full_time_employees, audit_risk, board_risk, compensation_risk,
                    shareholder_rights_risk, overall_risk, governance_epoch_date,
                    compensation_as_of_epoch_date, ir_website, max_age, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
                """
                
                cursor.execute(insert_sql, (
                    symbol,
                    company_data.get('shortName'),
                    company_data.get('longName'),
                    company_data.get('displayName'),
                    company_data.get('exchange'),
                    company_data.get('exchangeTimezoneName'),
                    company_data.get('exchangeTimezoneShortName'),
                    company_data.get('fullExchangeName'),
                    company_data.get('market'),
                    company_data.get('quoteType'),
                    company_data.get('region'),
                    company_data.get('language'),
                    company_data.get('country'),
                    company_data.get('sector'),
                    company_data.get('industry'),
                    company_data.get('industryKey'),
                    company_data.get('industryDisp'),
                    company_data.get('sectorKey'),
                    company_data.get('sectorDisp'),
                    company_data.get('website'),
                    company_data.get('phone'),
                    company_data.get('fax'),
                    company_data.get('address1'),
                    company_data.get('address2'),
                    company_data.get('city'),
                    company_data.get('zip'),
                    company_data.get('longBusinessSummary'),
                    self.safe_convert_to_int(company_data.get('fullTimeEmployees')),
                    self.safe_convert_to_int(company_data.get('auditRisk')),
                    self.safe_convert_to_int(company_data.get('boardRisk')),
                    self.safe_convert_to_int(company_data.get('compensationRisk')),
                    self.safe_convert_to_int(company_data.get('shareHolderRightsRisk')),
                    self.safe_convert_to_int(company_data.get('overallRisk')),
                    self.safe_convert_to_int(company_data.get('governanceEpochDate')),
                    self.safe_convert_to_int(company_data.get('compensationAsOfEpochDate')),
                    company_data.get('irWebsite'),
                    self.safe_convert_to_int(company_data.get('maxAge')),
                    datetime.now()
                ))
            else:
                # Create minimal company record
                cursor.execute("""
                    INSERT INTO companies (symbol, updated_at) 
                    VALUES (%s, %s) RETURNING id;
                """, (symbol, datetime.now()))
            
            company_id = cursor.fetchone()['id']
            self.logger.info(f"Created new company {symbol} with ID: {company_id}")
        
        cursor.close()
        return company_id
    
    def load_company_info_from_csv(self, file_path: str) -> Dict[str, int]:
        """Load company information from all_info CSV"""
        self.logger.info(f"Loading company info from {file_path}")
        
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                self.logger.warning(f"Empty CSV file: {file_path}")
                return {}
            
            company_ids = {}
            for _, row in df.iterrows():
                symbol = row.get('symbol', '').strip()
                if symbol:
                    company_data = row.to_dict()
                    company_id = self.get_or_create_company(symbol, company_data)
                    company_ids[symbol] = company_id
            
            self.conn.commit()
            return company_ids
            
        except Exception as e:
            self.logger.error(f"Error loading company info from {file_path}: {e}")
            self.conn.rollback()
            return {}
    
    def load_full_data_from_csv(self, file_path: str) -> Dict[str, int]:
        """Load company information from full_data CSV"""
        self.logger.info(f"Loading full data from {file_path}")
        
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                self.logger.warning(f"Empty CSV file: {file_path}")
                return {}
            
            company_ids = {}
            for _, row in df.iterrows():
                symbol = row.get('symbol', '').strip()
                if symbol:
                    company_data = row.to_dict()
                    company_id = self.get_or_create_company(symbol, company_data)
                    company_ids[symbol] = company_id
                    
                    # Also load company metrics if available
                    self.load_company_metrics_from_row(company_id, row)
            
            self.conn.commit()
            return company_ids
            
        except Exception as e:
            self.logger.error(f"Error loading full data from {file_path}: {e}")
            self.conn.rollback()
            return {}
    
    def load_company_metrics_from_row(self, company_id: int, row: pd.Series):
        """Load company metrics from a data row"""
        cursor = self.conn.cursor()
        
        metrics_sql = """
        INSERT INTO company_metrics (
            company_id, market_cap, shares_outstanding, float_shares, implied_shares_outstanding,
            previous_close, open_price, regular_market_open, regular_market_price,
            regular_market_high, regular_market_low, regular_market_volume,
            regular_market_previous_close, regular_market_day_high, regular_market_day_low,
            fifty_two_week_low, fifty_two_week_high, fifty_day_average, two_hundred_day_average,
            trailing_pe, forward_pe, price_to_book, dividend_yield, dividend_rate,
            ex_dividend_date, payout_ratio, five_year_avg_dividend_yield, beta, book_value,
            eps_trailing_twelve_months, eps_forward, earnings_growth, revenue_growth,
            revenue_per_share, total_revenue, gross_profits, ebitda, operating_cashflow,
            free_cashflow, total_cash, total_cash_per_share, total_debt, debt_to_equity,
            return_on_assets, return_on_equity, profit_margins, operating_margins,
            gross_margins, analyst_target_price, recommendation_mean, recommendation_key,
            number_of_analyst_opinions, enterprise_value, price_to_sales_trailing_12months,
            enterprise_to_revenue, enterprise_to_ebitda, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (company_id) DO UPDATE SET
            market_cap = EXCLUDED.market_cap,
            shares_outstanding = EXCLUDED.shares_outstanding,
            float_shares = EXCLUDED.float_shares,
            implied_shares_outstanding = EXCLUDED.implied_shares_outstanding,
            previous_close = EXCLUDED.previous_close,
            open_price = EXCLUDED.open_price,
            regular_market_open = EXCLUDED.regular_market_open,
            regular_market_price = EXCLUDED.regular_market_price,
            regular_market_high = EXCLUDED.regular_market_high,
            regular_market_low = EXCLUDED.regular_market_low,
            regular_market_volume = EXCLUDED.regular_market_volume,
            regular_market_previous_close = EXCLUDED.regular_market_previous_close,
            regular_market_day_high = EXCLUDED.regular_market_day_high,
            regular_market_day_low = EXCLUDED.regular_market_day_low,
            fifty_two_week_low = EXCLUDED.fifty_two_week_low,
            fifty_two_week_high = EXCLUDED.fifty_two_week_high,
            fifty_day_average = EXCLUDED.fifty_day_average,
            two_hundred_day_average = EXCLUDED.two_hundred_day_average,
            trailing_pe = EXCLUDED.trailing_pe,
            forward_pe = EXCLUDED.forward_pe,
            price_to_book = EXCLUDED.price_to_book,
            dividend_yield = EXCLUDED.dividend_yield,
            dividend_rate = EXCLUDED.dividend_rate,
            ex_dividend_date = EXCLUDED.ex_dividend_date,
            payout_ratio = EXCLUDED.payout_ratio,
            five_year_avg_dividend_yield = EXCLUDED.five_year_avg_dividend_yield,
            beta = EXCLUDED.beta,
            book_value = EXCLUDED.book_value,
            eps_trailing_twelve_months = EXCLUDED.eps_trailing_twelve_months,
            eps_forward = EXCLUDED.eps_forward,
            earnings_growth = EXCLUDED.earnings_growth,
            revenue_growth = EXCLUDED.revenue_growth,
            revenue_per_share = EXCLUDED.revenue_per_share,
            total_revenue = EXCLUDED.total_revenue,
            gross_profits = EXCLUDED.gross_profits,
            ebitda = EXCLUDED.ebitda,
            operating_cashflow = EXCLUDED.operating_cashflow,
            free_cashflow = EXCLUDED.free_cashflow,
            total_cash = EXCLUDED.total_cash,
            total_cash_per_share = EXCLUDED.total_cash_per_share,
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
            enterprise_value = EXCLUDED.enterprise_value,
            price_to_sales_trailing_12months = EXCLUDED.price_to_sales_trailing_12months,
            enterprise_to_revenue = EXCLUDED.enterprise_to_revenue,
            enterprise_to_ebitda = EXCLUDED.enterprise_to_ebitda,
            updated_at = EXCLUDED.updated_at;
        """
        
        cursor.execute(metrics_sql, (
            company_id,
            self.safe_convert_to_int(row.get('marketCap')),
            self.safe_convert_to_int(row.get('sharesOutstanding')),
            self.safe_convert_to_int(row.get('floatShares')),
            self.safe_convert_to_int(row.get('impliedSharesOutstanding')),
            self.safe_convert_to_float(row.get('previousClose')),
            self.safe_convert_to_float(row.get('open')),
            self.safe_convert_to_float(row.get('regularMarketOpen')),
            self.safe_convert_to_float(row.get('regularMarketPrice')),
            self.safe_convert_to_float(row.get('regularMarketDayHigh')),
            self.safe_convert_to_float(row.get('regularMarketDayLow')),
            self.safe_convert_to_int(row.get('regularMarketVolume')),
            self.safe_convert_to_float(row.get('regularMarketPreviousClose')),
            self.safe_convert_to_float(row.get('regularMarketDayHigh')),
            self.safe_convert_to_float(row.get('regularMarketDayLow')),
            self.safe_convert_to_float(row.get('fiftyTwoWeekLow')),
            self.safe_convert_to_float(row.get('fiftyTwoWeekHigh')),
            self.safe_convert_to_float(row.get('fiftyDayAverage')),
            self.safe_convert_to_float(row.get('twoHundredDayAverage')),
            self.safe_convert_to_float(row.get('trailingPE')),
            self.safe_convert_to_float(row.get('forwardPE')),
            self.safe_convert_to_float(row.get('priceToBook')),
            self.safe_convert_to_float(row.get('dividendYield')),
            self.safe_convert_to_float(row.get('dividendRate')),
            self.safe_convert_to_date(row.get('exDividendDate')),
            self.safe_convert_to_float(row.get('payoutRatio')),
            self.safe_convert_to_float(row.get('fiveYearAvgDividendYield')),
            self.safe_convert_to_float(row.get('beta')),
            self.safe_convert_to_float(row.get('bookValue')),
            self.safe_convert_to_float(row.get('epsTrailingTwelveMonths')),
            self.safe_convert_to_float(row.get('forwardEps')),
            self.safe_convert_to_float(row.get('earningsGrowth')),
            self.safe_convert_to_float(row.get('revenueGrowth')),
            self.safe_convert_to_float(row.get('revenuePerShare')),
            self.safe_convert_to_int(row.get('totalRevenue')),
            self.safe_convert_to_int(row.get('grossProfits')),
            self.safe_convert_to_int(row.get('ebitda')),
            self.safe_convert_to_int(row.get('operatingCashflow')),
            self.safe_convert_to_int(row.get('freeCashflow')),
            self.safe_convert_to_int(row.get('totalCash')),
            self.safe_convert_to_float(row.get('totalCashPerShare')),
            self.safe_convert_to_int(row.get('totalDebt')),
            self.safe_convert_to_float(row.get('debtToEquity')),
            self.safe_convert_to_float(row.get('returnOnAssets')),
            self.safe_convert_to_float(row.get('returnOnEquity')),
            self.safe_convert_to_float(row.get('profitMargins')),
            self.safe_convert_to_float(row.get('operatingMargins')),
            self.safe_convert_to_float(row.get('grossMargins')),
            self.safe_convert_to_float(row.get('targetMeanPrice')),
            self.safe_convert_to_float(row.get('recommendationMean')),
            row.get('recommendationKey'),
            self.safe_convert_to_int(row.get('numberOfAnalystOpinions')),
            self.safe_convert_to_int(row.get('enterpriseValue')),
            self.safe_convert_to_float(row.get('priceToSalesTrailing12Months')),
            self.safe_convert_to_float(row.get('enterpriseToRevenue')),
            self.safe_convert_to_float(row.get('enterpriseToEbitda')),
            datetime.now()
        ))
        
        cursor.close()
    
    def load_price_history_from_csv(self, file_path: str, company_id: int, symbol: str):
        """Load price history from CSV"""
        self.logger.info(f"Loading price history from {file_path} for {symbol}")
        
        try:
            df = pd.read_csv(file_path, index_col='Date', parse_dates=True)
            if df.empty:
                self.logger.warning(f"Empty price history file: {file_path}")
                return
            
            cursor = self.conn.cursor()
            
            # Clear existing price history for this company
            cursor.execute("DELETE FROM price_history WHERE company_id = %s", (company_id,))
            
            # Prepare data for bulk insert
            price_data = []
            for date_idx, row in df.iterrows():
                price_data.append((
                    company_id,
                    date_idx.date(),
                    self.safe_convert_to_float(row.get('Open')),
                    self.safe_convert_to_float(row.get('High')),
                    self.safe_convert_to_float(row.get('Low')),
                    self.safe_convert_to_float(row.get('Close')),
                    self.safe_convert_to_float(row.get('Adj Close', row.get('Close'))),
                    self.safe_convert_to_int(row.get('Volume')),
                    self.safe_convert_to_float(row.get('Dividends', 0)),
                    self.safe_convert_to_float(row.get('Stock Splits', 0))
                ))
            
            # Bulk insert
            if price_data:
                execute_values(
                    cursor,
                    """INSERT INTO price_history 
                       (company_id, date, open_price, high_price, low_price, close_price, 
                        adj_close_price, volume, dividends, stock_splits) VALUES %s""",
                    price_data,
                    page_size=1000
                )
                
                self.logger.info(f"Loaded {len(price_data)} price history records for {symbol}")
            
            cursor.close()
            
        except Exception as e:
            self.logger.error(f"Error loading price history from {file_path}: {e}")
            self.conn.rollback()
    
    def load_financial_statements_from_csv(self, file_path: str, company_id: int, symbol: str, statement_type: str, period_type: str):
        """Load financial statements from CSV"""
        self.logger.info(f"Loading {statement_type} {period_type} from {file_path} for {symbol}")
        
        try:
            df = pd.read_csv(file_path, index_col=0)
            if df.empty:
                self.logger.warning(f"Empty financial statement file: {file_path}")
                return
            
            cursor = self.conn.cursor()
            
            # Clear existing data
            table_name = f"{statement_type}_statements"
            cursor.execute(
                f"DELETE FROM {table_name} WHERE company_id = %s AND period_type = %s",
                (company_id, period_type)
            )
            
            # Process each column (date) in the DataFrame
            for col in df.columns:
                period_ending = self.safe_convert_to_date(col)
                if not period_ending:
                    continue
                
                if statement_type == 'income':
                    self._insert_income_statement(cursor, company_id, period_ending, period_type, df[col])
                elif statement_type == 'balance_sheet':
                    self._insert_balance_sheet(cursor, company_id, period_ending, period_type, df[col])
                elif statement_type == 'cash_flow':
                    self._insert_cash_flow_statement(cursor, company_id, period_ending, period_type, df[col])
            
            cursor.close()
            self.logger.info(f"Loaded {statement_type} {period_type} for {symbol}")
            
        except Exception as e:
            self.logger.error(f"Error loading {statement_type} from {file_path}: {e}")
            self.conn.rollback()
    
    def _insert_income_statement(self, cursor, company_id: int, period_ending: date, period_type: str, data: pd.Series):
        """Insert income statement data"""
        cursor.execute("""
            INSERT INTO income_statements (
                company_id, period_ending, period_type, total_revenue, cost_of_revenue,
                gross_profit, research_development, selling_general_administrative,
                total_operating_expenses, operating_income, ebit, ebitda,
                interest_income, interest_expense, other_income_expense_net,
                income_before_tax, tax_provision, net_income, net_income_common_stockholders,
                diluted_eps, basic_eps, diluted_average_shares, basic_average_shares,
                operating_expense, normalized_income, total_expenses
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (company_id, period_ending, period_type) DO NOTHING
        """, (
            company_id, period_ending, period_type,
            self.safe_convert_to_int(data.get('Total Revenue')),
            self.safe_convert_to_int(data.get('Cost Of Revenue')),
            self.safe_convert_to_int(data.get('Gross Profit')),
            self.safe_convert_to_int(data.get('Research Development')),
            self.safe_convert_to_int(data.get('Selling General Administrative')),
            self.safe_convert_to_int(data.get('Total Operating Expenses')),
            self.safe_convert_to_int(data.get('Operating Income')),
            self.safe_convert_to_int(data.get('EBIT')),
            self.safe_convert_to_int(data.get('EBITDA')),
            self.safe_convert_to_int(data.get('Interest Income')),
            self.safe_convert_to_int(data.get('Interest Expense')),
            self.safe_convert_to_int(data.get('Other Income Expense Net')),
            self.safe_convert_to_int(data.get('Income Before Tax')),
            self.safe_convert_to_int(data.get('Tax Provision')),
            self.safe_convert_to_int(data.get('Net Income')),
            self.safe_convert_to_int(data.get('Net Income Common Stockholders')),
            self.safe_convert_to_float(data.get('Diluted EPS')),
            self.safe_convert_to_float(data.get('Basic EPS')),
            self.safe_convert_to_int(data.get('Diluted Average Shares')),
            self.safe_convert_to_int(data.get('Basic Average Shares')),
            self.safe_convert_to_int(data.get('Operating Expense')),
            self.safe_convert_to_int(data.get('Normalized Income')),
            self.safe_convert_to_int(data.get('Total Expenses'))
        ))
    
    def _insert_balance_sheet(self, cursor, company_id: int, period_ending: date, period_type: str, data: pd.Series):
        """Insert balance sheet data"""
        cursor.execute("""
            INSERT INTO balance_sheets (
                company_id, period_ending, period_type, total_assets, current_assets,
                cash_and_cash_equivalents, cash_cash_equivalents_and_short_term_investments,
                other_short_term_investments, accounts_receivable, inventory, prepaid_assets,
                other_current_assets, non_current_assets, net_ppe, goodwill,
                other_intangible_assets, investments_and_advances, other_non_current_assets,
                total_liabilities, current_liabilities, accounts_payable, accrued_liabilities,
                short_term_debt, current_debt_and_capital_lease_obligation, other_current_liabilities,
                non_current_liabilities, long_term_debt, long_term_debt_and_capital_lease_obligation,
                other_non_current_liabilities, total_debt, stockholders_equity, retained_earnings,
                common_stock, capital_stock, additional_paid_in_capital, treasury_shares_number,
                treasury_stock, accumulated_other_comprehensive_income, working_capital,
                total_equity_gross_minority_interest, minority_interest, total_capitalization,
                common_stock_equity, net_tangible_assets
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (company_id, period_ending, period_type) DO NOTHING
        """, (
            company_id, period_ending, period_type,
            self.safe_convert_to_int(data.get('Total Assets')),
            self.safe_convert_to_int(data.get('Current Assets')),
            self.safe_convert_to_int(data.get('Cash And Cash Equivalents')),
            self.safe_convert_to_int(data.get('Cash Cash Equivalents And Short Term Investments')),
            self.safe_convert_to_int(data.get('Other Short Term Investments')),
            self.safe_convert_to_int(data.get('Accounts Receivable')),
            self.safe_convert_to_int(data.get('Inventory')),
            self.safe_convert_to_int(data.get('Prepaid Assets')),
            self.safe_convert_to_int(data.get('Other Current Assets')),
            self.safe_convert_to_int(data.get('Non Current Assets')),
            self.safe_convert_to_int(data.get('Net PPE')),
            self.safe_convert_to_int(data.get('Goodwill')),
            self.safe_convert_to_int(data.get('Other Intangible Assets')),
            self.safe_convert_to_int(data.get('Investments And Advances')),
            self.safe_convert_to_int(data.get('Other Non Current Assets')),
            self.safe_convert_to_int(data.get('Total Liabilities Net Minority Interest')),
            self.safe_convert_to_int(data.get('Current Liabilities')),
            self.safe_convert_to_int(data.get('Accounts Payable')),
            self.safe_convert_to_int(data.get('Accrued Liabilities')),
            self.safe_convert_to_int(data.get('Short Term Debt')),
            self.safe_convert_to_int(data.get('Current Debt And Capital Lease Obligation')),
            self.safe_convert_to_int(data.get('Other Current Liabilities')),
            self.safe_convert_to_int(data.get('Non Current Liabilities')),
            self.safe_convert_to_int(data.get('Long Term Debt')),
            self.safe_convert_to_int(data.get('Long Term Debt And Capital Lease Obligation')),
            self.safe_convert_to_int(data.get('Other Non Current Liabilities')),
            self.safe_convert_to_int(data.get('Total Debt')),
            self.safe_convert_to_int(data.get('Stockholders Equity')),
            self.safe_convert_to_int(data.get('Retained Earnings')),
            self.safe_convert_to_int(data.get('Common Stock')),
            self.safe_convert_to_int(data.get('Capital Stock')),
            self.safe_convert_to_int(data.get('Additional Paid In Capital')),
            self.safe_convert_to_int(data.get('Treasury Shares Number')),
            self.safe_convert_to_int(data.get('Treasury Stock')),
            self.safe_convert_to_int(data.get('Accumulated Other Comprehensive Income')),
            self.safe_convert_to_int(data.get('Working Capital')),
            self.safe_convert_to_int(data.get('Total Equity Gross Minority Interest')),
            self.safe_convert_to_int(data.get('Minority Interest')),
            self.safe_convert_to_int(data.get('Total Capitalization')),
            self.safe_convert_to_int(data.get('Common Stock Equity')),
            self.safe_convert_to_int(data.get('Net Tangible Assets'))
        ))
    
    def _insert_cash_flow_statement(self, cursor, company_id: int, period_ending: date, period_type: str, data: pd.Series):
        """Insert cash flow statement data"""
        cursor.execute("""
            INSERT INTO cash_flow_statements (
                company_id, period_ending, period_type, operating_cash_flow, investing_cash_flow,
                financing_cash_flow, free_cash_flow, capital_expenditure, net_income,
                net_income_from_continuing_operations, depreciation_depletion_and_amortization,
                depreciation_and_amortization, deferred_income_tax, stock_based_compensation,
                change_in_working_capital, change_in_accounts_receivable, change_in_inventory,
                change_in_accounts_payable, change_in_other_working_capital, other_non_cash_items,
                investments_in_property_plant_and_equipment, acquisitions_net, purchases_of_investments,
                sales_maturities_of_investments, other_investing_activities, issuance_of_debt,
                repayment_of_debt, repurchase_of_capital_stock, cash_dividends_paid,
                other_financing_activities, effect_of_exchange_rate_changes, end_cash_position,
                beginning_cash_position, changes_in_cash, financing_cash_flow_net
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (company_id, period_ending, period_type) DO NOTHING
        """, (
            company_id, period_ending, period_type,
            self.safe_convert_to_int(data.get('Operating Cash Flow')),
            self.safe_convert_to_int(data.get('Investing Cash Flow')),
            self.safe_convert_to_int(data.get('Financing Cash Flow')),
            self.safe_convert_to_int(data.get('Free Cash Flow')),
            self.safe_convert_to_int(data.get('Capital Expenditure')),
            self.safe_convert_to_int(data.get('Net Income')),
            self.safe_convert_to_int(data.get('Net Income From Continuing Operations')),
            self.safe_convert_to_int(data.get('Depreciation Depletion And Amortization')),
            self.safe_convert_to_int(data.get('Depreciation And Amortization')),
            self.safe_convert_to_int(data.get('Deferred Income Tax')),
            self.safe_convert_to_int(data.get('Stock Based Compensation')),
            self.safe_convert_to_int(data.get('Change In Working Capital')),
            self.safe_convert_to_int(data.get('Change In Accounts Receivable')),
            self.safe_convert_to_int(data.get('Change In Inventory')),
            self.safe_convert_to_int(data.get('Change In Accounts Payable')),
            self.safe_convert_to_int(data.get('Change In Other Working Capital')),
            self.safe_convert_to_int(data.get('Other Non Cash Items')),
            self.safe_convert_to_int(data.get('Investments In Property Plant And Equipment')),
            self.safe_convert_to_int(data.get('Acquisitions Net')),
            self.safe_convert_to_int(data.get('Purchases Of Investments')),
            self.safe_convert_to_int(data.get('Sales Maturities Of Investments')),
            self.safe_convert_to_int(data.get('Other Investing Activities')),
            self.safe_convert_to_int(data.get('Issuance Of Debt')),
            self.safe_convert_to_int(data.get('Repayment Of Debt')),
            self.safe_convert_to_int(data.get('Repurchase Of Capital Stock')),
            self.safe_convert_to_int(data.get('Cash Dividends Paid')),
            self.safe_convert_to_int(data.get('Other Financing Activities')),
            self.safe_convert_to_int(data.get('Effect Of Exchange Rate Changes')),
            self.safe_convert_to_int(data.get('End Cash Position')),
            self.safe_convert_to_int(data.get('Beginning Cash Position')),
            self.safe_convert_to_int(data.get('Changes In Cash')),
            self.safe_convert_to_int(data.get('Financing Cash Flow'))
        ))
    
    def load_corporate_actions_from_csv(self, file_path: str, company_id: int, symbol: str):
        """Load corporate actions from CSV"""
        self.logger.info(f"Loading corporate actions from {file_path} for {symbol}")
        
        try:
            df = pd.read_csv(file_path, parse_dates=['Date'])
            if df.empty:
                self.logger.warning(f"Empty corporate actions file: {file_path}")
                return
            
            cursor = self.conn.cursor()
            
            # Clear existing corporate actions for this company
            cursor.execute("DELETE FROM corporate_actions WHERE company_id = %s", (company_id,))
            
            for _, row in df.iterrows():
                action_date = self.safe_convert_to_date(row.get('Date'))
                if not action_date:
                    continue
                
                # Check for dividends
                if 'Dividends' in row and pd.notna(row['Dividends']) and row['Dividends'] != 0:
                    cursor.execute("""
                        INSERT INTO corporate_actions (company_id, action_date, action_type, amount)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (company_id, action_date, action_type) DO NOTHING
                    """, (company_id, action_date, 'dividend', self.safe_convert_to_float(row['Dividends'])))
                
                # Check for stock splits
                if 'Stock Splits' in row and pd.notna(row['Stock Splits']) and row['Stock Splits'] != 0:
                    cursor.execute("""
                        INSERT INTO corporate_actions (company_id, action_date, action_type, amount)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (company_id, action_date, action_type) DO NOTHING
                    """, (company_id, action_date, 'stock_split', self.safe_convert_to_float(row['Stock Splits'])))
            
            cursor.close()
            self.logger.info(f"Loaded corporate actions for {symbol}")
            
        except Exception as e:
            self.logger.error(f"Error loading corporate actions from {file_path}: {e}")
            self.conn.rollback()
    
    def load_earnings_from_csv(self, file_path: str, company_id: int, symbol: str):
        """Load earnings data from CSV"""
        self.logger.info(f"Loading earnings from {file_path} for {symbol}")
        
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                self.logger.warning(f"Empty earnings file: {file_path}")
                return
            
            cursor = self.conn.cursor()
            
            for _, row in df.iterrows():
                earnings_date = self.safe_convert_to_date(row.get('Earnings Date'))
                
                cursor.execute("""
                    INSERT INTO earnings (
                        company_id, earnings_date, eps_estimate, reported_eps, surprise_percent
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    company_id,
                    earnings_date,
                    self.safe_convert_to_float(row.get('EPS Estimate')),
                    self.safe_convert_to_float(row.get('Reported EPS')),
                    self.safe_convert_to_float(row.get('Surprise(%)'))
                ))
            
            cursor.close()
            self.logger.info(f"Loaded earnings for {symbol}")
            
        except Exception as e:
            self.logger.error(f"Error loading earnings from {file_path}: {e}")
            self.conn.rollback()
    
    def extract_symbol_from_filename(self, filename: str) -> Optional[str]:
        """Extract symbol from filename like 'RELIANCE_NS_all_history_123456.csv'"""
        parts = filename.split('_')
        if len(parts) >= 2:
            return f"{parts[0]}.{parts[1]}" if parts[1] in ['NS', 'BO'] else parts[0]
        return None
    
    def load_all_csv_files(self, directory: str = "attached_assets"):
        """Load all CSV files from the specified directory"""
        try:
            self.connect_db()
            
            # Find all CSV files
            csv_files = glob.glob(os.path.join(directory, "*.csv"))
            
            if not csv_files:
                self.logger.warning(f"No CSV files found in {directory}")
                return
            
            self.logger.info(f"Found {len(csv_files)} CSV files in {directory}")
            
            # Group files by symbol
            symbol_files = {}
            for file_path in csv_files:
                filename = os.path.basename(file_path)
                symbol = self.extract_symbol_from_filename(filename)
                
                if symbol:
                    if symbol not in symbol_files:
                        symbol_files[symbol] = {}
                    
                    # Categorize file type
                    if 'all_info' in filename:
                        symbol_files[symbol]['all_info'] = file_path
                    elif 'full_data' in filename:
                        symbol_files[symbol]['full_data'] = file_path
                    elif 'all_history' in filename:
                        symbol_files[symbol]['all_history'] = file_path
                    elif 'key_info' in filename:
                        symbol_files[symbol]['key_info'] = file_path
                    elif 'income_stmt_annual' in filename:
                        symbol_files[symbol]['income_annual'] = file_path
                    elif 'income_stmt_quarterly' in filename:
                        symbol_files[symbol]['income_quarterly'] = file_path
                    elif 'balance_sheet_annual' in filename:
                        symbol_files[symbol]['balance_annual'] = file_path
                    elif 'balance_sheet_quarterly' in filename:
                        symbol_files[symbol]['balance_quarterly'] = file_path
                    elif 'cashflow_annual' in filename:
                        symbol_files[symbol]['cashflow_annual'] = file_path
                    elif 'cashflow_quarterly' in filename:
                        symbol_files[symbol]['cashflow_quarterly'] = file_path
                    elif 'corporate_actions' in filename:
                        symbol_files[symbol]['corporate_actions'] = file_path
                    elif 'dividends' in filename:
                        symbol_files[symbol]['dividends'] = file_path
                    elif 'splits' in filename:
                        symbol_files[symbol]['splits'] = file_path
                    elif 'earnings_dates' in filename:
                        symbol_files[symbol]['earnings_dates'] = file_path
            
            # Process each symbol
            for symbol, files in symbol_files.items():
                self.logger.info(f"Processing symbol: {symbol}")
                
                try:
                    # Start with company info
                    company_id = None
                    
                    if 'all_info' in files:
                        company_ids = self.load_company_info_from_csv(files['all_info'])
                        company_id = company_ids.get(symbol)
                    elif 'full_data' in files:
                        company_ids = self.load_full_data_from_csv(files['full_data'])
                        company_id = company_ids.get(symbol)
                    elif 'key_info' in files:
                        # Load key info as basic company info
                        try:
                            df = pd.read_csv(files['key_info'])
                            if not df.empty:
                                row = df.iloc[0]
                                company_data = row.to_dict()
                                company_id = self.get_or_create_company(symbol, company_data)
                        except Exception as e:
                            self.logger.error(f"Error loading key info for {symbol}: {e}")
                    
                    if not company_id:
                        company_id = self.get_or_create_company(symbol)
                    
                    # Load price history
                    if 'all_history' in files:
                        self.load_price_history_from_csv(files['all_history'], company_id, symbol)
                    
                    # Load financial statements
                    if 'income_annual' in files:
                        self.load_financial_statements_from_csv(files['income_annual'], company_id, symbol, 'income', 'annual')
                    if 'income_quarterly' in files:
                        self.load_financial_statements_from_csv(files['income_quarterly'], company_id, symbol, 'income', 'quarterly')
                    if 'balance_annual' in files:
                        self.load_financial_statements_from_csv(files['balance_annual'], company_id, symbol, 'balance_sheet', 'annual')
                    if 'balance_quarterly' in files:
                        self.load_financial_statements_from_csv(files['balance_quarterly'], company_id, symbol, 'balance_sheet', 'quarterly')
                    if 'cashflow_annual' in files:
                        self.load_financial_statements_from_csv(files['cashflow_annual'], company_id, symbol, 'cash_flow', 'annual')
                    if 'cashflow_quarterly' in files:
                        self.load_financial_statements_from_csv(files['cashflow_quarterly'], company_id, symbol, 'cash_flow', 'quarterly')
                    
                    # Load corporate actions
                    if 'corporate_actions' in files:
                        self.load_corporate_actions_from_csv(files['corporate_actions'], company_id, symbol)
                    
                    # Load earnings
                    if 'earnings_dates' in files:
                        self.load_earnings_from_csv(files['earnings_dates'], company_id, symbol)
                    
                    # Log successful load
                    cursor = self.conn.cursor()
                    cursor.execute("""
                        INSERT INTO data_updates (company_id, table_name, update_type, records_affected, file_source)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (company_id, 'all_tables', 'csv_load', len(files), f"{symbol}_csv_files"))
                    cursor.close()
                    
                    self.conn.commit()
                    self.logger.info(f"✓ Successfully loaded all data for {symbol}")
                    
                except Exception as e:
                    self.logger.error(f"✗ Error processing {symbol}: {e}")
                    self.conn.rollback()
                    continue
            
            self.logger.info("CSV loading completed!")
            
        except Exception as e:
            self.logger.error(f"Error in load_all_csv_files: {e}")
            if self.conn:
                self.conn.rollback()
            raise
        finally:
            self.close_db()

if __name__ == "__main__":
    loader = CSVToDatabaseLoader()
    loader.load_all_csv_files()
