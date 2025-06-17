import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import os
import time
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Any
import json
from database_config import get_database_config
from nse_symbols_fetcher import NSESymbolsFetcher

class YFinanceNSEDownloader:
    def __init__(self):
        """
        Initialize the NSE downloader
        """
        self.db_config = get_database_config()
        self.conn = None
        self.setup_logging()
        self.batch_size = 50  # Larger batches for 4000+ stocks efficiency
        self.delay_between_stocks = 0.3  # Optimized delay for high volume
        self.delay_between_batches = 2  # Reduced batch delay for faster processing

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('yfinance_nse_downloader.log')
            ]
        )
        self.logger = logging.getLogger(__name__)

    def connect_db(self):
        """Establish database connection"""
        try:
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

    def create_database_schema(self):
        """Create database schema"""
        try:
            cursor = self.conn.cursor()

            # Read and execute schema file
            schema_file = 'enhanced_yfinance_schema.sql'
            if os.path.exists(schema_file):
                self.logger.info(f"Executing schema from {schema_file}")
                with open(schema_file, 'r') as f:
                    schema_sql = f.read()

                # Execute schema with proper error handling
                try:
                    cursor.execute(schema_sql)
                    self.conn.commit()
                    self.logger.info("✓ Database schema created successfully")
                except psycopg2.Error as e:
                    if "already exists" in str(e):
                        self.logger.info("✓ Database schema already exists, continuing...")
                        self.conn.rollback()  # Rollback the failed transaction
                    else:
                        raise e

                cursor.close()
                return True
            else:
                self.logger.error(f"Schema file {schema_file} not found")
                return False

        except Exception as e:
            self.logger.error(f"Error creating database schema: {e}")
            self.conn.rollback()
            return False

    def safe_convert_to_float(self, value):
        """Safely convert value to float"""
        if pd.isna(value) or value == '' or value == 'N/A' or value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def safe_convert_to_int(self, value):
        """Safely convert value to int"""
        if pd.isna(value) or value == '' or value == 'N/A' or value is None:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def safe_convert_to_date(self, value):
        """Safely convert value to date"""
        if pd.isna(value) or value == '' or value == 'N/A' or value is None:
            return None
        try:
            if isinstance(value, str):
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

    def get_or_create_company(self, symbol: str, ticker_info: Dict = None) -> int:
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
            if ticker_info:
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
                    ticker_info.get('shortName'),
                    ticker_info.get('longName'),
                    ticker_info.get('displayName'),
                    ticker_info.get('exchange'),
                    ticker_info.get('exchangeTimezoneName'),
                    ticker_info.get('exchangeTimezoneShortName'),
                    ticker_info.get('fullExchangeName'),
                    ticker_info.get('market'),
                    ticker_info.get('quoteType'),
                    ticker_info.get('region'),
                    ticker_info.get('language'),
                    ticker_info.get('country'),
                    ticker_info.get('sector'),
                    ticker_info.get('industry'),
                    ticker_info.get('industryKey'),
                    ticker_info.get('industryDisp'),
                    ticker_info.get('sectorKey'),
                    ticker_info.get('sectorDisp'),
                    ticker_info.get('website'),
                    ticker_info.get('phone'),
                    ticker_info.get('fax'),
                    ticker_info.get('address1'),
                    ticker_info.get('address2'),
                    ticker_info.get('city'),
                    ticker_info.get('zip'),
                    ticker_info.get('longBusinessSummary'),
                    self.safe_convert_to_int(ticker_info.get('fullTimeEmployees')),
                    self.safe_convert_to_int(ticker_info.get('auditRisk')),
                    self.safe_convert_to_int(ticker_info.get('boardRisk')),
                    self.safe_convert_to_int(ticker_info.get('compensationRisk')),
                    self.safe_convert_to_int(ticker_info.get('shareHolderRightsRisk')),
                    self.safe_convert_to_int(ticker_info.get('overallRisk')),
                    self.safe_convert_to_int(ticker_info.get('governanceEpochDate')),
                    self.safe_convert_to_int(ticker_info.get('compensationAsOfEpochDate')),
                    ticker_info.get('irWebsite'),
                    self.safe_convert_to_int(ticker_info.get('maxAge')),
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

    def download_and_store_company_data(self, symbol: str) -> bool:
        """Download and store all data for a single company"""
        try:
            self.logger.info(f"📈 Starting download for {symbol}")
            ticker = yf.Ticker(symbol)

            # Get company info
            try:
                info = ticker.info
                if not info or len(info) < 5:
                    self.logger.warning(f"⚠️ No info data for {symbol}")
                    company_id = self.get_or_create_company(symbol)
                else:
                    company_id = self.get_or_create_company(symbol, info)
                    # Store company metrics
                    try:
                        self.store_company_metrics(company_id, info)
                        self.logger.info(f"✓ Stored company metrics for {symbol}")
                    except Exception as e:
                        self.logger.error(f"Error storing company metrics for {symbol}: {e}")

            except Exception as e:
                self.logger.error(f"Error getting info for {symbol}: {e}")
                company_id = self.get_or_create_company(symbol)

            # Download historical data (5 years)
            price_success = False
            try:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=5*365)  # 5 years
                hist_data = ticker.history(start=start_date, end=end_date)

                if not hist_data.empty:
                    self.store_price_history(company_id, hist_data)
                    self.logger.info(f"✓ Stored {len(hist_data)} price records for {symbol}")
                    price_success = True
                else:
                    self.logger.warning(f"⚠️ No historical data for {symbol}")

            except Exception as e:
                self.logger.error(f"❌ Error downloading history for {symbol}: {e}")

            # Download financial statements
            financials_success = False
            try:
                self.logger.info(f"📊 Downloading financial statements for {symbol}")

                # Annual financials - try each type separately
                annual_success = False
                try:
                    self.download_and_store_financials(ticker, company_id, symbol, 'annual')
                    annual_success = True
                    self.logger.info(f"✓ Annual financials downloaded for {symbol}")
                except Exception as e:
                    self.logger.warning(f"⚠️ Annual financials failed for {symbol}: {e}")

                # Quarterly financials - try each type separately
                quarterly_success = False
                try:
                    self.download_and_store_financials(ticker, company_id, symbol, 'quarterly')
                    quarterly_success = True
                    self.logger.info(f"✓ Quarterly financials downloaded for {symbol}")
                except Exception as e:
                    self.logger.warning(f"⚠️ Quarterly financials failed for {symbol}: {e}")

                financials_success = annual_success or quarterly_success

            except Exception as e:
                self.logger.error(f"❌ Error downloading financials for {symbol}: {e}")

            # Download corporate actions
            actions_success = False
            try:
                self.download_and_store_corporate_actions(ticker, company_id, symbol)
                actions_success = True
            except Exception as e:
                self.logger.error(f"❌ Error downloading corporate actions for {symbol}: {e}")

            # Log successful download
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO data_updates (company_id, table_name, update_type, records_affected, file_source)
                VALUES (%s, %s, %s, %s, %s)
            """, (company_id, 'all_tables', 'yfinance_download', 1, f"yfinance_{symbol}"))
            cursor.close()

            self.conn.commit()

            # Summary log
            status_summary = []
            if price_success:
                status_summary.append("Price✓")
            if financials_success:
                status_summary.append("Financials✓")
            if actions_success:
                status_summary.append("Actions✓")

            self.logger.info(f"🎯 {symbol} complete: {', '.join(status_summary) if status_summary else 'Basic info only'}")
            return True

        except Exception as e:
            self.logger.error(f"💥 Fatal error processing {symbol}: {e}")
            self.conn.rollback()
            return False

    def store_company_metrics(self, company_id: int, info: Dict):
        """Store company metrics"""
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
            self.safe_convert_to_int(info.get('marketCap')),
            self.safe_convert_to_int(info.get('sharesOutstanding')),
            self.safe_convert_to_int(info.get('floatShares')),
            self.safe_convert_to_int(info.get('impliedSharesOutstanding')),
            self.safe_convert_to_float(info.get('previousClose')),
            self.safe_convert_to_float(info.get('open')),
            self.safe_convert_to_float(info.get('regularMarketOpen')),
            self.safe_convert_to_float(info.get('regularMarketPrice')),
            self.safe_convert_to_float(info.get('regularMarketDayHigh')),
            self.safe_convert_to_float(info.get('regularMarketDayLow')),
            self.safe_convert_to_int(info.get('regularMarketVolume')),
            self.safe_convert_to_float(info.get('regularMarketPreviousClose')),
            self.safe_convert_to_float(info.get('regularMarketDayHigh')),
            self.safe_convert_to_float(info.get('regularMarketDayLow')),
            self.safe_convert_to_float(info.get('fiftyTwoWeekLow')),
            self.safe_convert_to_float(info.get('fiftyTwoWeekHigh')),
            self.safe_convert_to_float(info.get('fiftyDayAverage')),
            self.safe_convert_to_float(info.get('twoHundredDayAverage')),
            self.safe_convert_to_float(info.get('trailingPE')),
            self.safe_convert_to_float(info.get('forwardPE')),
            self.safe_convert_to_float(info.get('priceToBook')),
            self.safe_convert_to_float(info.get('dividendYield')),
            self.safe_convert_to_float(info.get('dividendRate')),
            self.safe_convert_to_date(info.get('exDividendDate')),
            self.safe_convert_to_float(info.get('payoutRatio')),
            self.safe_convert_to_float(info.get('fiveYearAvgDividendYield')),
            self.safe_convert_to_float(info.get('beta')),
            self.safe_convert_to_float(info.get('bookValue')),
            self.safe_convert_to_float(info.get('epsTrailingTwelveMonths')),
            self.safe_convert_to_float(info.get('forwardEps')),
            self.safe_convert_to_float(info.get('earningsGrowth')),
            self.safe_convert_to_float(info.get('revenueGrowth')),
            self.safe_convert_to_float(info.get('revenuePerShare')),
            self.safe_convert_to_int(info.get('totalRevenue')),
            self.safe_convert_to_int(info.get('grossProfits')),
            self.safe_convert_to_int(info.get('ebitda')),
            self.safe_convert_to_int(info.get('operatingCashflow')),
            self.safe_convert_to_int(info.get('freeCashflow')),
            self.safe_convert_to_int(info.get('totalCash')),
            self.safe_convert_to_float(info.get('totalCashPerShare')),
            self.safe_convert_to_int(info.get('totalDebt')),
            self.safe_convert_to_float(info.get('debtToEquity')),
            self.safe_convert_to_float(info.get('returnOnAssets')),
            self.safe_convert_to_float(info.get('returnOnEquity')),
            self.safe_convert_to_float(info.get('profitMargins')),
            self.safe_convert_to_float(info.get('operatingMargins')),
            self.safe_convert_to_float(info.get('grossMargins')),
            self.safe_convert_to_float(info.get('targetMeanPrice')),
            self.safe_convert_to_float(info.get('recommendationMean')),
            info.get('recommendationKey'),
            self.safe_convert_to_int(info.get('numberOfAnalystOpinions')),
            self.safe_convert_to_int(info.get('enterpriseValue')),
            self.safe_convert_to_float(info.get('priceToSalesTrailing12Months')),
            self.safe_convert_to_float(info.get('enterpriseToRevenue')),
            self.safe_convert_to_float(info.get('enterpriseToEbitda')),
            datetime.now()
        ))

        cursor.close()

    def store_price_history(self, company_id: int, hist_data: pd.DataFrame):
        """Store price history data"""
        cursor = self.conn.cursor()

        # Clear existing price history for this company
        cursor.execute("DELETE FROM price_history WHERE company_id = %s", (company_id,))

        # Prepare data for bulk insert
        price_data = []
        for date_idx, row in hist_data.iterrows():
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

        cursor.close()

    def download_and_store_financials(self, ticker, company_id: int, symbol: str, period_type: str):
        """Download and store financial statements"""
        try:
            if period_type == 'annual':
                income_stmt = ticker.income_stmt
                balance_sheet = ticker.balance_sheet
                cashflow = ticker.cashflow
            else:
                income_stmt = ticker.quarterly_income_stmt
                balance_sheet = ticker.quarterly_balance_sheet
                cashflow = ticker.quarterly_cashflow

            # Store income statements
            try:
                if income_stmt is not None and not income_stmt.empty:
                    self.store_income_statements(company_id, income_stmt, period_type)
                    self.logger.info(f"Stored {period_type} income statements for {symbol}")
                else:
                    self.logger.warning(f"No {period_type} income statement data for {symbol}")
            except Exception as e:
                self.logger.error(f"Error storing {period_type} income statements for {symbol}: {e}")

            # Store balance sheets
            try:
                if balance_sheet is not None and not balance_sheet.empty:
                    self.store_balance_sheets(company_id, balance_sheet, period_type)
                    self.logger.info(f"Stored {period_type} balance sheets for {symbol}")
                else:
                    self.logger.warning(f"No {period_type} balance sheet data for {symbol}")
            except Exception as e:
                self.logger.error(f"Error storing {period_type} balance sheets for {symbol}: {e}")

            # Store cash flow statements
            try:
                if cashflow is not None and not cashflow.empty:
                    self.store_cash_flow_statements(company_id, cashflow, period_type)
                    self.logger.info(f"Stored {period_type} cash flow statements for {symbol}")
                else:
                    self.logger.warning(f"No {period_type} cash flow data for {symbol}")
            except Exception as e:
                self.logger.error(f"Error storing {period_type} cash flow statements for {symbol}: {e}")

        except Exception as e:
            self.logger.error(f"Error downloading {period_type} financials for {symbol}: {e}")

    def store_income_statements(self, company_id: int, data: pd.DataFrame, period_type: str):
        """Store income statement data"""
        cursor = self.conn.cursor()

        # Clear existing data
        cursor.execute(
            "DELETE FROM income_statements WHERE company_id = %s AND period_type = %s",
            (company_id, period_type)
        )

        records_inserted = 0
        for col in data.columns:
            period_ending = self.safe_convert_to_date(col)
            if not period_ending:
                continue

            try:
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
                    self.safe_convert_to_int(data.loc['Total Revenue', col] if 'Total Revenue' in data.index else None),
                    self.safe_convert_to_int(data.loc['Cost Of Revenue', col] if 'Cost Of Revenue' in data.index else None),
                    self.safe_convert_to_int(data.loc['Gross Profit', col] if 'Gross Profit' in data.index else None),
                    self.safe_convert_to_int(data.loc['Research Development', col] if 'Research Development' in data.index else None),
                    self.safe_convert_to_int(data.loc['Selling General Administrative', col] if 'Selling General Administrative' in data.index else None),
                    self.safe_convert_to_int(data.loc['Total Operating Expenses', col] if 'Total Operating Expenses' in data.index else None),
                    self.safe_convert_to_int(data.loc['Operating Income', col] if 'Operating Income' in data.index else None),
                    self.safe_convert_to_int(data.loc['EBIT', col] if 'EBIT' in data.index else None),
                    self.safe_convert_to_int(data.loc['EBITDA', col] if 'EBITDA' in data.index else None),
                    self.safe_convert_to_int(data.loc['Interest Income', col] if 'Interest Income' in data.index else None),
                    self.safe_convert_to_int(data.loc['Interest Expense', col] if 'Interest Expense' in data.index else None),
                    self.safe_convert_to_int(data.loc['Other Income Expense Net', col] if 'Other Income Expense Net' in data.index else None),
                    self.safe_convert_to_int(data.loc['Income Before Tax', col] if 'Income Before Tax' in data.index else None),
                    self.safe_convert_to_int(data.loc['Tax Provision', col] if 'Tax Provision' in data.index else None),
                    self.safe_convert_to_int(data.loc['Net Income', col] if 'Net Income' in data.index else None),
                    self.safe_convert_to_int(data.loc['Net Income Common Stockholders', col] if 'Net Income Common Stockholders' in data.index else None),
                    self.safe_convert_to_float(data.loc['Diluted EPS', col] if 'Diluted EPS' in data.index else None),
                    self.safe_convert_to_float(data.loc['Basic EPS', col] if 'Basic EPS' in data.index else None),
                    self.safe_convert_to_int(data.loc['Diluted Average Shares', col] if 'Diluted Average Shares' in data.index else None),
                    self.safe_convert_to_int(data.loc['Basic Average Shares', col] if 'Basic Average Shares' in data.index else None),
                    self.safe_convert_to_int(data.loc['Operating Expense', col] if 'Operating Expense' in data.index else None),
                    self.safe_convert_to_int(data.loc['Normalized Income', col] if 'Normalized Income' in data.index else None),
                    self.safe_convert_to_int(data.loc['Total Expenses', col] if 'Total Expenses' in data.index else None)
                ))
                records_inserted += 1
            except Exception as e:
                self.logger.error(f"Error inserting income statement record for period {period_ending}: {e}")

        self.logger.info(f"Inserted {records_inserted} income statement records ({period_type})")
        cursor.close()

    def store_balance_sheets(self, company_id: int, data: pd.DataFrame, period_type: str):
        """Store balance sheet data"""
        cursor = self.conn.cursor()

        # Clear existing data
        cursor.execute(
            "DELETE FROM balance_sheets WHERE company_id = %s AND period_type = %s",
            (company_id, period_type)
        )

        for col in data.columns:
            period_ending = self.safe_convert_to_date(col)
            if not period_ending:
                continue

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
                self.safe_convert_to_int(data.loc['Total Assets', col] if 'Total Assets' in data.index else None),
                self.safe_convert_to_int(data.loc['Current Assets', col] if 'Current Assets' in data.index else None),
                self.safe_convert_to_int(data.loc['Cash And Cash Equivalents', col] if 'Cash And Cash Equivalents' in data.index else None),
                self.safe_convert_to_int(data.loc['Cash Cash Equivalents And Short Term Investments', col] if 'Cash Cash Equivalents And Short Term Investments' in data.index else None),
                self.safe_convert_to_int(data.loc['Other Short Term Investments', col] if 'Other Short Term Investments' in data.index else None),
                self.safe_convert_to_int(data.loc['Accounts Receivable', col] if 'Accounts Receivable' in data.index else None),
                self.safe_convert_to_int(data.loc['Inventory', col] if 'Inventory' in data.index else None),
                self.safe_convert_to_int(data.loc['Prepaid Assets', col] if 'Prepaid Assets' in data.index else None),
                self.safe_convert_to_int(data.loc['Other Current Assets', col] if 'Other Current Assets' in data.index else None),
                self.safe_convert_to_int(data.loc['Non Current Assets', col] if 'Non Current Assets' in data.index else None),
                self.safe_convert_to_int(data.loc['Net PPE', col] if 'Net PPE' in data.index else None),
                self.safe_convert_to_int(data.loc['Goodwill', col] if 'Goodwill' in data.index else None),
                self.safe_convert_to_int(data.loc['Other Intangible Assets', col] if 'Other Intangible Assets' in data.index else None),
                self.safe_convert_to_int(data.loc['Investments And Advances', col] if 'Investments And Advances' in data.index else None),
                self.safe_convert_to_int(data.loc['Other Non Current Assets', col] if 'Other Non Current Assets' in data.index else None),
                self.safe_convert_to_int(data.loc['Total Liabilities Net Minority Interest', col] if 'Total Liabilities Net Minority Interest' in data.index else None),
                self.safe_convert_to_int(data.loc['Current Liabilities', col] if 'Current Liabilities' in data.index else None),
                self.safe_convert_to_int(data.loc['Accounts Payable', col] if 'Accounts Payable' in data.index else None),
                self.safe_convert_to_int(data.loc['Accrued Liabilities', col] if 'Accrued Liabilities' in data.index else None),
                self.safe_convert_to_int(data.loc['Short Term Debt', col] if 'Short Term Debt' in data.index else None),
                self.safe_convert_to_int(data.loc['Current Debt And Capital Lease Obligation', col] if 'Current Debt And Capital Lease Obligation' in data.index else None),
                self.safe_convert_to_int(data.loc['Other Current Liabilities', col] if 'Other Current Liabilities' in data.index else None),
                self.safe_convert_to_int(data.loc['Non Current Liabilities', col] if 'Non Current Liabilities' in data.index else None),
                self.safe_convert_to_int(data.loc['Long Term Debt', col] if 'Long Term Debt' in data.index else None),
                self.safe_convert_to_int(data.loc['Long Term Debt And Capital Lease Obligation', col] if 'Long Term Debt And Capital Lease Obligation' in data.index else None),
                self.safe_convert_to_int(data.loc['Other Non Current Liabilities', col] if 'Other Non Current Liabilities' in data.index else None),
                self.safe_convert_to_int(data.loc['Total Debt', col] if 'Total Debt' in data.index else None),
                self.safe_convert_to_int(data.loc['Stockholders Equity', col] if 'Stockholders Equity' in data.index else None),
                self.safe_convert_to_int(data.loc['Retained Earnings', col] if 'Retained Earnings' in data.index else None),
                self.safe_convert_to_int(data.loc['Common Stock', col] if 'Common Stock' in data.index else None),
                self.safe_convert_to_int(data.loc['Capital Stock', col] if 'Capital Stock' in data.index else None),
                self.safe_convert_to_int(data.loc['Additional Paid In Capital', col] if 'Additional Paid In Capital' in data.index else None),
                self.safe_convert_to_int(data.loc['Treasury Shares Number', col] if 'Treasury Shares Number' in data.index else None),
                self.safe_convert_to_int(data.loc['Treasury Stock', col] if 'Treasury Stock' in data.index else None),
                self.safe_convert_to_int(data.loc['Accumulated Other Comprehensive Income', col] if 'Accumulated Other Comprehensive Income' in data.index else None),
                self.safe_convert_to_int(data.loc['Working Capital', col] if 'Working Capital' in data.index else None),
                self.safe_convert_to_int(data.loc['Total Equity Gross Minority Interest', col] if 'Total Equity Gross Minority Interest' in data.index else None),
                self.safe_convert_to_int(data.loc['Minority Interest', col] if 'Minority Interest' in data.index else None),
                self.safe_convert_to_int(data.loc['Total Capitalization', col] if 'Total Capitalization' in data.index else None),
                self.safe_convert_to_int(data.loc['Common Stock Equity', col] if 'Common Stock Equity' in data.index else None),
                self.safe_convert_to_int(data.loc['Net Tangible Assets', col] if 'Net Tangible Assets' in data.index else None)
            ))

        cursor.close()

    def store_cash_flow_statements(self, company_id: int, data: pd.DataFrame, period_type: str):
        """Store cash flow statement data"""
        cursor = self.conn.cursor()

        # Clear existing data
        cursor.execute(
            "DELETE FROM cash_flow_statements WHERE company_id = %s AND period_type = %s",
            (company_id, period_type)
        )

        for col in data.columns:
            period_ending = self.safe_convert_to_date(col)
            if not period_ending:
                continue

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
                self.safe_convert_to_int(data.loc['Operating Cash Flow', col] if 'Operating Cash Flow' in data.index else None),
                self.safe_convert_to_int(data.loc['Investing Cash Flow', col] if 'Investing Cash Flow' in data.index else None),
                self.safe_convert_to_int(data.loc['Financing Cash Flow', col] if 'Financing Cash Flow' in data.index else None),
                self.safe_convert_to_int(data.loc['Free Cash Flow', col] if 'Free Cash Flow' in data.index else None),
                self.safe_convert_to_int(data.loc['Capital Expenditure', col] if 'Capital Expenditure' in data.index else None),
                self.safe_convert_to_int(data.loc['Net Income', col] if 'Net Income' in data.index else None),
                self.safe_convert_to_int(data.loc['Net Income From Continuing Operations', col] if 'Net Income From Continuing Operations' in data.index else None),
                self.safe_convert_to_int(data.loc['Depreciation Depletion And Amortization', col] if 'Depreciation Depletion And Amortization' in data.index else None),
                self.safe_convert_to_int(data.loc['Depreciation And Amortization', col] if 'Depreciation And Amortization' in data.index else None),
                self.safe_convert_to_int(data.loc['Deferred Income Tax', col] if 'Deferred Income Tax' in data.index else None),
                self.safe_convert_to_int(data.loc['Stock Based Compensation', col] if 'Stock Based Compensation' in data.index else None),
                self.safe_convert_to_int(data.loc['Change In Working Capital', col] if 'Change In Working Capital' in data.index else None),
                self.safe_convert_to_int(data.loc['Change In Accounts Receivable', col] if 'Change In Accounts Receivable' in data.index else None),
                self.safe_convert_to_int(data.loc['Change In Inventory', col] if 'Change In Inventory' in data.index else None),
                self.safe_convert_to_int(data.loc['Change In Accounts Payable', col] if 'Change In Accounts Payable' in data.index else None),
                self.safe_convert_to_int(data.loc['Change In Other Working Capital', col] if 'Change In Other Working Capital' in data.index else None),
                self.safe_convert_to_int(data.loc['Other Non Cash Items', col] if 'Other Non Cash Items' in data.index else None),
                self.safe_convert_to_int(data.loc['Investments In Property Plant And Equipment', col] if 'Investments In Property Plant And Equipment' in data.index else None),
                self.safe_convert_to_int(data.loc['Acquisitions Net', col] if 'Acquisitions Net' in data.index else None),
                self.safe_convert_to_int(data.loc['Purchases Of Investments', col] if 'Purchases Of Investments' in data.index else None),
                self.safe_convert_to_int(data.loc['Sales Maturities Of Investments', col] if 'Sales Maturities Of Investments' in data.index else None),
                self.safe_convert_to_int(data.loc['Other Investing Activities', col] if 'Other Investing Activities' in data.index else None),
                self.safe_convert_to_int(data.loc['Issuance Of Debt', col] if 'Issuance Of Debt' in data.index else None),
                self.safe_convert_to_int(data.loc['Repayment Of Debt', col] if 'Repayment Of Debt' in data.index else None),
                self.safe_convert_to_int(data.loc['Repurchase Of Capital Stock', col] if 'Repurchase Of Capital Stock' in data.index else None),
                self.safe_convert_to_int(data.loc['Cash Dividends Paid', col] if 'Cash Dividends Paid' in data.index else None),
                self.safe_convert_to_int(data.loc['Other Financing Activities', col] if 'Other Financing Activities' in data.index else None),
                self.safe_convert_to_int(data.loc['Effect Of Exchange Rate Changes', col] if 'Effect Of Exchange Rate Changes' in data.index else None),
                self.safe_convert_to_int(data.loc['End Cash Position', col] if 'End Cash Position' in data.index else None),
                self.safe_convert_to_int(data.loc['Beginning Cash Position', col] if 'Beginning Cash Position' in data.index else None),
                self.safe_convert_to_int(data.loc['Changes In Cash', col] if 'Changes In Cash' in data.index else None),
                self.safe_convert_to_int(data.loc['Financing Cash Flow', col] if 'Financing Cash Flow' in data.index else None)
            ))

        cursor.close()

    def download_and_store_corporate_actions(self, ticker, company_id: int, symbol: str):
        """Download and store corporate actions"""
        try:
            # Get dividends
            dividends = ticker.dividends
            if not dividends.empty:
                cursor = self.conn.cursor()
                cursor.execute("DELETE FROM corporate_actions WHERE company_id = %s AND action_type = 'dividend'", (company_id,))

                for date_idx, amount in dividends.items():
                    cursor.execute("""
                        INSERT INTO corporate_actions (company_id, action_date, action_type, amount)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (company_id, action_date, action_type) DO NOTHING
                    """, (company_id, date_idx.date(), 'dividend', float(amount)))

                cursor.close()

            # Get stock splits
            splits = ticker.splits
            if not splits.empty:
                cursor = self.conn.cursor()
                cursor.execute("DELETE FROM corporate_actions WHERE company_id = %s AND action_type = 'stock_split'", (company_id,))

                for date_idx, ratio in splits.items():
                    cursor.execute("""
                        INSERT INTO corporate_actions (company_id, action_date, action_type, amount)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (company_id, action_date, action_type) DO NOTHING
                    """, (company_id, date_idx.date(), 'stock_split', float(ratio)))

                cursor.close()

        except Exception as e:
            self.logger.warning(f"Error downloading corporate actions for {symbol}: {e}")

    def download_all_nse_stocks(self, symbols_file: str = "nse_complete_universe.txt"):
        """Download data for all NSE stocks"""
        try:
            self.connect_db()

            # Create database schema
            if not self.create_database_schema():
                self.logger.error("Failed to create database schema")
                return False

            # Get symbols
            if os.path.exists(symbols_file):
                with open(symbols_file, 'r') as f:
                    symbols = [line.strip() for line in f.readlines() if line.strip()]
                self.logger.info(f"Loaded {len(symbols)} symbols from existing file")
            else:
                self.logger.info("Getting ALL NSE symbols (complete universe)...")
                fetcher = NSESymbolsFetcher()
                symbols = fetcher.get_all_nse_symbols(complete_universe=True)
                fetcher.save_symbols_to_file(symbols, symbols_file)
                self.logger.info(f"Generated new symbols file with {len(symbols)} NSE stocks")

            self.logger.info(f"🚀 Starting download for {len(symbols)} NSE stocks")
            self.logger.info(f"📊 Processing in batches of {self.batch_size} stocks")
            self.logger.info(f"⏱️ Estimated completion time: {(len(symbols) * self.delay_between_stocks + (len(symbols)//self.batch_size) * self.delay_between_batches) / 60:.1f} minutes")

            # Process stocks in batches
            successful_downloads = 0
            failed_downloads = 0
            total_batches = (len(symbols) + self.batch_size - 1) // self.batch_size

            for i in range(0, len(symbols), self.batch_size):
                batch = symbols[i:i + self.batch_size]
                batch_num = i//self.batch_size + 1

                self.logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch)} stocks): Progress {successful_downloads + failed_downloads}/{len(symbols)}")

                batch_start_time = time.time()
                batch_successful = 0

                for j, symbol in enumerate(batch):
                    try:
                        self.logger.info(f"  📈 [{batch_num}.{j+1}] Processing {symbol}")
                        if self.download_and_store_company_data(symbol):
                            successful_downloads += 1
                            batch_successful += 1
                        else:
                            failed_downloads += 1

                        time.sleep(self.delay_between_stocks)  # Rate limiting

                    except Exception as e:
                        self.logger.error(f"❌ Error processing {symbol}: {e}")
                        failed_downloads += 1
                        continue

                batch_time = time.time() - batch_start_time
                self.logger.info(f"✅ Batch {batch_num} completed: {batch_successful}/{len(batch)} successful in {batch_time:.1f}s")
                self.logger.info(f"📊 Overall progress: {successful_downloads} successful, {failed_downloads} failed, {len(symbols) - successful_downloads - failed_downloads} remaining")

                # Delay between batches
                if i + self.batch_size < len(symbols):
                    self.logger.info(f"⏸️ Waiting {self.delay_between_batches}s before next batch...")
                    time.sleep(self.delay_between_batches)

            self.logger.info(f"Download completed! Successful: {successful_downloads}, Failed: {failed_downloads}")

            # Print summary
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM companies;")
            companies_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM price_history;")
            price_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM income_statements;")
            income_count = cursor.fetchone()[0]

            cursor.close()

            self.logger.info(f"Database Summary:")
            self.logger.info(f"  Companies: {companies_count}")
            self.logger.info(f"  Price records: {price_count}")
            self.logger.info(f"  Income statements: {income_count}")

            return True

        except Exception as e:
            self.logger.error(f"Error in download_all_nse_stocks: {e}")
            return False
        finally:
            self.close_db()

    
    def store_financial_statements(self, company_id: int, data: pd.DataFrame, period_type: str):
        """Store income statement data"""
        cursor = self.conn.cursor()

        # Clear existing data
        cursor.execute(
            "DELETE FROM income_statements WHERE company_id = %s AND period_type = %s",
            (company_id, period_type)
        )

        records_inserted = 0
        for col in data.columns:
            period_ending = self.safe_convert_to_date(col)
            if not period_ending:
                continue

            try:
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
                    self.safe_convert_to_int(data.loc['Total Revenue', col] if 'Total Revenue' in data.index else None),
                    self.safe_convert_to_int(data.loc['Cost Of Revenue', col] if 'Cost Of Revenue' in data.index else None),
                    self.safe_convert_to_int(data.loc['Gross Profit', col] if 'Gross Profit' in data.index else None),
                    self.safe_convert_to_int(data.loc['Research Development', col] if 'Research Development' in data.index else None),
                    self.safe_convert_to_int(data.loc['Selling General Administrative', col] if 'Selling General Administrative' in data.index else None),
                    self.safe_convert_to_int(data.loc['Total Operating Expenses', col] if 'Total Operating Expenses' in data.index else None),
                    self.safe_convert_to_int(data.loc['Operating Income', col] if 'Operating Income' in data.index else None),
                    self.safe_convert_to_int(data.loc['EBIT', col] if 'EBIT' in data.index else None),
                    self.safe_convert_to_int(data.loc['EBITDA', col] if 'EBITDA' in data.index else None),
                    self.safe_convert_to_int(data.loc['Interest Income', col] if 'Interest Income' in data.index else None),
                    self.safe_convert_to_int(data.loc['Interest Expense', col] if 'Interest Expense' in data.index else None),
                    self.safe_convert_to_int(data.loc['Other Income Expense Net', col] if 'Other Income Expense Net' in data.index else None),
                    self.safe_convert_to_int(data.loc['Income Before Tax', col] if 'Income Before Tax' in data.index else None),
                    self.safe_convert_to_int(data.loc['Tax Provision', col] if 'Tax Provision' in data.index else None),
                    self.safe_convert_to_int(data.loc['Net Income', col] if 'Net Income' in data.index else None),
                    self.safe_convert_to_int(data.loc['Net Income Common Stockholders', col] if 'Net Income Common Stockholders' in data.index else None),
                    self.safe_convert_to_float(data.loc['Diluted EPS', col] if 'Diluted EPS' in data.index else None),
                    self.safe_convert_to_float(data.loc['Basic EPS', col] if 'Basic EPS' in data.index else None),
                    self.safe_convert_to_int(data.loc['Diluted Average Shares', col] if 'Diluted Average Shares' in data.index else None),
                    self.safe_convert_to_int(data.loc['Basic Average Shares', col] if 'Basic Average Shares' in data.index else None),
                    self.safe_convert_to_int(data.loc['Operating Expense', col] if 'Operating Expense' in data.index else None),
                    self.safe_convert_to_int(data.loc['Normalized Income', col] if 'Normalized Income' in data.index else None),
                    self.safe_convert_to_int(data.loc['Total Expenses', col] if 'Total Expenses' in data.index else None)
                ))
                records_inserted += 1
            except Exception as e:
                self.logger.error(f"Error inserting income statement record for period {period_ending}: {e}")

        self.logger.info(f"Inserted {records_inserted} income statement records ({period_type})")
        cursor.close()


    def store_balance_sheets(self, company_id: int, data: pd.DataFrame, period_type: str):
        """Store balance sheet data"""
        cursor = self.conn.cursor()

        # Clear existing data
        cursor.execute(
            "DELETE FROM balance_sheets WHERE company_id = %s AND period_type = %s",
            (company_id, period_type)
        )

        for col in data.columns:
            period_ending = self.safe_convert_to_date(col)
            if not period_ending:
                continue

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
                self.safe_convert_to_int(data.loc['Total Assets', col] if 'Total Assets' in data.index else None),
                self.safe_convert_to_int(data.loc['Current Assets', col] if 'Current Assets' in data.index else None),
                self.safe_convert_to_int(data.loc['Cash And Cash Equivalents', col] if 'Cash And Cash Equivalents' in data.index else None),
                self.safe_convert_to_int(data.loc['Cash Cash Equivalents And Short Term Investments', col] if 'Cash Cash Equivalents And Short Term Investments' in data.index else None),
                self.safe_convert_to_int(data.loc['Other Short Term Investments', col] if 'Other Short Term Investments' in data.index else None),
                self.safe_convert_to_int(data.loc['Accounts Receivable', col] if 'Accounts Receivable' in data.index else None),
                self.safe_convert_to_int(data.loc['Inventory', col] if 'Inventory' in data.index else None),
                self.safe_convert_to_int(data.loc['Prepaid Assets', col] if 'Prepaid Assets' in data.index else None),
                self.safe_convert_to_int(data.loc['Other Current Assets', col] if 'Other Current Assets' in data.index else None),
                self.safe_convert_to_int(data.loc['Non Current Assets', col] if 'Non Current Assets' in data.index else None),
                self.safe_convert_to_int(data.loc['Net PPE', col] if 'Net PPE' in data.index else None),
                self.safe_convert_to_int(data.loc['Goodwill', col] if 'Goodwill' in data.index else None),
                self.safe_convert_to_int(data.loc['Other Intangible Assets', col] if 'Other Intangible Assets' in data.index else None),
                self.safe_convert_to_int(data.loc['Investments And Advances', col] if 'Investments And Advances' in data.index else None),
                self.safe_convert_to_int(data.loc['Other Non Current Assets', col] if 'Other Non Current Assets' in data.index else None),
                self.safe_convert_to_int(data.loc['Total Liabilities Net Minority Interest', col] if 'Total Liabilities Net Minority Interest' in data.index else None),
                self.safe_convert_to_int(data.loc['Current Liabilities', col] if 'Current Liabilities' in data.index else None),
                self.safe_convert_to_int(data.loc['Accounts Payable', col] if 'Accounts Payable' in data.index else None),
                self.safe_convert_to_int(data.loc['Accrued Liabilities', col] if 'Accrued Liabilities' in data.index else None),
                self.safe_convert_to_int(data.loc['Short Term Debt', col] if 'Short Term Debt' in data.index else None),
                self.safe_convert_to_int(data.loc['Current Debt And Capital Lease Obligation', col] if 'Current Debt And Capital Lease Obligation' in data.index else None),
                self.safe_convert_to_int(data.loc['Other Current Liabilities', col] if 'Other Current Liabilities' in data.index else None),
                self.safe_convert_to_int(data.loc['Non Current Liabilities', col] if 'Non Current Liabilities' in data.index else None),
                self.safe_convert_to_int(data.loc['Long Term Debt', col] if 'Long Term Debt' in data.index else None),
                self.safe_convert_to_int(data.loc['Long Term Debt And Capital Lease Obligation', col] if 'Long Term Debt And Capital Lease Obligation' in data.index else None),
                self.safe_convert_to_int(data.loc['Other Non Current Liabilities', col] if 'Other Non Current Liabilities' in data.index else None),
                self.safe_convert_to_int(data.loc['Total Debt', col] if 'Total Debt' in data.index else None),
                self.safe_convert_to_int(data.loc['Stockholders Equity', col] if 'Stockholders Equity' in data.index else None),
                self.safe_convert_to_int(data.loc['Retained Earnings', col] if 'Retained Earnings' in data.index else None),
                self.safe_convert_to_int(data.loc['Common Stock', col] if 'Common Stock' in data.index else None),
                self.safe_convert_to_int(data.loc['Capital Stock', col] if 'Capital Stock' in data.index else None),
                self.safe_convert_to_int(data.loc['Additional Paid In Capital', col] if 'Additional Paid In Capital' in data.index else None),
                self.safe_convert_to_int(data.loc['Treasury Shares Number', col] if 'Treasury Shares Number' in data.index else None),
                self.safe_convert_to_int(data.loc['Treasury Stock', col] if 'Treasury Stock' in data.index else None),
                self.safe_convert_to_int(data.loc['Accumulated Other Comprehensive Income', col] if 'Accumulated Other Comprehensive Income' in data.index else None),
                self.safe_convert_to_int(data.loc['Working Capital', col] if 'Working Capital' in data.index else None),
                self.safe_convert_to_int(data.loc['Total Equity Gross Minority Interest', col] if 'Total Equity Gross Minority Interest' in data.index else None),
                self.safe_convert_to_int(data.loc['Minority Interest', col] if 'Minority Interest' in data.index else None),
                self.safe_convert_to_int(data.loc['Total Capitalization', col] if 'Total Capitalization' in data.index else None),
                self.safe_convert_to_int(data.loc['Common Stock Equity', col] if 'Common Stock Equity' in data.index else None),
                self.safe_convert_to_int(data.loc['Net Tangible Assets', col] if 'Net Tangible Assets' in data.index else None)
            ))

        cursor.close()

    def store_cash_flow_statements(self, company_id: int, data: pd.DataFrame, period_type: str):
        """Store cash flow statement data"""
        cursor = self.conn.cursor()

        # Clear existing data
        cursor.execute(
            "DELETE FROM cash_flow_statements WHERE company_id = %s AND period_type = %s",
            (company_id, period_type)
        )

        for col in data.columns:
            period_ending = self.safe_convert_to_date(col)
            if not period_ending:
                continue

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
                self.safe_convert_to_int(data.loc['Operating Cash Flow', col] if 'Operating Cash Flow' in data.index else None),
                self.safe_convert_to_int(data.loc['Investing Cash Flow', col] if 'Investing Cash Flow' in data.index else None),
                self.safe_convert_to_int(data.loc['Financing Cash Flow', col] if 'Financing Cash Flow' in data.index else None),
                self.safe_convert_to_int(data.loc['Free Cash Flow', col] if 'Free Cash Flow' in data.index else None),
                self.safe_convert_to_int(data.loc['Capital Expenditure', col] if 'Capital Expenditure' in data.index else None),
                self.safe_convert_to_int(data.loc['Net Income', col] if 'Net Income' in data.index else None),
                self.safe_convert_to_int(data.loc['Net Income From Continuing Operations', col] if 'Net Income From Continuing Operations' in data.index else None),
                self.safe_convert_to_int(data.loc['Depreciation Depletion And Amortization', col] if 'Depreciation Depletion And Amortization' in data.index else None),
                self.safe_convert_to_int(data.loc['Depreciation And Amortization', col] if 'Depreciation And Amortization' in data.index else None),
                self.safe_convert_to_int(data.loc['Deferred Income Tax', col] if 'Deferred Income Tax' in data.index else None),
                self.safe_convert_to_int(data.loc['Stock Based Compensation', col] if 'Stock Based Compensation' in data.index else None),
                self.safe_convert_to_int(data.loc['Change In Working Capital', col] if 'Change In Working Capital' in data.index else None),
                self.safe_convert_to_int(data.loc['Change In Accounts Receivable', col] if 'Change In Accounts Receivable' in data.index else None),
                self.safe_convert_to_int(data.loc['Change In Inventory', col] if 'Change In Inventory' in data.index else None),
                self.safe_convert_to_int(data.loc['Change In Accounts Payable', col] if 'Change In Accounts Payable' in data.index else None),
                self.safe_convert_to_int(data.loc['Change In Other Working Capital', col] if 'Change In Other Working Capital' in data.index else None),
                self.safe_convert_to_int(data.loc['Other Non Cash Items', col] if 'Other Non Cash Items' in data.index else None),
                self.safe_convert_to_int(data.loc['Investments In Property Plant And Equipment', col] if 'Investments In Property Plant And Equipment' in data.index else None),
                self.safe_convert_to_int(data.loc['Acquisitions Net', col] if 'Acquisitions Net' in data.index else None),
                self.safe_convert_to_int(data.loc['Purchases Of Investments', col] if 'Purchases Of Investments' in data.index else None),
                self.safe_convert_to_int(data.loc['Sales Maturities Of Investments', col] if 'Sales Maturities Of Investments' in data.index else None),
                self.safe_convert_to_int(data.loc['Other Investing Activities', col] if 'Other Investing Activities' in data.index else None),
                self.safe_convert_to_int(data.loc['Issuance Of Debt', col] if 'Issuance Of Debt' in data.index else None),
                self.safe_convert_to_int(data.loc['Repayment Of Debt', col] if 'Repayment Of Debt' in data.index else None),
                self.safe_convert_to_int(data.loc['Repurchase Of Capital Stock', col] if 'Repurchase Of Capital Stock' in data.index else None),
                self.safe_convert_to_int(data.loc['Cash Dividends Paid', col] if 'Cash Dividends Paid' in data.index else None),
                self.safe_convert_to_int(data.loc['Other Financing Activities', col] if 'Other Financing Activities' in data.index else None),
                self.safe_convert_to_int(data.loc['Effect Of Exchange Rate Changes', col] if 'Effect Of Exchange Rate Changes' in data.index else None),
                self.safe_convert_to_int(data.loc['End Cash Position', col] if 'End Cash Position' in data.index else None),
                self.safe_convert_to_int(data.loc['Beginning Cash Position', col] if 'Beginning Cash Position' in data.index else None),
                self.safe_convert_to_int(data.loc['Changes In Cash', col] if 'Changes In Cash' in data.index else None),
                self.safe_convert_to_int(data.loc['Financing Cash Flow', col] if 'Financing Cash Flow' in data.index else None)
            ))

        cursor.close()

    def download_and_store_corporate_actions(self, ticker, company_id: int, symbol: str):
        """Download and store corporate actions"""
        try:
            # Get dividends
            dividends = ticker.dividends
            if not dividends.empty:
                cursor = self.conn.cursor()
                cursor.execute("DELETE FROM corporate_actions WHERE company_id = %s AND action_type = 'dividend'", (company_id,))

                for date_idx, amount in dividends.items():
                    cursor.execute("""
                        INSERT INTO corporate_actions (company_id, action_date, action_type, amount)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (company_id, action_date, action_type) DO NOTHING
                    """, (company_id, date_idx.date(), 'dividend', float(amount)))

                cursor.close()

            # Get stock splits
            splits = ticker.splits
            if not splits.empty:
                cursor = self.conn.cursor()
                cursor.execute("DELETE FROM corporate_actions WHERE company_id = %s AND action_type = 'stock_split'", (company_id,))

                for date_idx, ratio in splits.items():
                    cursor.execute("""
                        INSERT INTO corporate_actions (company_id, action_date, action_type, amount)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (company_id, action_date, action_type) DO NOTHING
                    """, (company_id, date_idx.date(), 'stock_split', float(ratio)))

                cursor.close()

        except Exception as e:
            self.logger.warning(f"Error downloading corporate actions for {symbol}: {e}")

    def download_all_nse_stocks(self, symbols_file: str = "nse_complete_universe.txt"):
        """Download data for all NSE stocks"""
        try:
            self.connect_db()

            # Create database schema
            if not self.create_database_schema():
                self.logger.error("Failed to create database schema")
                return False

            # Get symbols
            if os.path.exists(symbols_file):
                with open(symbols_file, 'r') as f:
                    symbols = [line.strip() for line in f.readlines() if line.strip()]
                self.logger.info(f"Loaded {len(symbols)} symbols from existing file")
            else:
                self.logger.info("Getting ALL NSE symbols (complete universe)...")
                fetcher = NSESymbolsFetcher()
                symbols = fetcher.get_all_nse_symbols(complete_universe=True)
                fetcher.save_symbols_to_file(symbols, symbols_file)
                self.logger.info(f"Generated new symbols file with {len(symbols)} NSE stocks")

            self.logger.info(f"🚀 Starting download for {len(symbols)} NSE stocks")
            self.logger.info(f"📊 Processing in batches of {self.batch_size} stocks")
            self.logger.info(f"⏱️ Estimated completion time: {(len(symbols) * self.delay_between_stocks + (len(symbols)//self.batch_size) * self.delay_between_batches) / 60:.1f} minutes")

            # Process stocks in batches
            successful_downloads = 0
            failed_downloads = 0
            total_batches = (len(symbols) + self.batch_size - 1) // self.batch_size

            for i in range(0, len(symbols), self.batch_size):
                batch = symbols[i:i + self.batch_size]
                batch_num = i//self.batch_size + 1

                self.logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch)} stocks): Progress {successful_downloads + failed_downloads}/{len(symbols)}")

                batch_start_time = time.time()
                batch_successful = 0

                for j, symbol in enumerate(batch):
                    try:
                        self.logger.info(f"  📈 [{batch_num}.{j+1}] Processing {symbol}")
                        if self.download_and_store_company_data(symbol):
                            successful_downloads += 1
                            batch_successful += 1
                        else:
                            failed_downloads += 1

                        time.sleep(self.delay_between_stocks)  # Rate limiting

                    except Exception as e:
                        self.logger.error(f"❌ Error processing {symbol}: {e}")
                        failed_downloads += 1
                        continue

                batch_time = time.time() - batch_start_time
                self.logger.info(f"✅ Batch {batch_num} completed: {batch_successful}/{len(batch)} successful in {batch_time:.1f}s")
                self.logger.info(f"📊 Overall progress: {successful_downloads} successful, {failed_downloads} failed, {len(symbols) - successful_downloads - failed_downloads} remaining")

                # Delay between batches
                if i + self.batch_size < len(symbols):
                    self.logger.info(f"⏸️ Waiting {self.delay_between_batches}s before next batch...")
                    time.sleep(self.delay_between_batches)

            self.logger.info(f"Download completed! Successful: {successful_downloads}, Failed: {failed_downloads}")

            # Print summary
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM companies;")
            companies_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM price_history;")
            price_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM income_statements;")
            income_count = cursor.fetchone()[0]

            cursor.close()

            self.logger.info(f"Database Summary:")
            self.logger.info(f"  Companies: {companies_count}")
            self.logger.info(f"  Price records: {price_count}")
            self.logger.info(f"  Income statements: {income_count}")

            return True

        except Exception as e:
            self.logger.error(f"Error in download_all_nse_stocks: {e}")
            return False
        finally:
            self.close_db()

if __name__ == "__main__":
    downloader = YFinanceNSEDownloader()
    downloader.download_all_nse_stocks()