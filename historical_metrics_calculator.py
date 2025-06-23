import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, date, timedelta
import logging
import yfinance as yf
from database_config import get_db_connection
from typing import Dict, List, Optional, Tuple

class HistoricalMetricsCalculator:
    def __init__(self):
        """Initialize the historical metrics calculator"""
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
            self.conn = get_db_connection()
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

    def safe_get(self, data_dict, key, default=None):
        """Safely get value from dictionary"""
        if data_dict is None:
            return default
        return data_dict.get(key, default)

    def fetch_yfinance_metrics(self, symbol: str) -> Dict:
        """Fetch current metrics directly from yfinance"""
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info

            metrics = {}

            # Market Data
            metrics['market_cap'] = self.safe_get(info, 'marketCap')
            metrics['shares_outstanding'] = self.safe_get(info, 'sharesOutstanding')
            metrics['float_shares'] = self.safe_get(info, 'floatShares')
            metrics['current_price'] = self.safe_get(info, 'currentPrice') or self.safe_get(info, 'regularMarketPrice')

            # Valuation Metrics
            metrics['trailing_pe'] = self.safe_get(info, 'trailingPE')
            metrics['forward_pe'] = self.safe_get(info, 'forwardPE')
            metrics['price_to_book'] = self.safe_get(info, 'priceToBook')
            metrics['price_to_sales'] = self.safe_get(info, 'priceToSalesTrailing12Months')
            metrics['ev_to_ebitda'] = self.safe_get(info, 'enterpriseToEbitda')

            # Profitability Metrics
            metrics['gross_margin'] = self.safe_get(info, 'grossMargins')
            metrics['operating_margin'] = self.safe_get(info, 'operatingMargins')
            metrics['profit_margin'] = self.safe_get(info, 'profitMargins')
            metrics['return_on_assets'] = self.safe_get(info, 'returnOnAssets')
            metrics['return_on_equity'] = self.safe_get(info, 'returnOnEquity')

            # Growth Metrics
            metrics['revenue_growth_yoy'] = self.safe_get(info, 'revenueGrowth')
            metrics['earnings_growth_yoy'] = self.safe_get(info, 'earningsGrowth')

            # Financial Health
            metrics['debt_to_equity'] = self.safe_get(info, 'debtToEquity')
            metrics['current_ratio'] = self.safe_get(info, 'currentRatio')
            metrics['quick_ratio'] = self.safe_get(info, 'quickRatio')

            # Cash Flow Metrics
            metrics['operating_cashflow'] = self.safe_get(info, 'operatingCashflow')
            metrics['free_cashflow'] = self.safe_get(info, 'freeCashflow')

            # Dividend Metrics
            metrics['dividend_yield'] = self.safe_get(info, 'dividendYield')
            metrics['dividend_rate'] = self.safe_get(info, 'dividendRate')
            metrics['payout_ratio'] = self.safe_get(info, 'payoutRatio')

            # Other Key Metrics
            metrics['book_value_per_share'] = self.safe_get(info, 'bookValue')
            metrics['beta'] = self.safe_get(info, 'beta')

            # Calculate FCF per share if we have the data
            if metrics['free_cashflow'] and metrics['shares_outstanding']:
                metrics['fcf_per_share'] = metrics['free_cashflow'] / metrics['shares_outstanding']

            return metrics

        except Exception as e:
            self.logger.error(f"Error fetching yfinance data for {symbol}: {e}")
            return {}

    def fetch_historical_financials(self, symbol: str, period_type: str = 'quarterly') -> List[Dict]:
        """Fetch historical financial data from yfinance"""
        try:
            ticker = yf.Ticker(symbol)

            if period_type == 'quarterly':
                income_stmt = ticker.quarterly_income_stmt
                balance_sheet = ticker.quarterly_balance_sheet
                cashflow = ticker.quarterly_cashflow
            else:  # annual
                income_stmt = ticker.income_stmt
                balance_sheet = ticker.balance_sheet
                cashflow = ticker.cashflow

            historical_data = []

            # Get all unique dates from financial statements
            all_dates = set()
            if not income_stmt.empty:
                all_dates.update(income_stmt.columns)
            if not balance_sheet.empty:
                all_dates.update(balance_sheet.columns)
            if not cashflow.empty:
                all_dates.update(cashflow.columns)

            for date_col in sorted(all_dates, reverse=True):
                metrics = {'period_date': date_col.date(), 'period_type': period_type}

                # Income statement metrics
                if not income_stmt.empty and date_col in income_stmt.columns:
                    income_data = income_stmt[date_col]
                    total_revenue = self.safe_get(income_data, 'Total Revenue')
                    net_income = self.safe_get(income_data, 'Net Income')
                    gross_profit = self.safe_get(income_data, 'Gross Profit')
                    operating_income = self.safe_get(income_data, 'Operating Income')

                    if total_revenue and total_revenue > 0:
                        if gross_profit:
                            metrics['gross_margin'] = gross_profit / total_revenue
                        if operating_income:
                            metrics['operating_margin'] = operating_income / total_revenue
                        if net_income:
                            metrics['profit_margin'] = net_income / total_revenue

                # Balance sheet metrics
                if not balance_sheet.empty and date_col in balance_sheet.columns:
                    balance_data = balance_sheet[date_col]
                    total_assets = self.safe_get(balance_data, 'Total Assets')
                    stockholders_equity = self.safe_get(balance_data, 'Stockholders Equity')
                    total_debt = self.safe_get(balance_data, 'Total Debt')
                    current_assets = self.safe_get(balance_data, 'Current Assets')
                    current_liabilities = self.safe_get(balance_data, 'Current Liabilities')

                    if stockholders_equity and stockholders_equity > 0:
                        if total_debt:
                            metrics['debt_to_equity'] = total_debt / stockholders_equity
                        if net_income:
                            metrics['return_on_equity'] = net_income / stockholders_equity

                    if total_assets and total_assets > 0 and net_income:
                        metrics['return_on_assets'] = net_income / total_assets

                    if current_liabilities and current_liabilities > 0 and current_assets:
                        metrics['current_ratio'] = current_assets / current_liabilities

                # Cash flow metrics
                if not cashflow.empty and date_col in cashflow.columns:
                    cf_data = cashflow[date_col]
                    metrics['operating_cashflow'] = self.safe_get(cf_data, 'Operating Cash Flow')
                    metrics['free_cashflow'] = self.safe_get(cf_data, 'Free Cash Flow')

                historical_data.append(metrics)

            return historical_data

        except Exception as e:
            self.logger.error(f"Error fetching historical financials for {symbol}: {e}")
            return []

    def store_current_metrics(self, company_id: int, symbol: str):
        """Store current metrics from yfinance"""
        metrics = self.fetch_yfinance_metrics(symbol)

        if not metrics:
            self.logger.warning(f"No current metrics found for {symbol}")
            return

        current_date = date.today()
        self.store_historical_metrics(company_id, current_date, 'current', metrics)
        self.logger.info(f"✓ Stored current metrics for {symbol}")

    def store_historical_metrics(self, company_id: int, metric_date: date, period_type: str, metrics: Dict):
        """Store calculated metrics in the historical_company_metrics table"""
        cursor = self.conn.cursor()

        insert_sql = """
        INSERT INTO historical_company_metrics (
            company_id, metric_date, period_type, market_cap, shares_outstanding, float_shares,
            current_price, trailing_pe, forward_pe, price_to_book, price_to_sales, ev_to_ebitda,
            gross_margin, operating_margin, profit_margin, return_on_assets, return_on_equity,
            revenue_growth_yoy, earnings_growth_yoy, debt_to_equity, current_ratio, quick_ratio,
            operating_cashflow, free_cashflow, fcf_per_share, dividend_yield, dividend_rate,
            payout_ratio, book_value_per_share, beta
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (company_id, metric_date, period_type) DO UPDATE SET
            market_cap = EXCLUDED.market_cap,
            shares_outstanding = EXCLUDED.shares_outstanding,
            float_shares = EXCLUDED.float_shares,
            current_price = EXCLUDED.current_price,
            trailing_pe = EXCLUDED.trailing_pe,
            forward_pe = EXCLUDED.forward_pe,
            price_to_book = EXCLUDED.price_to_book,
            price_to_sales = EXCLUDED.price_to_sales,
            ev_to_ebitda = EXCLUDED.ev_to_ebitda,
            gross_margin = EXCLUDED.gross_margin,
            operating_margin = EXCLUDED.operating_margin,
            profit_margin = EXCLUDED.profit_margin,
            return_on_assets = EXCLUDED.return_on_assets,
            return_on_equity = EXCLUDED.return_on_equity,
            revenue_growth_yoy = EXCLUDED.revenue_growth_yoy,
            earnings_growth_yoy = EXCLUDED.earnings_growth_yoy,
            debt_to_equity = EXCLUDED.debt_to_equity,
            current_ratio = EXCLUDED.current_ratio,
            quick_ratio = EXCLUDED.quick_ratio,
            operating_cashflow = EXCLUDED.operating_cashflow,
            free_cashflow = EXCLUDED.free_cashflow,
            fcf_per_share = EXCLUDED.fcf_per_share,
            dividend_yield = EXCLUDED.dividend_yield,
            dividend_rate = EXCLUDED.dividend_rate,
            payout_ratio = EXCLUDED.payout_ratio,
            book_value_per_share = EXCLUDED.book_value_per_share,
            beta = EXCLUDED.beta,
            updated_at = CURRENT_TIMESTAMP
        """

        cursor.execute(insert_sql, (
            company_id, metric_date, period_type,
            metrics.get('market_cap'),
            metrics.get('shares_outstanding'),
            metrics.get('float_shares'),
            metrics.get('current_price'),
            metrics.get('trailing_pe'),
            metrics.get('forward_pe'),
            metrics.get('price_to_book'),
            metrics.get('price_to_sales'),
            metrics.get('ev_to_ebitda'),
            metrics.get('gross_margin'),
            metrics.get('operating_margin'),
            metrics.get('profit_margin'),
            metrics.get('return_on_assets'),
            metrics.get('return_on_equity'),
            metrics.get('revenue_growth_yoy'),
            metrics.get('earnings_growth_yoy'),
            metrics.get('debt_to_equity'),
            metrics.get('current_ratio'),
            metrics.get('quick_ratio'),
            metrics.get('operating_cashflow'),
            metrics.get('free_cashflow'),
            metrics.get('fcf_per_share'),
            metrics.get('dividend_yield'),
            metrics.get('dividend_rate'),
            metrics.get('payout_ratio'),
            metrics.get('book_value_per_share'),
            metrics.get('beta')
        ))

        cursor.close()

    def populate_historical_metrics_for_company(self, company_id: int, symbol: str):
        """Populate historical metrics for a company using yfinance data"""
        self.logger.info(f"Populating historical metrics for {symbol}")

        try:
            # Store current metrics
            self.store_current_metrics(company_id, symbol)

            # Store historical quarterly metrics
            quarterly_data = self.fetch_historical_financials(symbol, 'quarterly')
            for metrics in quarterly_data:
                period_date = metrics.pop('period_date')
                period_type = metrics.pop('period_type')
                self.store_historical_metrics(company_id, period_date, period_type, metrics)
                self.logger.info(f"✓ Stored quarterly metrics for {symbol} on {period_date}")

            # Store historical annual metrics
            annual_data = self.fetch_historical_financials(symbol, 'annual')
            for metrics in annual_data:
                period_date = metrics.pop('period_date')
                period_type = metrics.pop('period_type')
                self.store_historical_metrics(company_id, period_date, period_type, metrics)
                self.logger.info(f"✓ Stored annual metrics for {symbol} on {period_date}")

            self.conn.commit()

        except Exception as e:
            self.logger.error(f"Error processing {symbol}: {e}")
            self.conn.rollback()

    def populate_all_current_metrics(self):
        """Populate current metrics for all companies"""
        try:
            self.connect_db()

            cursor = self.conn.cursor()
            cursor.execute("SELECT id, symbol FROM companies ORDER BY symbol")
            companies = cursor.fetchall()
            cursor.close()

            total_companies = len(companies)
            self.logger.info(f"Processing current metrics for {total_companies} companies")

            for idx, (company_id, symbol) in enumerate(companies, 1):
                try:
                    self.logger.info(f"[{idx}/{total_companies}] Processing {symbol}")
                    self.store_current_metrics(company_id, symbol)

                except Exception as e:
                    self.logger.error(f"Error processing {symbol}: {e}")
                    continue

            self.logger.info("Current metrics population completed!")

        except Exception as e:
            self.logger.error(f"Error in populate_all_current_metrics: {e}")
            raise
        finally:
            self.close_db()

if __name__ == "__main__":
    calculator = HistoricalMetricsCalculator()

    # Populate current metrics for all companies
    calculator.populate_all_current_metrics()