
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, date, timedelta
import logging
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
    
    def safe_divide(self, numerator, denominator):
        """Safely divide two numbers, return None if division by zero"""
        if denominator is None or denominator == 0:
            return None
        if numerator is None:
            return None
        return float(numerator) / float(denominator)
    
    def calculate_metrics_for_period(self, company_id: int, period_date: date, period_type: str) -> Dict:
        """Calculate metrics for a specific company and period"""
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
        metrics = {}
        
        try:
            # Get financial statement data for the period
            income_data = self.get_closest_income_statement(cursor, company_id, period_date, period_type)
            balance_data = self.get_closest_balance_sheet(cursor, company_id, period_date, period_type)
            cashflow_data = self.get_closest_cashflow_statement(cursor, company_id, period_date, period_type)
            price_data = self.get_closest_price_data(cursor, company_id, period_date)
            
            if not any([income_data, balance_data, cashflow_data, price_data]):
                return None
            
            # Calculate basic metrics
            if price_data:
                metrics['current_price'] = price_data.get('close_price')
                
                # Calculate market cap (price * shares outstanding)
                if balance_data and balance_data.get('common_stock'):
                    shares = balance_data.get('common_stock', 0)
                    if shares > 0:
                        metrics['market_cap'] = int(price_data['close_price'] * shares)
            
            # Profitability Metrics
            if income_data:
                total_revenue = income_data.get('total_revenue', 0)
                gross_profit = income_data.get('gross_profit', 0)
                operating_income = income_data.get('operating_income', 0)
                net_income = income_data.get('net_income', 0)
                
                if total_revenue and total_revenue > 0:
                    metrics['gross_margin'] = self.safe_divide(gross_profit, total_revenue)
                    metrics['operating_margin'] = self.safe_divide(operating_income, total_revenue)
                    metrics['profit_margin'] = self.safe_divide(net_income, total_revenue)
            
            # Valuation Metrics
            if income_data and metrics.get('market_cap'):
                eps = income_data.get('diluted_eps')
                if eps and eps > 0 and price_data:
                    metrics['trailing_pe'] = self.safe_divide(price_data['close_price'], eps)
                
                total_revenue = income_data.get('total_revenue')
                if total_revenue and total_revenue > 0:
                    metrics['price_to_sales'] = self.safe_divide(metrics['market_cap'], total_revenue)
            
            # Financial Health Metrics
            if balance_data:
                total_assets = balance_data.get('total_assets', 0)
                total_debt = balance_data.get('total_debt', 0)
                stockholders_equity = balance_data.get('stockholders_equity', 0)
                current_assets = balance_data.get('current_assets', 0)
                current_liabilities = balance_data.get('current_liabilities', 0)
                
                if stockholders_equity and stockholders_equity != 0:
                    metrics['debt_to_equity'] = self.safe_divide(total_debt, stockholders_equity)
                    
                    if income_data and income_data.get('net_income'):
                        metrics['return_on_equity'] = self.safe_divide(income_data['net_income'], stockholders_equity)
                
                if total_assets and total_assets > 0 and income_data and income_data.get('net_income'):
                    metrics['return_on_assets'] = self.safe_divide(income_data['net_income'], total_assets)
                
                if current_liabilities and current_liabilities > 0:
                    metrics['current_ratio'] = self.safe_divide(current_assets, current_liabilities)
                
                # Book value per share
                shares_outstanding = self.get_shares_outstanding(cursor, company_id, period_date)
                if shares_outstanding and shares_outstanding > 0:
                    metrics['book_value_per_share'] = self.safe_divide(stockholders_equity, shares_outstanding)
                    metrics['shares_outstanding'] = shares_outstanding
            
            # Cash Flow Metrics
            if cashflow_data:
                operating_cf = cashflow_data.get('operating_cash_flow')
                free_cf = cashflow_data.get('free_cash_flow')
                
                metrics['operating_cashflow'] = operating_cf
                metrics['free_cashflow'] = free_cf
                
                if free_cf and metrics.get('shares_outstanding'):
                    metrics['fcf_per_share'] = self.safe_divide(free_cf, metrics['shares_outstanding'])
            
            # Growth Metrics (Year-over-Year)
            yoy_metrics = self.calculate_yoy_growth(cursor, company_id, period_date, period_type)
            metrics.update(yoy_metrics)
            
        except Exception as e:
            self.logger.error(f"Error calculating metrics for company {company_id}, period {period_date}: {e}")
            return None
        finally:
            cursor.close()
        
        return metrics
    
    def get_closest_income_statement(self, cursor, company_id: int, target_date: date, period_type: str) -> Dict:
        """Get the closest income statement to the target date"""
        cursor.execute("""
            SELECT * FROM income_statements 
            WHERE company_id = %s 
            AND period_type = %s 
            AND period_ending <= %s
            ORDER BY period_ending DESC 
            LIMIT 1
        """, (company_id, period_type, target_date))
        
        result = cursor.fetchone()
        return dict(result) if result else None
    
    def get_closest_balance_sheet(self, cursor, company_id: int, target_date: date, period_type: str) -> Dict:
        """Get the closest balance sheet to the target date"""
        cursor.execute("""
            SELECT * FROM balance_sheets 
            WHERE company_id = %s 
            AND period_type = %s 
            AND period_ending <= %s
            ORDER BY period_ending DESC 
            LIMIT 1
        """, (company_id, period_type, target_date))
        
        result = cursor.fetchone()
        return dict(result) if result else None
    
    def get_closest_cashflow_statement(self, cursor, company_id: int, target_date: date, period_type: str) -> Dict:
        """Get the closest cash flow statement to the target date"""
        cursor.execute("""
            SELECT * FROM cash_flow_statements 
            WHERE company_id = %s 
            AND period_type = %s 
            AND period_ending <= %s
            ORDER BY period_ending DESC 
            LIMIT 1
        """, (company_id, period_type, target_date))
        
        result = cursor.fetchone()
        return dict(result) if result else None
    
    def get_closest_price_data(self, cursor, company_id: int, target_date: date) -> Dict:
        """Get the closest price data to the target date"""
        cursor.execute("""
            SELECT * FROM price_history 
            WHERE company_id = %s 
            AND date <= %s
            ORDER BY date DESC 
            LIMIT 1
        """, (company_id, target_date))
        
        result = cursor.fetchone()
        return dict(result) if result else None
    
    def get_shares_outstanding(self, cursor, company_id: int, target_date: date) -> Optional[int]:
        """Get shares outstanding from the most recent data"""
        # Try to get from company_metrics first
        cursor.execute("""
            SELECT shares_outstanding FROM company_metrics 
            WHERE company_id = %s
        """, (company_id,))
        
        result = cursor.fetchone()
        if result and result['shares_outstanding']:
            return result['shares_outstanding']
        
        # Fallback: estimate from balance sheet common stock
        cursor.execute("""
            SELECT common_stock FROM balance_sheets 
            WHERE company_id = %s 
            AND period_ending <= %s
            ORDER BY period_ending DESC 
            LIMIT 1
        """, (company_id, target_date))
        
        result = cursor.fetchone()
        return result['common_stock'] if result and result['common_stock'] else None
    
    def calculate_yoy_growth(self, cursor, company_id: int, current_date: date, period_type: str) -> Dict:
        """Calculate year-over-year growth metrics"""
        metrics = {}
        
        # Calculate previous year date
        prev_year_date = date(current_date.year - 1, current_date.month, current_date.day)
        
        # Get current and previous year income statements
        current_income = self.get_closest_income_statement(cursor, company_id, current_date, period_type)
        prev_income = self.get_closest_income_statement(cursor, company_id, prev_year_date, period_type)
        
        if current_income and prev_income:
            # Revenue growth
            current_revenue = current_income.get('total_revenue', 0)
            prev_revenue = prev_income.get('total_revenue', 0)
            
            if prev_revenue and prev_revenue > 0:
                revenue_growth = ((current_revenue - prev_revenue) / prev_revenue) * 100
                metrics['revenue_growth_yoy'] = revenue_growth
            
            # Earnings growth
            current_earnings = current_income.get('net_income', 0)
            prev_earnings = prev_income.get('net_income', 0)
            
            if prev_earnings and prev_earnings != 0:
                earnings_growth = ((current_earnings - prev_earnings) / abs(prev_earnings)) * 100
                metrics['earnings_growth_yoy'] = earnings_growth
        
        return metrics
    
    def store_historical_metrics(self, company_id: int, metric_date: date, period_type: str, metrics: Dict):
        """Store calculated metrics in the historical_company_metrics table"""
        cursor = self.conn.cursor()
        
        insert_sql = """
        INSERT INTO historical_company_metrics (
            company_id, metric_date, period_type, market_cap, shares_outstanding,
            current_price, trailing_pe, forward_pe, price_to_book, price_to_sales,
            gross_margin, operating_margin, profit_margin, return_on_assets, return_on_equity,
            revenue_growth_yoy, earnings_growth_yoy, debt_to_equity, current_ratio,
            operating_cashflow, free_cashflow, fcf_per_share, book_value_per_share
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (company_id, metric_date, period_type) DO UPDATE SET
            market_cap = EXCLUDED.market_cap,
            shares_outstanding = EXCLUDED.shares_outstanding,
            current_price = EXCLUDED.current_price,
            trailing_pe = EXCLUDED.trailing_pe,
            price_to_book = EXCLUDED.price_to_book,
            price_to_sales = EXCLUDED.price_to_sales,
            gross_margin = EXCLUDED.gross_margin,
            operating_margin = EXCLUDED.operating_margin,
            profit_margin = EXCLUDED.profit_margin,
            return_on_assets = EXCLUDED.return_on_assets,
            return_on_equity = EXCLUDED.return_on_equity,
            revenue_growth_yoy = EXCLUDED.revenue_growth_yoy,
            earnings_growth_yoy = EXCLUDED.earnings_growth_yoy,
            debt_to_equity = EXCLUDED.debt_to_equity,
            current_ratio = EXCLUDED.current_ratio,
            operating_cashflow = EXCLUDED.operating_cashflow,
            free_cashflow = EXCLUDED.free_cashflow,
            fcf_per_share = EXCLUDED.fcf_per_share,
            book_value_per_share = EXCLUDED.book_value_per_share,
            updated_at = CURRENT_TIMESTAMP
        """
        
        cursor.execute(insert_sql, (
            company_id, metric_date, period_type,
            metrics.get('market_cap'),
            metrics.get('shares_outstanding'),
            metrics.get('current_price'),
            metrics.get('trailing_pe'),
            metrics.get('forward_pe'),
            metrics.get('price_to_book'),
            metrics.get('price_to_sales'),
            metrics.get('gross_margin'),
            metrics.get('operating_margin'),
            metrics.get('profit_margin'),
            metrics.get('return_on_assets'),
            metrics.get('return_on_equity'),
            metrics.get('revenue_growth_yoy'),
            metrics.get('earnings_growth_yoy'),
            metrics.get('debt_to_equity'),
            metrics.get('current_ratio'),
            metrics.get('operating_cashflow'),
            metrics.get('free_cashflow'),
            metrics.get('fcf_per_share'),
            metrics.get('book_value_per_share')
        ))
        
        cursor.close()
    
    def populate_historical_metrics_for_company(self, company_id: int, start_date: date, end_date: date):
        """Populate historical metrics for a company across a date range"""
        self.logger.info(f"Populating historical metrics for company {company_id}")
        
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
        # Get all available financial statement dates for this company
        cursor.execute("""
            SELECT DISTINCT period_ending, period_type 
            FROM income_statements 
            WHERE company_id = %s 
            AND period_ending BETWEEN %s AND %s
            ORDER BY period_ending, period_type
        """, (company_id, start_date, end_date))
        
        periods = cursor.fetchall()
        cursor.close()
        
        for period in periods:
            period_date = period['period_ending']
            period_type = period['period_type']
            
            try:
                metrics = self.calculate_metrics_for_period(company_id, period_date, period_type)
                
                if metrics:
                    self.store_historical_metrics(company_id, period_date, period_type, metrics)
                    self.logger.info(f"✓ Stored {period_type} metrics for company {company_id} on {period_date}")
                else:
                    self.logger.warning(f"⚠️ No metrics calculated for company {company_id} on {period_date}")
                    
            except Exception as e:
                self.logger.error(f"Error processing company {company_id}, period {period_date}: {e}")
                continue
        
        self.conn.commit()
    
    def populate_all_historical_metrics(self, start_date: date = None, end_date: date = None):
        """Populate historical metrics for all companies"""
        try:
            self.connect_db()
            
            if not start_date:
                start_date = date(2020, 1, 1)  # Default to 2020
            if not end_date:
                end_date = date.today()
            
            self.logger.info(f"Starting historical metrics population from {start_date} to {end_date}")
            
            # Get all companies
            cursor = self.conn.cursor()
            cursor.execute("SELECT id, symbol FROM companies ORDER BY symbol")
            companies = cursor.fetchall()
            cursor.close()
            
            total_companies = len(companies)
            self.logger.info(f"Processing {total_companies} companies")
            
            for idx, (company_id, symbol) in enumerate(companies, 1):
                try:
                    self.logger.info(f"[{idx}/{total_companies}] Processing {symbol} (ID: {company_id})")
                    self.populate_historical_metrics_for_company(company_id, start_date, end_date)
                    
                except Exception as e:
                    self.logger.error(f"Error processing company {symbol}: {e}")
                    self.conn.rollback()
                    continue
            
            self.logger.info("Historical metrics population completed!")
            
        except Exception as e:
            self.logger.error(f"Error in populate_all_historical_metrics: {e}")
            if self.conn:
                self.conn.rollback()
            raise
        finally:
            self.close_db()

if __name__ == "__main__":
    calculator = HistoricalMetricsCalculator()
    
    # Populate historical metrics for the last 3 years
    start_date = date(2021, 1, 1)
    end_date = date.today()
    
    calculator.populate_all_historical_metrics(start_date, end_date)
