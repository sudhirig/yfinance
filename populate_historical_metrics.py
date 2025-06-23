
#!/usr/bin/env python3
"""
Populate Historical Company Metrics Table
Calculates and stores historical metrics from financial statements data
"""

import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from database_config import get_db_connection
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HistoricalMetricsPopulator:
    def __init__(self):
        self.conn = get_db_connection()
        self.cursor = self.conn.cursor()
        
        # Create historical metrics table if it doesn't exist
        self.create_historical_metrics_table()
    
    def create_historical_metrics_table(self):
        """Create the historical metrics table"""
        create_sql = """
        CREATE TABLE IF NOT EXISTS historical_company_metrics (
            id SERIAL PRIMARY KEY,
            company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
            metric_date DATE NOT NULL,
            period_type VARCHAR(10) NOT NULL,
            
            -- Market Data
            market_cap BIGINT,
            shares_outstanding BIGINT,
            regular_market_price DECIMAL(15,6),
            
            -- Valuation Metrics
            trailing_pe NUMERIC(12, 6),
            price_to_book NUMERIC(12, 6),
            price_to_sales DECIMAL(10,4),
            enterprise_value BIGINT,
            
            -- Financial Health Metrics
            debt_to_equity DECIMAL(10,4),
            return_on_assets DECIMAL(8,4),
            return_on_equity DECIMAL(8,4),
            current_ratio DECIMAL(8,4),
            
            -- Profitability Metrics
            profit_margins DECIMAL(8,4),
            operating_margins DECIMAL(8,4),
            gross_margins DECIMAL(8,4),
            
            -- Growth Metrics (YoY)
            revenue_growth DECIMAL(8,4),
            earnings_growth DECIMAL(8,4),
            
            -- Per Share Metrics
            eps DECIMAL(10,4),
            revenue_per_share DECIMAL(10,4),
            book_value_per_share DECIMAL(10,4),
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(company_id, metric_date, period_type)
        );
        
        CREATE INDEX IF NOT EXISTS idx_historical_metrics_company_date 
        ON historical_company_metrics(company_id, metric_date DESC);
        """
        
        self.cursor.execute(create_sql)
        self.conn.commit()
        logger.info("Historical metrics table created/verified")
    
    def calculate_quarterly_metrics(self):
        """Calculate and insert quarterly metrics from financial statements"""
        logger.info("Calculating quarterly metrics...")
        
        # Get all quarterly financial data
        query = """
        SELECT 
            c.id as company_id,
            c.symbol,
            i.period_ending,
            i.total_revenue,
            i.gross_profit,
            i.operating_income,
            i.net_income,
            i.diluted_eps,
            b.total_assets,
            b.current_assets,
            b.current_liabilities,
            b.stockholders_equity,
            b.total_debt,
            cf.operating_cash_flow,
            
            -- Get previous year same quarter for growth calculations
            LAG(i.total_revenue) OVER (
                PARTITION BY c.id 
                ORDER BY i.period_ending 
                ROWS 4 PRECEDING
            ) as prev_year_revenue,
            
            LAG(i.net_income) OVER (
                PARTITION BY c.id 
                ORDER BY i.period_ending 
                ROWS 4 PRECEDING
            ) as prev_year_earnings
            
        FROM companies c
        JOIN income_statements i ON c.id = i.company_id AND i.period_type = 'quarterly'
        LEFT JOIN balance_sheets b ON c.id = b.company_id 
            AND b.period_ending = i.period_ending AND b.period_type = 'quarterly'
        LEFT JOIN cash_flow_statements cf ON c.id = cf.company_id 
            AND cf.period_ending = i.period_ending AND cf.period_type = 'quarterly'
        WHERE i.period_ending >= '2020-01-01'
        ORDER BY c.id, i.period_ending
        """
        
        df = pd.read_sql(query, self.conn)
        
        for _, row in df.iterrows():
            try:
                # Calculate metrics
                metrics = self.calculate_financial_metrics(row)
                
                # Insert into historical metrics table
                self.insert_historical_metrics(
                    company_id=row['company_id'],
                    metric_date=row['period_ending'],
                    period_type='quarterly',
                    metrics=metrics
                )
                
            except Exception as e:
                logger.error(f"Error processing {row['symbol']} {row['period_ending']}: {e}")
                continue
        
        self.conn.commit()
        logger.info(f"Processed {len(df)} quarterly records")
    
    def calculate_annual_metrics(self):
        """Calculate and insert annual metrics from financial statements"""
        logger.info("Calculating annual metrics...")
        
        query = """
        SELECT 
            c.id as company_id,
            c.symbol,
            i.period_ending,
            i.total_revenue,
            i.gross_profit,
            i.operating_income,
            i.net_income,
            i.diluted_eps,
            b.total_assets,
            b.current_assets,
            b.current_liabilities,
            b.stockholders_equity,
            b.total_debt,
            cf.operating_cash_flow,
            
            -- Previous year for growth calculations
            LAG(i.total_revenue) OVER (
                PARTITION BY c.id ORDER BY i.period_ending
            ) as prev_year_revenue,
            
            LAG(i.net_income) OVER (
                PARTITION BY c.id ORDER BY i.period_ending
            ) as prev_year_earnings
            
        FROM companies c
        JOIN income_statements i ON c.id = i.company_id AND i.period_type = 'annual'
        LEFT JOIN balance_sheets b ON c.id = b.company_id 
            AND b.period_ending = i.period_ending AND b.period_type = 'annual'
        LEFT JOIN cash_flow_statements cf ON c.id = cf.company_id 
            AND cf.period_ending = i.period_ending AND cf.period_type = 'annual'
        WHERE i.period_ending >= '2015-01-01'
        ORDER BY c.id, i.period_ending
        """
        
        df = pd.read_sql(query, self.conn)
        
        for _, row in df.iterrows():
            try:
                metrics = self.calculate_financial_metrics(row)
                
                self.insert_historical_metrics(
                    company_id=row['company_id'],
                    metric_date=row['period_ending'],
                    period_type='annual',
                    metrics=metrics
                )
                
            except Exception as e:
                logger.error(f"Error processing {row['symbol']} {row['period_ending']}: {e}")
                continue
        
        self.conn.commit()
        logger.info(f"Processed {len(df)} annual records")
    
    def calculate_financial_metrics(self, row):
        """Calculate financial metrics from statement data"""
        metrics = {}
        
        try:
            # Profitability ratios
            if row['total_revenue'] and row['total_revenue'] > 0:
                if row['gross_profit']:
                    metrics['gross_margins'] = (row['gross_profit'] / row['total_revenue']) * 100
                if row['operating_income']:
                    metrics['operating_margins'] = (row['operating_income'] / row['total_revenue']) * 100
                if row['net_income']:
                    metrics['profit_margins'] = (row['net_income'] / row['total_revenue']) * 100
            
            # Return ratios
            if row['total_assets'] and row['total_assets'] > 0 and row['net_income']:
                metrics['return_on_assets'] = (row['net_income'] / row['total_assets']) * 100
            
            if row['stockholders_equity'] and row['stockholders_equity'] > 0 and row['net_income']:
                metrics['return_on_equity'] = (row['net_income'] / row['stockholders_equity']) * 100
            
            # Debt ratios
            if row['stockholders_equity'] and row['stockholders_equity'] > 0 and row['total_debt']:
                metrics['debt_to_equity'] = (row['total_debt'] / row['stockholders_equity']) * 100
            
            # Liquidity ratios
            if row['current_liabilities'] and row['current_liabilities'] > 0 and row['current_assets']:
                metrics['current_ratio'] = row['current_assets'] / row['current_liabilities']
            
            # Growth rates (YoY)
            if row['prev_year_revenue'] and row['prev_year_revenue'] > 0 and row['total_revenue']:
                metrics['revenue_growth'] = ((row['total_revenue'] - row['prev_year_revenue']) / row['prev_year_revenue']) * 100
            
            if row['prev_year_earnings'] and row['prev_year_earnings'] > 0 and row['net_income']:
                metrics['earnings_growth'] = ((row['net_income'] - row['prev_year_earnings']) / row['prev_year_earnings']) * 100
            
            # Per share metrics
            metrics['eps'] = row['diluted_eps']
            
        except Exception as e:
            logger.error(f"Error calculating metrics: {e}")
        
        return metrics
    
    def insert_historical_metrics(self, company_id, metric_date, period_type, metrics):
        """Insert calculated metrics into historical table"""
        
        insert_sql = """
        INSERT INTO historical_company_metrics (
            company_id, metric_date, period_type,
            profit_margins, operating_margins, gross_margins,
            return_on_assets, return_on_equity, debt_to_equity,
            current_ratio, revenue_growth, earnings_growth, eps
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (company_id, metric_date, period_type) DO UPDATE SET
            profit_margins = EXCLUDED.profit_margins,
            operating_margins = EXCLUDED.operating_margins,
            gross_margins = EXCLUDED.gross_margins,
            return_on_assets = EXCLUDED.return_on_assets,
            return_on_equity = EXCLUDED.return_on_equity,
            debt_to_equity = EXCLUDED.debt_to_equity,
            current_ratio = EXCLUDED.current_ratio,
            revenue_growth = EXCLUDED.revenue_growth,
            earnings_growth = EXCLUDED.earnings_growth,
            eps = EXCLUDED.eps
        """
        
        self.cursor.execute(insert_sql, (
            company_id, metric_date, period_type,
            metrics.get('profit_margins'),
            metrics.get('operating_margins'),
            metrics.get('gross_margins'),
            metrics.get('return_on_assets'),
            metrics.get('return_on_equity'),
            metrics.get('debt_to_equity'),
            metrics.get('current_ratio'),
            metrics.get('revenue_growth'),
            metrics.get('earnings_growth'),
            metrics.get('eps')
        ))
    
    def get_metrics_summary(self):
        """Get summary of populated historical metrics"""
        query = """
        SELECT 
            period_type,
            COUNT(*) as total_records,
            COUNT(DISTINCT company_id) as companies_covered,
            MIN(metric_date) as earliest_date,
            MAX(metric_date) as latest_date
        FROM historical_company_metrics
        GROUP BY period_type
        ORDER BY period_type
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        print("\n📊 HISTORICAL METRICS SUMMARY")
        print("=" * 50)
        for period_type, total, companies, earliest, latest in results:
            print(f"{period_type.upper()}:")
            print(f"  Records: {total:,}")
            print(f"  Companies: {companies}")
            print(f"  Date Range: {earliest} to {latest}")
            print()
    
    def close(self):
        """Close database connection"""
        self.cursor.close()
        self.conn.close()

def main():
    """Main execution function"""
    populator = HistoricalMetricsPopulator()
    
    try:
        print("🚀 Starting Historical Metrics Population")
        print("=" * 50)
        
        # Populate quarterly metrics
        populator.calculate_quarterly_metrics()
        
        # Populate annual metrics
        populator.calculate_annual_metrics()
        
        # Show summary
        populator.get_metrics_summary()
        
        print("✅ Historical metrics population completed!")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise
    finally:
        populator.close()

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Populate Historical Company Metrics Table
Calculates and stores historical metrics from financial statements data
"""

import psycopg2
import pandas as pd
from datetime import datetime, timedelta, date
from database_config import get_db_connection
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HistoricalMetricsPopulator:
    def __init__(self):
        self.conn = get_db_connection()
        self.cursor = self.conn.cursor()
        
        # Create historical metrics table if it doesn't exist
        self.create_historical_metrics_table()
    
    def create_historical_metrics_table(self):
        """Create the historical metrics table"""
        create_sql = """
        CREATE TABLE IF NOT EXISTS historical_company_metrics (
            id SERIAL PRIMARY KEY,
            company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
            metric_date DATE NOT NULL,
            period_type VARCHAR(20) NOT NULL,
            
            -- Market Data
            market_cap BIGINT,
            shares_outstanding BIGINT,
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
            
            UNIQUE(company_id, metric_date, period_type)
        );
        
        CREATE INDEX IF NOT EXISTS idx_historical_metrics_company_date 
        ON historical_company_metrics(company_id, metric_date DESC);
        """
        
        self.cursor.execute(create_sql)
        self.conn.commit()
        logger.info("Historical metrics table created/verified")
    
    def calculate_quarterly_metrics(self):
        """Calculate and insert quarterly metrics from financial statements"""
        logger.info("Calculating quarterly metrics...")
        
        # Get all quarterly financial data
        query = """
        SELECT 
            c.id as company_id,
            c.symbol,
            i.period_ending,
            i.total_revenue,
            i.gross_profit,
            i.operating_income,
            i.net_income,
            i.diluted_eps,
            b.total_assets,
            b.current_assets,
            b.current_liabilities,
            b.stockholders_equity,
            b.total_debt,
            cf.operating_cash_flow,
            
            -- Get previous year same quarter for growth calculations
            LAG(i.total_revenue, 4) OVER (
                PARTITION BY c.id 
                ORDER BY i.period_ending
            ) as prev_year_revenue,
            
            LAG(i.net_income, 4) OVER (
                PARTITION BY c.id 
                ORDER BY i.period_ending
            ) as prev_year_earnings
            
        FROM companies c
        JOIN income_statements i ON c.id = i.company_id AND i.period_type = 'quarterly'
        LEFT JOIN balance_sheets b ON c.id = b.company_id 
            AND b.period_ending = i.period_ending AND b.period_type = 'quarterly'
        LEFT JOIN cash_flow_statements cf ON c.id = cf.company_id 
            AND cf.period_ending = i.period_ending AND cf.period_type = 'quarterly'
        WHERE i.period_ending >= '2020-01-01'
        ORDER BY c.id, i.period_ending
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        df = pd.DataFrame(results, columns=columns)
        
        for _, row in df.iterrows():
            try:
                # Calculate metrics
                metrics = self.calculate_financial_metrics(row)
                
                # Insert into historical metrics table
                self.insert_historical_metrics(
                    company_id=row['company_id'],
                    metric_date=row['period_ending'],
                    period_type='quarterly',
                    metrics=metrics
                )
                
            except Exception as e:
                logger.error(f"Error processing {row['symbol']} {row['period_ending']}: {e}")
                continue
        
        self.conn.commit()
        logger.info(f"Processed {len(df)} quarterly records")
    
    def calculate_annual_metrics(self):
        """Calculate and insert annual metrics from financial statements"""
        logger.info("Calculating annual metrics...")
        
        query = """
        SELECT 
            c.id as company_id,
            c.symbol,
            i.period_ending,
            i.total_revenue,
            i.gross_profit,
            i.operating_income,
            i.net_income,
            i.diluted_eps,
            b.total_assets,
            b.current_assets,
            b.current_liabilities,
            b.stockholders_equity,
            b.total_debt,
            cf.operating_cash_flow,
            
            -- Previous year for growth calculations
            LAG(i.total_revenue) OVER (
                PARTITION BY c.id ORDER BY i.period_ending
            ) as prev_year_revenue,
            
            LAG(i.net_income) OVER (
                PARTITION BY c.id ORDER BY i.period_ending
            ) as prev_year_earnings
            
        FROM companies c
        JOIN income_statements i ON c.id = i.company_id AND i.period_type = 'annual'
        LEFT JOIN balance_sheets b ON c.id = b.company_id 
            AND b.period_ending = i.period_ending AND b.period_type = 'annual'
        LEFT JOIN cash_flow_statements cf ON c.id = cf.company_id 
            AND cf.period_ending = i.period_ending AND cf.period_type = 'annual'
        WHERE i.period_ending >= '2015-01-01'
        ORDER BY c.id, i.period_ending
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        df = pd.DataFrame(results, columns=columns)
        
        for _, row in df.iterrows():
            try:
                metrics = self.calculate_financial_metrics(row)
                
                self.insert_historical_metrics(
                    company_id=row['company_id'],
                    metric_date=row['period_ending'],
                    period_type='annual',
                    metrics=metrics
                )
                
            except Exception as e:
                logger.error(f"Error processing {row['symbol']} {row['period_ending']}: {e}")
                continue
        
        self.conn.commit()
        logger.info(f"Processed {len(df)} annual records")
    
    def calculate_financial_metrics(self, row):
        """Calculate financial metrics from statement data"""
        metrics = {}
        
        try:
            # Profitability ratios
            if row['total_revenue'] and row['total_revenue'] > 0:
                if row['gross_profit']:
                    metrics['gross_margin'] = (row['gross_profit'] / row['total_revenue'])
                if row['operating_income']:
                    metrics['operating_margin'] = (row['operating_income'] / row['total_revenue'])
                if row['net_income']:
                    metrics['profit_margin'] = (row['net_income'] / row['total_revenue'])
            
            # Return ratios
            if row['total_assets'] and row['total_assets'] > 0 and row['net_income']:
                metrics['return_on_assets'] = (row['net_income'] / row['total_assets'])
            
            if row['stockholders_equity'] and row['stockholders_equity'] > 0 and row['net_income']:
                metrics['return_on_equity'] = (row['net_income'] / row['stockholders_equity'])
            
            # Debt ratios
            if row['stockholders_equity'] and row['stockholders_equity'] > 0 and row['total_debt']:
                metrics['debt_to_equity'] = (row['total_debt'] / row['stockholders_equity'])
            
            # Liquidity ratios
            if row['current_liabilities'] and row['current_liabilities'] > 0 and row['current_assets']:
                metrics['current_ratio'] = row['current_assets'] / row['current_liabilities']
            
            # Growth rates (YoY)
            if row['prev_year_revenue'] and row['prev_year_revenue'] > 0 and row['total_revenue']:
                metrics['revenue_growth_yoy'] = ((row['total_revenue'] - row['prev_year_revenue']) / row['prev_year_revenue'])
            
            if row['prev_year_earnings'] and row['prev_year_earnings'] > 0 and row['net_income']:
                metrics['earnings_growth_yoy'] = ((row['net_income'] - row['prev_year_earnings']) / row['prev_year_earnings'])
            
            # Cash flow metrics
            if row['operating_cash_flow']:
                metrics['operating_cashflow'] = row['operating_cash_flow']
            
        except Exception as e:
            logger.error(f"Error calculating metrics: {e}")
        
        return metrics
    
    def insert_historical_metrics(self, company_id, metric_date, period_type, metrics):
        """Insert calculated metrics into historical table"""
        
        insert_sql = """
        INSERT INTO historical_company_metrics (
            company_id, metric_date, period_type,
            profit_margin, operating_margin, gross_margin,
            return_on_assets, return_on_equity, debt_to_equity,
            current_ratio, revenue_growth_yoy, earnings_growth_yoy, operating_cashflow
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (company_id, metric_date, period_type) DO UPDATE SET
            profit_margin = EXCLUDED.profit_margin,
            operating_margin = EXCLUDED.operating_margin,
            gross_margin = EXCLUDED.gross_margin,
            return_on_assets = EXCLUDED.return_on_assets,
            return_on_equity = EXCLUDED.return_on_equity,
            debt_to_equity = EXCLUDED.debt_to_equity,
            current_ratio = EXCLUDED.current_ratio,
            revenue_growth_yoy = EXCLUDED.revenue_growth_yoy,
            earnings_growth_yoy = EXCLUDED.earnings_growth_yoy,
            operating_cashflow = EXCLUDED.operating_cashflow,
            updated_at = CURRENT_TIMESTAMP
        """
        
        self.cursor.execute(insert_sql, (
            company_id, metric_date, period_type,
            metrics.get('profit_margin'),
            metrics.get('operating_margin'),
            metrics.get('gross_margin'),
            metrics.get('return_on_assets'),
            metrics.get('return_on_equity'),
            metrics.get('debt_to_equity'),
            metrics.get('current_ratio'),
            metrics.get('revenue_growth_yoy'),
            metrics.get('earnings_growth_yoy'),
            metrics.get('operating_cashflow')
        ))
    
    def get_metrics_summary(self):
        """Get summary of populated historical metrics"""
        query = """
        SELECT 
            period_type,
            COUNT(*) as total_records,
            COUNT(DISTINCT company_id) as companies_covered,
            MIN(metric_date) as earliest_date,
            MAX(metric_date) as latest_date
        FROM historical_company_metrics
        GROUP BY period_type
        ORDER BY period_type
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        print("\n📊 HISTORICAL METRICS SUMMARY")
        print("=" * 50)
        for period_type, total, companies, earliest, latest in results:
            print(f"{period_type.upper()}:")
            print(f"  Records: {total:,}")
            print(f"  Companies: {companies}")
            print(f"  Date Range: {earliest} to {latest}")
            print()
    
    def close(self):
        """Close database connection"""
        self.cursor.close()
        self.conn.close()

def main():
    """Main execution function"""
    populator = HistoricalMetricsPopulator()
    
    try:
        print("🚀 Starting Historical Metrics Population")
        print("=" * 50)
        
        # Populate quarterly metrics
        populator.calculate_quarterly_metrics()
        
        # Populate annual metrics
        populator.calculate_annual_metrics()
        
        # Show summary
        populator.get_metrics_summary()
        
        print("✅ Historical metrics population completed!")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise
    finally:
        populator.close()

if __name__ == "__main__":
    main()
