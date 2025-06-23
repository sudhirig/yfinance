
#!/usr/bin/env python3
"""
Script to set up historical metrics system and populate historical data
"""

import os
import sys
import psycopg2
from datetime import date, timedelta
import logging
from database_config import get_db_connection
from historical_metrics_calculator import HistoricalMetricsCalculator

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('historical_metrics_setup.log')
        ]
    )
    return logging.getLogger(__name__)

def create_historical_metrics_schema():
    """Create the historical metrics schema"""
    logger = logging.getLogger(__name__)
    
    try:
        conn = get_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Read and execute historical metrics schema
        schema_file = 'historical_company_metrics_schema.sql'
        if os.path.exists(schema_file):
            logger.info(f"Executing historical metrics schema from {schema_file}")
            with open(schema_file, 'r') as f:
                schema_sql = f.read()
            
            cursor.execute(schema_sql)
            logger.info("✓ Historical metrics schema created successfully")
        else:
            logger.error(f"Schema file {schema_file} not found")
            return False
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"✗ Error creating historical metrics schema: {e}")
        return False

def populate_sample_historical_data():
    """Populate historical metrics for a sample of companies"""
    logger = logging.getLogger(__name__)
    
    try:
        # Get sample companies
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, symbol FROM companies 
            WHERE symbol IN ('RELIANCE.NS', 'TCS.NS', 'INFY.NS', 'HDFCBANK.NS', 'ICICIBANK.NS')
            ORDER BY symbol
        """)
        
        sample_companies = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if not sample_companies:
            logger.error("No sample companies found")
            return False
        
        logger.info(f"Populating historical metrics for {len(sample_companies)} sample companies")
        
        calculator = HistoricalMetricsCalculator()
        
        # Populate for last 2 years
        start_date = date.today() - timedelta(days=2 * 365)
        end_date = date.today()
        
        calculator.connect_db()
        
        for company_id, symbol in sample_companies:
            logger.info(f"Processing {symbol} (ID: {company_id})")
            try:
                calculator.populate_historical_metrics_for_company(company_id, start_date, end_date)
                logger.info(f"✓ Completed {symbol}")
            except Exception as e:
                logger.error(f"✗ Error processing {symbol}: {e}")
                continue
        
        calculator.close_db()
        logger.info("✓ Sample historical data population completed")
        return True
        
    except Exception as e:
        logger.error(f"✗ Error populating sample data: {e}")
        return False

def verify_historical_data():
    """Verify that historical data was populated correctly"""
    logger = logging.getLogger(__name__)
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check data counts
        cursor.execute("SELECT COUNT(*) FROM historical_company_metrics;")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT company_id) FROM historical_company_metrics;")
        companies_with_data = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT period_type, COUNT(*) as count 
            FROM historical_company_metrics 
            GROUP BY period_type 
            ORDER BY period_type;
        """)
        period_breakdown = cursor.fetchall()
        
        # Sample data
        cursor.execute("""
            SELECT 
                c.symbol,
                hcm.metric_date,
                hcm.period_type,
                hcm.trailing_pe,
                hcm.return_on_equity
            FROM companies c
            JOIN historical_company_metrics hcm ON c.id = hcm.company_id
            ORDER BY c.symbol, hcm.metric_date DESC
            LIMIT 10;
        """)
        sample_data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        logger.info("=== HISTORICAL METRICS VERIFICATION ===")
        logger.info(f"Total historical records: {total_records:,}")
        logger.info(f"Companies with historical data: {companies_with_data}")
        
        logger.info("Period type breakdown:")
        for period_type, count in period_breakdown:
            logger.info(f"  {period_type}: {count:,} records")
        
        logger.info("Sample historical data:")
        for row in sample_data:
            symbol, metric_date, period_type, pe, roe = row
            logger.info(f"  {symbol} ({period_type}) {metric_date}: PE={pe}, ROE={roe}")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Error verifying historical data: {e}")
        return False

def main():
    """Main function to set up historical metrics system"""
    logger = setup_logging()
    logger.info("=== Historical Metrics Setup Started ===")
    
    # Step 1: Create schema
    logger.info("Step 1: Creating historical metrics schema...")
    if not create_historical_metrics_schema():
        logger.error("Schema creation failed. Exiting.")
        return False
    
    # Step 2: Populate sample data
    logger.info("Step 2: Populating sample historical data...")
    if not populate_sample_historical_data():
        logger.error("Sample data population failed. Exiting.")
        return False
    
    # Step 3: Verify data
    logger.info("Step 3: Verifying historical data...")
    if not verify_historical_data():
        logger.error("Data verification failed.")
        return False
    
    logger.info("=== Historical Metrics Setup Completed Successfully ===")
    logger.info("")
    logger.info("New API endpoints available:")
    logger.info("1. /api/stock/<symbol>/historical-metrics?period=quarterly&limit=20")
    logger.info("2. /api/stock/<symbol>/metrics-trend?metric=trailing_pe&years=3")
    logger.info("3. /api/metrics/comparison?symbols=RELIANCE.NS,TCS.NS&metric=return_on_equity")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
