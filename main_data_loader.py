
#!/usr/bin/env python3
"""
Main script to set up the database and load yfinance data from CSV files
"""

import os
import sys
import psycopg2
from csv_to_database_loader import CSVToDatabaseLoader
from database_config import get_database_config
import logging

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('yfinance_loader.log')
        ]
    )
    return logging.getLogger(__name__)

def create_database_schema():
    """Create database schema from SQL file"""
    logger = logging.getLogger(__name__)
    
    try:
        # Get database configuration
        db_config = get_database_config()
        
        # Connect to database
        database_url = os.getenv('DATABASE_URL')
        if database_url:
            conn = psycopg2.connect(database_url)
        else:
            conn = psycopg2.connect(**db_config)
        
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Read and execute schema file
        schema_file = 'enhanced_yfinance_schema.sql'
        if os.path.exists(schema_file):
            logger.info(f"Executing schema from {schema_file}")
            with open(schema_file, 'r') as f:
                schema_sql = f.read()
            
            cursor.execute(schema_sql)
            logger.info("✓ Database schema created successfully")
        else:
            logger.error(f"Schema file {schema_file} not found")
            return False
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"✗ Error creating database schema: {e}")
        return False

def check_database_connection():
    """Check if database connection is working"""
    logger = logging.getLogger(__name__)
    
    try:
        db_config = get_database_config()
        
        database_url = os.getenv('DATABASE_URL')
        if database_url:
            conn = psycopg2.connect(database_url)
            logger.info("✓ Connected to database using DATABASE_URL")
        else:
            conn = psycopg2.connect(**db_config)
            logger.info("✓ Connected to database using config")
        
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        logger.info(f"Database version: {db_version[0]}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        return False

def check_csv_files(directory="attached_assets"):
    """Check if CSV files exist"""
    logger = logging.getLogger(__name__)
    
    if not os.path.exists(directory):
        logger.error(f"✗ Directory {directory} does not exist")
        return False
    
    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    if not csv_files:
        logger.error(f"✗ No CSV files found in {directory}")
        return False
    
    logger.info(f"✓ Found {len(csv_files)} CSV files in {directory}")
    for file in sorted(csv_files)[:5]:  # Show first 5 files
        logger.info(f"  - {file}")
    
    if len(csv_files) > 5:
        logger.info(f"  ... and {len(csv_files) - 5} more files")
    
    return True

def load_data_from_csv():
    """Load data from CSV files into database"""
    logger = logging.getLogger(__name__)
    
    try:
        loader = CSVToDatabaseLoader()
        loader.load_all_csv_files()
        logger.info("✓ Data loading completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"✗ Data loading failed: {e}")
        return False

def verify_data_load():
    """Verify that data was loaded correctly"""
    logger = logging.getLogger(__name__)
    
    try:
        db_config = get_database_config()
        
        database_url = os.getenv('DATABASE_URL')
        if database_url:
            conn = psycopg2.connect(database_url)
        else:
            conn = psycopg2.connect(**db_config)
        
        cursor = conn.cursor()
        
        # Check companies table
        cursor.execute("SELECT COUNT(*) FROM companies;")
        companies_count = cursor.fetchone()[0]
        logger.info(f"✓ Companies loaded: {companies_count}")
        
        # Check price history
        cursor.execute("SELECT COUNT(*) FROM price_history;")
        price_count = cursor.fetchone()[0]
        logger.info(f"✓ Price history records: {price_count}")
        
        # Check financial statements
        cursor.execute("SELECT COUNT(*) FROM income_statements;")
        income_count = cursor.fetchone()[0]
        logger.info(f"✓ Income statements: {income_count}")
        
        cursor.execute("SELECT COUNT(*) FROM balance_sheets;")
        balance_count = cursor.fetchone()[0]
        logger.info(f"✓ Balance sheets: {balance_count}")
        
        cursor.execute("SELECT COUNT(*) FROM cash_flow_statements;")
        cashflow_count = cursor.fetchone()[0]
        logger.info(f"✓ Cash flow statements: {cashflow_count}")
        
        # Show sample data
        cursor.execute("""
            SELECT c.symbol, c.long_name, COUNT(ph.id) as price_records
            FROM companies c
            LEFT JOIN price_history ph ON c.id = ph.company_id
            GROUP BY c.id, c.symbol, c.long_name
            ORDER BY c.symbol
            LIMIT 5;
        """)
        
        results = cursor.fetchall()
        logger.info("Sample data loaded:")
        for row in results:
            logger.info(f"  {row[0]} ({row[1]}): {row[2]} price records")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"✗ Data verification failed: {e}")
        return False

def main():
    """Main function to orchestrate the data loading process"""
    logger = setup_logging()
    logger.info("=== YFinance Data Loader Started ===")
    
    # Step 1: Check database connection
    logger.info("Step 1: Checking database connection...")
    if not check_database_connection():
        logger.error("Database connection failed. Please check your configuration.")
        return False
    
    # Step 2: Check CSV files
    logger.info("Step 2: Checking CSV files...")
    if not check_csv_files():
        logger.error("CSV files not found. Please ensure they are in the attached_assets directory.")
        return False
    
    # Step 3: Create database schema
    logger.info("Step 3: Creating database schema...")
    if not create_database_schema():
        logger.error("Schema creation failed.")
        return False
    
    # Step 4: Load data from CSV files
    logger.info("Step 4: Loading data from CSV files...")
    if not load_data_from_csv():
        logger.error("Data loading failed.")
        return False
    
    # Step 5: Verify data load
    logger.info("Step 5: Verifying data load...")
    if not verify_data_load():
        logger.error("Data verification failed.")
        return False
    
    logger.info("=== YFinance Data Loader Completed Successfully ===")
    logger.info("")
    logger.info("You can now query your data using SQL. Some example queries:")
    logger.info("1. SELECT * FROM company_overview;")
    logger.info("2. SELECT * FROM latest_price_data;")
    logger.info("3. SELECT * FROM annual_financials WHERE symbol = 'RELIANCE.NS';")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
