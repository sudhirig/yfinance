
#!/usr/bin/env python3
"""
Import database data from SQL dump file
"""

import os
import sys
import subprocess
import psycopg2
import logging
from datetime import datetime

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

def import_database_from_sql(sql_file):
    """Import database from SQL file"""
    logger = setup_logging()
    
    try:
        if not os.path.exists(sql_file):
            logger.error(f"❌ File not found: {sql_file}")
            return False
        
        # Get database URL
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            logger.error("❌ DATABASE_URL not found")
            return False
        
        logger.info(f"🔄 Starting database import from {sql_file}")
        logger.info(f"📁 File size: {os.path.getsize(sql_file) / (1024*1024):.2f} MB")
        
        # Use psql to import database
        command = [
            'psql',
            database_url,
            '--file', sql_file,
            '--quiet'
        ]
        
        result = subprocess.run(command, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("✅ Database imported successfully!")
            return True
        else:
            logger.error(f"❌ Import failed: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Import error: {e}")
        return False

def verify_import():
    """Verify the imported data"""
    logger = setup_logging()
    
    try:
        database_url = os.getenv('DATABASE_URL')
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        tables = [
            'companies', 'price_history', 'company_metrics',
            'income_statements', 'balance_sheets', 'cash_flow_statements',
            'corporate_actions'
        ]
        
        logger.info("📊 Imported database contents:")
        total_records = 0
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                logger.info(f"  {table}: {count:,} records")
                total_records += count
            except Exception as e:
                logger.warning(f"  {table}: Error getting count - {e}")
        
        logger.info(f"📈 Total records imported: {total_records:,}")
        
        # Show sample companies
        cursor.execute("SELECT symbol, long_name FROM companies LIMIT 5")
        companies = cursor.fetchall()
        
        logger.info("🏢 Sample companies:")
        for symbol, name in companies:
            logger.info(f"  {symbol}: {name}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Verification error: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python import_database.py <sql_file>")
        print("Example: python import_database.py database_export_20241225_143022.sql")
        sys.exit(1)
    
    sql_file = sys.argv[1]
    
    print("📥 Database Import Utility")
    print("=" * 50)
    
    # Import database
    if import_database_from_sql(sql_file):
        print("\n🔍 Verifying import...")
        verify_import()
        print("\n✅ Import completed successfully!")
        print("\nYour remixed instance now has the same data as the original!")
    else:
        print("\n❌ Import failed.")
        print("\nAlternative: Run fresh download instead:")
        print("python main_data_loader.py --download-nse")
