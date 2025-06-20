
#!/usr/bin/env python3
"""
Export database data to SQL dump for importing into remixed instance
"""

import os
import subprocess
import psycopg2
from datetime import datetime
import logging

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

def export_database_to_sql():
    """Export the entire database to a SQL file"""
    logger = setup_logging()
    
    try:
        # Get database URL
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            logger.error("DATABASE_URL not found")
            return False
        
        # Create export filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        export_file = f"database_export_{timestamp}.sql"
        
        logger.info(f"Starting database export to {export_file}")
        
        # Use pg_dump to export database
        command = [
            'pg_dump',
            database_url,
            '--clean',
            '--if-exists',
            '--no-owner',
            '--no-privileges',
            '--file', export_file
        ]
        
        result = subprocess.run(command, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info(f"✅ Database exported successfully to {export_file}")
            logger.info(f"File size: {os.path.getsize(export_file) / (1024*1024):.2f} MB")
            return export_file
        else:
            logger.error(f"❌ Export failed: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Export error: {e}")
        return False

def get_table_counts():
    """Get row counts for verification"""
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
        
        logger.info("📊 Current database contents:")
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                logger.info(f"  {table}: {count:,} records")
            except Exception as e:
                logger.warning(f"  {table}: Error getting count - {e}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error getting table counts: {e}")

if __name__ == "__main__":
    print("🔄 Database Export Utility")
    print("=" * 50)
    
    # Show current data
    get_table_counts()
    print()
    
    # Export database
    export_file = export_database_to_sql()
    
    if export_file:
        print(f"""
✅ Export completed successfully!

To import into your remixed instance:
1. Download the file: {export_file}
2. Upload it to your remixed instance
3. Run: python import_database.py {export_file}

Or use the re-download approach for fresh data:
python main_data_loader.py --download-nse
        """)
    else:
        print("❌ Export failed. Try the re-download approach instead.")
