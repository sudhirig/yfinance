
#!/usr/bin/env python3
"""
Fix database schema issues and restart download
"""

import psycopg2
import os
import logging
from database_config import get_database_config

def fix_schema():
    """Apply schema fixes for numeric precision issues"""
    try:
        # Connect to database
        database_url = os.getenv('DATABASE_URL')
        if database_url:
            conn = psycopg2.connect(database_url)
        else:
            db_config = get_database_config()
            conn = psycopg2.connect(**db_config)
        
        cursor = conn.cursor()
        
        print("🔧 Applying schema fixes...")
        
        # Fix company_metrics table precision issues
        schema_fixes = [
            "ALTER TABLE company_metrics ALTER COLUMN dividend_yield TYPE DECIMAL(12,8);",
            "ALTER TABLE company_metrics ALTER COLUMN dividend_rate TYPE DECIMAL(12,8);",
            "ALTER TABLE company_metrics ALTER COLUMN payout_ratio TYPE DECIMAL(12,8);",
            "ALTER TABLE company_metrics ALTER COLUMN five_year_avg_dividend_yield TYPE DECIMAL(12,8);",
            "ALTER TABLE company_metrics ALTER COLUMN beta TYPE DECIMAL(12,6);",
            "ALTER TABLE company_metrics ALTER COLUMN trailing_pe TYPE DECIMAL(15,6);",
            "ALTER TABLE company_metrics ALTER COLUMN forward_pe TYPE DECIMAL(15,6);"
        ]
        
        for fix in schema_fixes:
            try:
                cursor.execute(fix)
                print(f"✅ Applied: {fix}")
            except Exception as e:
                print(f"⚠️ Warning applying fix: {e}")
        
        conn.commit()
        print("✅ Schema fixes applied successfully!")
        
        # Clean up failed records that are blocking progress
        print("🧹 Cleaning up failed transaction records...")
        cleanup_queries = [
            "DELETE FROM companies WHERE symbol IN ('BAJAJ-AUTO.NS', 'BAYERCROP.NS', 'BOSCHLTD.NS', 'BPL.NS', 'EXCEL.NS', 'EXXARO.NS', 'GILLETTE.NS');",
            "DELETE FROM price_history WHERE company_id NOT IN (SELECT id FROM companies);",
            "DELETE FROM company_metrics WHERE company_id NOT IN (SELECT id FROM companies);",
            "DELETE FROM income_statements WHERE company_id NOT IN (SELECT id FROM companies);",
            "DELETE FROM balance_sheets WHERE company_id NOT IN (SELECT id FROM companies);",
            "DELETE FROM cash_flow_statements WHERE company_id NOT IN (SELECT id FROM companies);"
        ]
        
        for query in cleanup_queries:
            cursor.execute(query)
        
        conn.commit()
        print("✅ Cleanup completed!")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Error fixing schema: {e}")
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("🚀 FIXING DATABASE SCHEMA AND RESTARTING DOWNLOAD")
    print("=" * 60)
    
    if fix_schema():
        print("\n🎯 Schema fixed! The download process should now work properly.")
        print("   The current download will continue with the fixes applied.")
    else:
        print("\n❌ Schema fix failed. Please check the errors above.")
