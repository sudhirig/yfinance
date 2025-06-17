#!/usr/bin/env python3
"""
Quick script to check the current status of data loading
"""

import psycopg2
import os
from database_config import get_database_config

def check_data_status():
    """Check the current status of all tables"""
    try:
        # Connect to database
        database_url = os.getenv('DATABASE_URL')
        if database_url:
            conn = psycopg2.connect(database_url)
        else:
            db_config = get_database_config()
            conn = psycopg2.connect(**db_config)

        cursor = conn.cursor()

        print("📊 DATABASE STATUS REPORT")
        print("=" * 50)

        # Check each table
        tables_to_check = [
            ('Companies', 'companies'),
            ('Price History', 'price_history'),
            ('Company Metrics', 'company_metrics'),
            ('Income Statements', 'income_statements'),
            ('Balance Sheets', 'balance_sheets'),
            ('Cash Flow Statements', 'cash_flow_statements'),
            ('Corporate Actions', 'corporate_actions'),
        ]

        for table_name, table_sql_name in tables_to_check:
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT company_id) as unique_companies
                FROM {table_sql_name}
            """)
            total, companies = cursor.fetchone()
            print(f"{table_name:<20}: {total:>8} records, {companies:>4} companies")

        print("=" * 50)

        # Check companies with different types of data
        cursor.execute("""
            SELECT 
                'Companies' as data_type,
                COUNT(*) as count
            FROM companies

            UNION ALL

            SELECT 
                'With Price Data' as data_type,
                COUNT(DISTINCT c.id) as count
            FROM companies c
            INNER JOIN price_history ph ON c.id = ph.company_id

            UNION ALL

            SELECT 
                'With Financial Data' as data_type,
                COUNT(DISTINCT c.id) as count
            FROM companies c
            WHERE EXISTS (
                SELECT 1 FROM income_statements i WHERE i.company_id = c.id
                OR EXISTS (SELECT 1 FROM balance_sheets b WHERE b.company_id = c.id)
                OR EXISTS (SELECT 1 FROM cash_flow_statements cf WHERE cf.company_id = c.id)
            )

            UNION ALL

            SELECT 
                'With Corporate Actions' as data_type,
                COUNT(DISTINCT c.id) as count
            FROM companies c
            INNER JOIN corporate_actions ca ON c.id = ca.company_id
        """)

        results = cursor.fetchall()
        print("\n📈 DATA COVERAGE SUMMARY")
        print("-" * 30)
        for data_type, count in results:
            print(f"{data_type:<25}: {count:>4}")

        # Check latest updates
        cursor.execute("""
            SELECT 
                table_name,
                COUNT(*) as updates,
                MAX(update_timestamp) as last_update
            FROM data_updates
            GROUP BY table_name
            ORDER BY last_update DESC
            LIMIT 5
        """)

        updates = cursor.fetchall()
        if updates:
            print("\n🕒 RECENT UPDATE ACTIVITY")
            print("-" * 40)
            for table, count, last_update in updates:
                print(f"{table:<20}: {count:>3} updates (last: {last_update.strftime('%Y-%m-%d %H:%M')})")

        # Check price history data
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT c.id) as unique_companies,
                MIN(ph.date) as earliest_date,
                MAX(ph.date) as latest_date
            FROM price_history ph
            JOIN companies c ON c.id = ph.company_id
        """)
        total, companies, earliest_date, latest_date = cursor.fetchone()
        print("\n💰 PRICE HISTORY DATA")
        print("-" * 30)
        print(f"Total Records    : {total:>8}")
        print(f"Unique Companies : {companies:>4}")
        print(f"Earliest Date    : {earliest_date}")
        print(f"Latest Date      : {latest_date}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error checking data status: {e}")

if __name__ == "__main__":
    check_data_status()