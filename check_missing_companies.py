
#!/usr/bin/env python3
"""
Check which NSE companies are missing from the database
"""

import os
import psycopg2
from database_config import get_database_config

def check_missing_companies():
    """Check which companies from NSE list are missing from database"""
    try:
        # Connect to database
        database_url = os.getenv('DATABASE_URL')
        if database_url:
            conn = psycopg2.connect(database_url)
        else:
            db_config = get_database_config()
            conn = psycopg2.connect(**db_config)
        
        # Get existing companies from database
        cursor = conn.cursor()
        cursor.execute("SELECT symbol FROM companies ORDER BY symbol")
        existing_symbols = {row[0] for row in cursor.fetchall()}
        
        # Get companies with recent price data
        cursor.execute("""
            SELECT DISTINCT c.symbol 
            FROM companies c 
            JOIN price_history ph ON c.id = ph.company_id 
            WHERE ph.date >= CURRENT_DATE - INTERVAL '30 days'
            ORDER BY c.symbol
        """)
        recent_data_symbols = {row[0] for row in cursor.fetchall()}
        
        # Get all NSE symbols
        symbols_file = "nse_complete_universe.txt"
        if os.path.exists(symbols_file):
            with open(symbols_file, 'r') as f:
                all_nse_symbols = {line.strip() for line in f.readlines() if line.strip()}
        else:
            print("❌ NSE symbols file not found. Run the downloader first to generate it.")
            return
        
        # Calculate missing companies
        missing_companies = all_nse_symbols - existing_symbols
        stale_companies = existing_symbols - recent_data_symbols
        
        print("=" * 60)
        print("📊 NSE COMPANIES STATUS REPORT")
        print("=" * 60)
        print(f"Total NSE companies in list: {len(all_nse_symbols):,}")
        print(f"Companies in database: {len(existing_symbols):,}")
        print(f"Companies with recent data (<30 days): {len(recent_data_symbols):,}")
        print()
        
        print(f"🆕 Missing companies (need to download): {len(missing_companies):,}")
        if missing_companies and len(missing_companies) <= 20:
            print("Missing companies:")
            for symbol in sorted(missing_companies)[:20]:
                print(f"  - {symbol}")
            if len(missing_companies) > 20:
                print(f"  ... and {len(missing_companies) - 20} more")
        elif len(missing_companies) > 20:
            print(f"Too many to list ({len(missing_companies)} companies)")
        
        print()
        print(f"⚠️ Companies with stale data (>30 days old): {len(stale_companies):,}")
        if stale_companies and len(stale_companies) <= 10:
            print("Companies with stale data:")
            for symbol in sorted(stale_companies)[:10]:
                print(f"  - {symbol}")
            if len(stale_companies) > 10:
                print(f"  ... and {len(stale_companies) - 10} more")
        elif len(stale_companies) > 10:
            print(f"Too many to list ({len(stale_companies)} companies)")
        
        print()
        print("📋 RECOMMENDED ACTIONS:")
        if missing_companies:
            print(f"1. Run: python main_data_loader.py --download-nse")
            print(f"   This will download {len(missing_companies)} missing companies")
        else:
            print("1. ✅ All NSE companies are in the database")
        
        if stale_companies:
            print(f"2. Consider updating {len(stale_companies)} companies with stale data")
        else:
            print("2. ✅ All companies have recent data")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error checking companies: {e}")

if __name__ == "__main__":
    check_missing_companies()
