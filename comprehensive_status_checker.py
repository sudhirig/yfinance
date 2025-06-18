
#!/usr/bin/env python3
"""
Comprehensive Status Checker for NSE Stock Database System
Analyzes if 2000+ NSE symbols are being processed and stored correctly
"""

import psycopg2
import os
import pandas as pd
from datetime import datetime, timedelta
import logging

class ComprehensiveStatusChecker:
    def __init__(self):
        self.setup_logging()
        self.connect_db()
    
    def setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
    
    def connect_db(self):
        try:
            database_url = os.getenv('DATABASE_URL')
            if database_url:
                self.conn = psycopg2.connect(database_url)
            else:
                from database_config import get_database_config
                db_config = get_database_config()
                self.conn = psycopg2.connect(**db_config)
            self.logger.info("✅ Database connection established")
        except Exception as e:
            self.logger.error(f"❌ Database connection failed: {e}")
            raise
    
    def check_symbol_files(self):
        """Check symbol files status"""
        print("\n🔍 SYMBOL FILES STATUS")
        print("=" * 50)
        
        # Check nse_complete_universe.txt
        if os.path.exists('nse_complete_universe.txt'):
            with open('nse_complete_universe.txt', 'r') as f:
                universe_symbols = [line.strip() for line in f.readlines() if line.strip()]
            print(f"✅ nse_complete_universe.txt: {len(universe_symbols)} symbols")
            print(f"   Sample symbols: {', '.join(universe_symbols[:5])}")
        else:
            print("❌ nse_complete_universe.txt not found")
        
        # Check nse_symbols.txt
        if os.path.exists('nse_symbols.txt'):
            with open('nse_symbols.txt', 'r') as f:
                basic_symbols = [line.strip() for line in f.readlines() if line.strip()]
            print(f"✅ nse_symbols.txt: {len(basic_symbols)} symbols")
        else:
            print("❌ nse_symbols.txt not found")
        
        return universe_symbols if 'universe_symbols' in locals() else []
    
    def check_database_tables(self):
        """Check all database tables and their data"""
        print("\n📊 DATABASE TABLES STATUS")
        print("=" * 50)
        
        cursor = self.conn.cursor()
        
        tables = [
            'companies', 'price_history', 'company_metrics', 
            'income_statements', 'balance_sheets', 'cash_flow_statements',
            'corporate_actions', 'earnings', 'holders', 'data_updates'
        ]
        
        table_stats = {}
        
        for table in tables:
            try:
                # Get basic count
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                total_records = cursor.fetchone()[0]
                
                # Get unique companies if company_id exists
                try:
                    cursor.execute(f"""
                        SELECT column_name FROM information_schema.columns 
                        WHERE table_name = '{table}' AND column_name = 'company_id'
                    """)
                    has_company_id = cursor.fetchone() is not None
                    
                    if has_company_id:
                        cursor.execute(f"SELECT COUNT(DISTINCT company_id) FROM {table}")
                        unique_companies = cursor.fetchone()[0]
                    else:
                        unique_companies = total_records if table == 'companies' else 'N/A'
                except Exception as e:
                    unique_companies = 'N/A'
                
                table_stats[table] = {
                    'records': total_records,
                    'companies': unique_companies
                }
                
                print(f"✅ {table:<20}: {total_records:>8} records, {unique_companies:>4} companies")
                
            except Exception as e:
                print(f"❌ {table:<20}: Error - {e}")
                table_stats[table] = {'records': 0, 'companies': 0}
        
        cursor.close()
        return table_stats
    
    def check_data_quality(self):
        """Check data quality and completeness"""
        print("\n🔍 DATA QUALITY ANALYSIS")
        print("=" * 50)
        
        cursor = self.conn.cursor()
        
        # Check companies with different types of data
        queries = {
            'Total Companies': "SELECT COUNT(*) FROM companies",
            'With Symbol': "SELECT COUNT(*) FROM companies WHERE symbol IS NOT NULL",
            'With Company Name': "SELECT COUNT(*) FROM companies WHERE long_name IS NOT NULL",
            'With Sector Info': "SELECT COUNT(*) FROM companies WHERE sector IS NOT NULL",
            'With Price Data': """
                SELECT COUNT(DISTINCT c.id) FROM companies c 
                INNER JOIN price_history ph ON c.id = ph.company_id
            """,
            'With Metrics': """
                SELECT COUNT(DISTINCT c.id) FROM companies c 
                INNER JOIN company_metrics cm ON c.id = cm.company_id
            """,
            'With Financial Data': """
                SELECT COUNT(DISTINCT c.id) FROM companies c 
                WHERE EXISTS (
                    SELECT 1 FROM income_statements i WHERE i.company_id = c.id
                    OR EXISTS (SELECT 1 FROM balance_sheets b WHERE b.company_id = c.id)
                    OR EXISTS (SELECT 1 FROM cash_flow_statements cf WHERE cf.company_id = c.id)
                )
            """,
            'With Corporate Actions': """
                SELECT COUNT(DISTINCT c.id) FROM companies c 
                INNER JOIN corporate_actions ca ON c.id = ca.company_id
            """
        }
        
        results = {}
        for description, query in queries.items():
            try:
                cursor.execute(query)
                count = cursor.fetchone()[0]
                results[description] = count
                print(f"{description:<25}: {count:>6}")
            except Exception as e:
                print(f"{description:<25}: Error - {e}")
                results[description] = 0
        
        cursor.close()
        return results
    
    def check_recent_activity(self):
        """Check recent download/update activity"""
        print("\n⏰ RECENT ACTIVITY STATUS")
        print("=" * 50)
        
        cursor = self.conn.cursor()
        
        try:
            # Check data_updates table for recent activity
            cursor.execute("""
                SELECT 
                    table_name,
                    COUNT(*) as updates,
                    MAX(update_timestamp) as last_update,
                    SUM(records_affected) as total_records
                FROM data_updates 
                WHERE update_timestamp >= NOW() - INTERVAL '24 hours'
                GROUP BY table_name
                ORDER BY last_update DESC
            """)
            
            recent_updates = cursor.fetchall()
            if recent_updates:
                print("Recent activity (last 24 hours):")
                for table, updates, last_update, records in recent_updates:
                    print(f"  {table:<20}: {updates:>3} updates, {records:>6} records, last: {last_update}")
            else:
                print("❌ No recent activity found in last 24 hours")
            
            # Check latest price data
            cursor.execute("""
                SELECT 
                    c.symbol,
                    MAX(ph.date) as latest_date,
                    COUNT(ph.id) as price_records
                FROM companies c
                JOIN price_history ph ON c.id = ph.company_id
                GROUP BY c.id, c.symbol
                ORDER BY latest_date DESC
                LIMIT 10
            """)
            
            latest_prices = cursor.fetchall()
            if latest_prices:
                print(f"\nLatest price data (top 10):")
                for symbol, latest_date, records in latest_prices:
                    print(f"  {symbol:<15}: {latest_date}, {records:>4} price records")
        
        except Exception as e:
            print(f"❌ Error checking recent activity: {e}")
        
        cursor.close()
    
    def check_running_processes(self):
        """Check if download processes are currently running"""
        print("\n🔄 RUNNING PROCESSES STATUS")
        print("=" * 50)
        
        import subprocess
        try:
            # Check for Python processes related to downloading
            result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
            python_processes = [line for line in result.stdout.split('\n') if 'python' in line.lower()]
            
            downloader_processes = [p for p in python_processes if any(keyword in p for keyword in [
                'yfinance_nse_downloader', 'main_data_loader', 'fetch_complete_nse'
            ])]
            
            if downloader_processes:
                print("✅ Found running download processes:")
                for process in downloader_processes:
                    print(f"  {process}")
            else:
                print("ℹ️  No download processes currently running")
        
        except Exception as e:
            print(f"❌ Error checking processes: {e}")
    
    def check_file_logs(self):
        """Check log files for status"""
        print("\n📋 LOG FILES STATUS")
        print("=" * 50)
        
        log_files = ['yfinance_nse_downloader.log', 'yfinance_loader.log']
        
        for log_file in log_files:
            if os.path.exists(log_file):
                try:
                    with open(log_file, 'r') as f:
                        lines = f.readlines()
                    
                    print(f"✅ {log_file}: {len(lines)} lines")
                    
                    # Show last few entries
                    if lines:
                        print(f"   Last entries:")
                        for line in lines[-3:]:
                            print(f"     {line.strip()}")
                except Exception as e:
                    print(f"❌ Error reading {log_file}: {e}")
            else:
                print(f"ℹ️  {log_file}: Not found")
    
    def generate_recommendations(self, symbol_count, table_stats, quality_results):
        """Generate recommendations based on analysis"""
        print("\n💡 RECOMMENDATIONS & NEXT STEPS")
        print("=" * 50)
        
        total_companies = quality_results.get('Total Companies', 0)
        with_price_data = quality_results.get('With Price Data', 0)
        with_financial_data = quality_results.get('With Financial Data', 0)
        
        # Calculate completion percentages
        price_completion = (with_price_data / symbol_count * 100) if symbol_count > 0 else 0
        financial_completion = (with_financial_data / symbol_count * 100) if symbol_count > 0 else 0
        
        print(f"📊 TARGET: {symbol_count} NSE symbols")
        print(f"📊 CURRENT: {total_companies} companies in database")
        print(f"📊 PROGRESS: {total_companies/symbol_count*100:.1f}% companies loaded")
        print(f"📊 PRICE DATA: {price_completion:.1f}% completion")
        print(f"📊 FINANCIAL DATA: {financial_completion:.1f}% completion")
        
        if total_companies < symbol_count * 0.1:  # Less than 10% loaded
            print("\n🚨 URGENT ACTIONS NEEDED:")
            print("1. Start the main downloader: python yfinance_nse_downloader.py")
            print("2. Monitor progress with: python check_data_status.py")
            print("3. Check logs for any errors")
        elif total_companies < symbol_count * 0.5:  # Less than 50% loaded
            print("\n⚠️  ACTIONS RECOMMENDED:")
            print("1. Continue running the downloader")
            print("2. Monitor for any stalled processes")
            print("3. Check for rate limiting issues")
        elif total_companies >= symbol_count * 0.8:  # 80% or more loaded
            print("\n✅ SYSTEM STATUS: GOOD")
            print("1. Download process is working well")
            print("2. Consider running data quality checks")
            print("3. Set up automated monitoring")
        
        print(f"\n🔧 QUICK COMMANDS:")
        print(f"• Check status: python check_data_status.py")
        print(f"• Start download: python yfinance_nse_downloader.py")
        print(f"• View data: python -c \"import psycopg2; print('Connect to view data')\"")
    
    def run_comprehensive_check(self):
        """Run all checks and generate report"""
        print("🚀 COMPREHENSIVE NSE DATA SYSTEM STATUS CHECK")
        print("=" * 60)
        from datetime import datetime
        import time
        current_time = datetime.now()
        print(f"⏰ Report generated: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏰ System time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        # 1. Check symbol files
        symbols = self.check_symbol_files()
        symbol_count = len(symbols)
        
        # 2. Check database tables
        table_stats = self.check_database_tables()
        
        # 3. Check data quality
        quality_results = self.check_data_quality()
        
        # 4. Check recent activity
        self.check_recent_activity()
        
        # 5. Check running processes
        self.check_running_processes()
        
        # 6. Check log files
        self.check_file_logs()
        
        # 7. Generate recommendations
        self.generate_recommendations(symbol_count, table_stats, quality_results)
        
        print("\n" + "=" * 60)
        print("✅ Comprehensive status check completed!")
    
    def close(self):
        if self.conn:
            self.conn.close()

if __name__ == "__main__":
    checker = ComprehensiveStatusChecker()
    try:
        checker.run_comprehensive_check()
    finally:
        checker.close()
