
#!/usr/bin/env python3
"""
Comprehensive System Verification and Fix Script
Analyzes and fixes issues with NSE data system
"""

import psycopg2
import os
import subprocess
import logging
from datetime import datetime

class SystemVerifier:
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
    
    def verify_symbol_files(self):
        """Verify symbol files and fix if needed"""
        print("\n🔍 VERIFYING SYMBOL FILES")
        print("=" * 50)
        
        issues = []
        
        # Check nse_complete_universe.txt
        if os.path.exists('nse_complete_universe.txt'):
            with open('nse_complete_universe.txt', 'r') as f:
                universe_symbols = [line.strip() for line in f.readlines() if line.strip()]
            print(f"✅ nse_complete_universe.txt: {len(universe_symbols)} symbols")
            
            if len(universe_symbols) < 2000:
                issues.append("Universe file has fewer than 2000 symbols")
        else:
            issues.append("nse_complete_universe.txt not found")
            universe_symbols = []
        
        # Check nse_symbols.txt
        if os.path.exists('nse_symbols.txt'):
            with open('nse_symbols.txt', 'r') as f:
                basic_symbols = [line.strip() for line in f.readlines() if line.strip()]
            print(f"✅ nse_symbols.txt: {len(basic_symbols)} symbols")
        else:
            issues.append("nse_symbols.txt not found")
            basic_symbols = []
        
        return issues, universe_symbols, basic_symbols
    
    def verify_database_population(self):
        """Verify database population"""
        print("\n📊 VERIFYING DATABASE POPULATION")
        print("=" * 50)
        
        cursor = self.conn.cursor()
        issues = []
        
        # Check company count
        cursor.execute("SELECT COUNT(*) FROM companies")
        company_count = cursor.fetchone()[0]
        print(f"Companies in database: {company_count}")
        
        if company_count < 2000:
            issues.append(f"Only {company_count} companies in database, expected 2000+")
        
        # Check data completeness
        cursor.execute("""
            SELECT 
                (SELECT COUNT(*) FROM companies) as companies,
                (SELECT COUNT(*) FROM price_history) as price_records,
                (SELECT COUNT(DISTINCT company_id) FROM price_history) as companies_with_prices,
                (SELECT COUNT(*) FROM income_statements) as income_statements,
                (SELECT COUNT(DISTINCT company_id) FROM income_statements) as companies_with_financials
        """)
        
        stats = cursor.fetchone()
        print(f"Companies with price data: {stats[2]}/{stats[0]} ({stats[2]/stats[0]*100:.1f}%)")
        print(f"Companies with financials: {stats[4]}/{stats[0]} ({stats[4]/stats[0]*100:.1f}%)")
        print(f"Total price records: {stats[1]:,}")
        print(f"Total income statements: {stats[3]:,}")
        
        if stats[2] < stats[0] * 0.8:  # Less than 80% have price data
            issues.append(f"Only {stats[2]} companies have price data")
        
        cursor.close()
        return issues, stats
    
    def verify_running_processes(self):
        """Verify which processes are running"""
        print("\n🔄 VERIFYING RUNNING PROCESSES")
        print("=" * 50)
        
        issues = []
        
        try:
            result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
            python_processes = [line for line in result.stdout.split('\n') if 'python' in line.lower()]
            
            downloader_processes = [p for p in python_processes if 'yfinance_nse_downloader' in p]
            
            if downloader_processes:
                print("✅ Found running download processes:")
                for process in downloader_processes:
                    print(f"  {process}")
                
                # Check which file they're using
                for process in downloader_processes:
                    if 'nse_complete_universe' in process:
                        print("✅ Process is using complete universe file")
                    else:
                        issues.append("Downloader process not using complete universe file")
            else:
                issues.append("No download processes currently running")
                print("❌ No download processes running")
        
        except Exception as e:
            issues.append(f"Error checking processes: {e}")
        
        return issues
    
    def fix_symbol_files(self):
        """Generate complete symbol files if needed"""
        print("\n🔧 FIXING SYMBOL FILES")
        print("=" * 50)
        
        try:
            from nse_symbols_fetcher import NSESymbolsFetcher
            fetcher = NSESymbolsFetcher()
            
            # Generate complete universe
            print("Generating complete NSE universe...")
            all_symbols = fetcher.get_all_nse_symbols(complete_universe=True)
            fetcher.save_symbols_to_file(all_symbols, "nse_complete_universe.txt")
            
            print(f"✅ Generated nse_complete_universe.txt with {len(all_symbols)} symbols")
            return True
        
        except Exception as e:
            print(f"❌ Error generating symbol files: {e}")
            return False
    
    def fix_downloader_configuration(self):
        """Fix downloader to use complete universe"""
        print("\n🔧 FIXING DOWNLOADER CONFIGURATION")
        print("=" * 50)
        
        # Check current downloader configuration
        with open('yfinance_nse_downloader.py', 'r') as f:
            content = f.read()
        
        # Look for the download_all_nse_stocks method
        if 'symbols_file: str = "nse_complete_universe.txt"' in content:
            print("✅ Downloader already configured to use complete universe")
            return True
        else:
            print("❌ Downloader is not using complete universe file")
            print("The downloader needs to be updated to use nse_complete_universe.txt by default")
            return False
    
    def recommend_actions(self, all_issues):
        """Recommend actions based on found issues"""
        print("\n💡 RECOMMENDED ACTIONS")
        print("=" * 50)
        
        if not all_issues:
            print("✅ No issues found! System is working correctly.")
            return
        
        print("🚨 Issues found:")
        for i, issue in enumerate(all_issues, 1):
            print(f"  {i}. {issue}")
        
        print("\n🔧 Recommended fixes:")
        print("1. Stop any running processes:")
        print("   pkill -f yfinance_nse_downloader")
        
        print("2. Start the corrected downloader:")
        print("   python yfinance_nse_downloader.py")
        
        print("3. Monitor progress:")
        print("   python comprehensive_status_checker.py")
        
        print("4. Check logs:")
        print("   tail -f yfinance_nse_downloader.log")
    
    def run_verification(self):
        """Run complete verification"""
        print("🚀 COMPREHENSIVE SYSTEM VERIFICATION")
        print("=" * 60)
        print(f"⏰ Verification started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        all_issues = []
        
        # 1. Verify symbol files
        symbol_issues, universe_symbols, basic_symbols = self.verify_symbol_files()
        all_issues.extend(symbol_issues)
        
        # 2. Verify database population
        db_issues, db_stats = self.verify_database_population()
        all_issues.extend(db_issues)
        
        # 3. Verify running processes
        process_issues = self.verify_running_processes()
        all_issues.extend(process_issues)
        
        # 4. Fix symbol files if needed
        if any("universe" in issue for issue in symbol_issues):
            if self.fix_symbol_files():
                print("✅ Symbol files fixed")
            else:
                all_issues.append("Failed to fix symbol files")
        
        # 5. Check downloader configuration
        if not self.fix_downloader_configuration():
            all_issues.append("Downloader not configured for complete universe")
        
        # 6. Provide recommendations
        self.recommend_actions(all_issues)
        
        print("\n" + "=" * 60)
        print("✅ Verification completed!")
        
        return len(all_issues) == 0
    
    def close(self):
        if self.conn:
            self.conn.close()

if __name__ == "__main__":
    verifier = SystemVerifier()
    try:
        success = verifier.run_verification()
        exit_code = 0 if success else 1
    finally:
        verifier.close()
    
    exit(exit_code)
