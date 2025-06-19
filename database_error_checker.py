
#!/usr/bin/env python3
"""
Comprehensive Database Error and Accuracy Checker
Validates data quality, consistency, and accuracy in the YFinance database
"""

import psycopg2
import pandas as pd
import os
from datetime import datetime, timedelta
from database_config import get_database_config
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseErrorChecker:
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        if self.database_url:
            self.conn = psycopg2.connect(self.database_url)
        else:
            db_config = get_database_config()
            self.conn = psycopg2.connect(**db_config)
        
        self.cursor = self.conn.cursor()
        self.errors = []
        self.warnings = []
        
    def check_data_integrity(self):
        """Check referential integrity and data consistency"""
        print("🔍 CHECKING DATA INTEGRITY")
        print("=" * 50)
        
        # 1. Check for orphaned records
        orphaned_queries = {
            'price_history': """
                SELECT COUNT(*) FROM price_history ph 
                LEFT JOIN companies c ON ph.company_id = c.id 
                WHERE c.id IS NULL
            """,
            'company_metrics': """
                SELECT COUNT(*) FROM company_metrics cm 
                LEFT JOIN companies c ON cm.company_id = c.id 
                WHERE c.id IS NULL
            """,
            'income_statements': """
                SELECT COUNT(*) FROM income_statements i 
                LEFT JOIN companies c ON i.company_id = c.id 
                WHERE c.id IS NULL
            """
        }
        
        for table, query in orphaned_queries.items():
            self.cursor.execute(query)
            orphaned_count = self.cursor.fetchone()[0]
            if orphaned_count > 0:
                self.errors.append(f"❌ {table}: {orphaned_count} orphaned records")
                print(f"❌ {table}: {orphaned_count} orphaned records")
            else:
                print(f"✅ {table}: No orphaned records")
    
    def check_data_quality(self):
        """Check for data quality issues"""
        print("\n📊 CHECKING DATA QUALITY")
        print("=" * 50)
        
        # 1. Check for negative prices
        self.cursor.execute("""
            SELECT COUNT(*), COUNT(DISTINCT company_id) 
            FROM price_history 
            WHERE open_price < 0 OR high_price < 0 OR low_price < 0 OR close_price < 0
        """)
        negative_prices, companies_affected = self.cursor.fetchone()
        if negative_prices > 0:
            self.errors.append(f"❌ Negative prices: {negative_prices} records in {companies_affected} companies")
            print(f"❌ Negative prices: {negative_prices} records in {companies_affected} companies")
        else:
            print("✅ No negative prices found")
        
        # 2. Check for invalid high/low relationships
        self.cursor.execute("""
            SELECT COUNT(*), COUNT(DISTINCT company_id) 
            FROM price_history 
            WHERE high_price < low_price
        """)
        invalid_highs, companies_affected = self.cursor.fetchone()
        if invalid_highs > 0:
            self.errors.append(f"❌ Invalid high < low: {invalid_highs} records in {companies_affected} companies")
            print(f"❌ Invalid high < low: {invalid_highs} records in {companies_affected} companies")
        else:
            print("✅ All high/low price relationships are valid")
        
        # 3. Check for extreme price movements (>90% in one day)
        self.cursor.execute("""
            SELECT symbol, date, 
                   ROUND(ABS((close_price - open_price) / open_price * 100), 2) as daily_change
            FROM price_history ph
            JOIN companies c ON ph.company_id = c.id
            WHERE open_price > 0 
              AND ABS((close_price - open_price) / open_price) > 0.9
            ORDER BY daily_change DESC
            LIMIT 10
        """)
        extreme_moves = self.cursor.fetchall()
        if extreme_moves:
            self.warnings.append(f"⚠️ {len(extreme_moves)} extreme price movements (>90% daily change)")
            print(f"⚠️ Extreme price movements found:")
            for symbol, date, change in extreme_moves[:5]:
                print(f"   {symbol} on {date}: {change}%")
        else:
            print("✅ No extreme price movements detected")
        
        # 4. Check for missing volume data
        self.cursor.execute("""
            SELECT COUNT(*), COUNT(DISTINCT company_id) 
            FROM price_history 
            WHERE volume IS NULL OR volume = 0
        """)
        missing_volume, companies_affected = self.cursor.fetchone()
        if missing_volume > 0:
            self.warnings.append(f"⚠️ Missing volume: {missing_volume} records in {companies_affected} companies")
            print(f"⚠️ Missing volume: {missing_volume} records in {companies_affected} companies")
        else:
            print("✅ All records have volume data")
    
    def check_financial_data_consistency(self):
        """Check financial statements for consistency"""
        print("\n💰 CHECKING FINANCIAL DATA CONSISTENCY")
        print("=" * 50)
        
        # 1. Check balance sheet equation: Assets = Liabilities + Equity
        self.cursor.execute("""
            SELECT c.symbol, bs.period_ending,
                   bs.total_assets, bs.total_liabilities, bs.stockholders_equity,
                   ROUND(ABS(bs.total_assets - (bs.total_liabilities + bs.stockholders_equity)), 2) as imbalance
            FROM balance_sheets bs
            JOIN companies c ON bs.company_id = c.id
            WHERE bs.total_assets IS NOT NULL 
              AND bs.total_liabilities IS NOT NULL 
              AND bs.stockholders_equity IS NOT NULL
              AND ABS(bs.total_assets - (bs.total_liabilities + bs.stockholders_equity)) > 1000000
            ORDER BY imbalance DESC
            LIMIT 10
        """)
        balance_errors = self.cursor.fetchall()
        if balance_errors:
            self.warnings.append(f"⚠️ Balance sheet imbalances found in {len(balance_errors)} records")
            print(f"⚠️ Balance sheet imbalances:")
            for symbol, period, assets, liab, equity, imbalance in balance_errors[:3]:
                print(f"   {symbol} ({period}): Imbalance of {imbalance:,.0f}")
        else:
            print("✅ Balance sheets are mathematically consistent")
        
        # 2. Check for negative equity
        self.cursor.execute("""
            SELECT COUNT(*), COUNT(DISTINCT company_id)
            FROM balance_sheets 
            WHERE stockholders_equity < 0
        """)
        negative_equity, companies_affected = self.cursor.fetchone()
        if negative_equity > 0:
            self.warnings.append(f"⚠️ Negative equity: {negative_equity} records in {companies_affected} companies")
            print(f"⚠️ Negative equity found in {negative_equity} records ({companies_affected} companies)")
        else:
            print("✅ No negative equity found")
        
        # 3. Check cash flow consistency
        self.cursor.execute("""
            SELECT COUNT(*)
            FROM cash_flow_statements
            WHERE operating_cash_flow IS NOT NULL 
              AND investing_cash_flow IS NOT NULL 
              AND financing_cash_flow IS NOT NULL
              AND changes_in_cash IS NOT NULL
              AND ABS((operating_cash_flow + investing_cash_flow + financing_cash_flow) - changes_in_cash) > 1000000
        """)
        cash_flow_errors = self.cursor.fetchone()[0]
        if cash_flow_errors > 0:
            self.warnings.append(f"⚠️ Cash flow inconsistencies: {cash_flow_errors} records")
            print(f"⚠️ Cash flow inconsistencies in {cash_flow_errors} records")
        else:
            print("✅ Cash flow statements are consistent")
    
    def check_data_completeness(self):
        """Check for data completeness"""
        print("\n📈 CHECKING DATA COMPLETENESS")
        print("=" * 50)
        
        # 1. Companies without price data
        self.cursor.execute("""
            SELECT COUNT(*)
            FROM companies c
            LEFT JOIN price_history ph ON c.id = ph.company_id
            WHERE ph.company_id IS NULL
        """)
        no_price_data = self.cursor.fetchone()[0]
        if no_price_data > 0:
            self.warnings.append(f"⚠️ {no_price_data} companies without price data")
            print(f"⚠️ {no_price_data} companies without price data")
        else:
            print("✅ All companies have price data")
        
        # 2. Companies without recent data (last 30 days)
        self.cursor.execute("""
            SELECT COUNT(DISTINCT c.id)
            FROM companies c
            LEFT JOIN price_history ph ON c.id = ph.company_id
            WHERE ph.date < CURRENT_DATE - INTERVAL '30 days' OR ph.date IS NULL
        """)
        stale_data = self.cursor.fetchone()[0]
        if stale_data > 0:
            self.warnings.append(f"⚠️ {stale_data} companies with stale price data (>30 days)")
            print(f"⚠️ {stale_data} companies with stale price data (>30 days)")
        else:
            print("✅ All companies have recent price data")
        
        # 3. Companies without financial statements
        self.cursor.execute("""
            SELECT COUNT(*)
            FROM companies c
            LEFT JOIN income_statements i ON c.id = i.company_id
            WHERE i.company_id IS NULL
        """)
        no_financials = self.cursor.fetchone()[0]
        if no_financials > 0:
            self.warnings.append(f"⚠️ {no_financials} companies without financial statements")
            print(f"⚠️ {no_financials} companies without financial statements")
        else:
            print("✅ All companies have financial statements")
    
    def check_duplicate_data(self):
        """Check for duplicate records"""
        print("\n🔄 CHECKING FOR DUPLICATES")
        print("=" * 50)
        
        # 1. Duplicate price records
        self.cursor.execute("""
            SELECT company_id, date, COUNT(*)
            FROM price_history
            GROUP BY company_id, date
            HAVING COUNT(*) > 1
            LIMIT 10
        """)
        price_duplicates = self.cursor.fetchall()
        if price_duplicates:
            self.errors.append(f"❌ Duplicate price records found: {len(price_duplicates)} instances")
            print(f"❌ Duplicate price records: {len(price_duplicates)} instances")
        else:
            print("✅ No duplicate price records")
        
        # 2. Duplicate company records
        self.cursor.execute("""
            SELECT symbol, COUNT(*)
            FROM companies
            GROUP BY symbol
            HAVING COUNT(*) > 1
        """)
        company_duplicates = self.cursor.fetchall()
        if company_duplicates:
            self.errors.append(f"❌ Duplicate companies: {len(company_duplicates)} symbols")
            print(f"❌ Duplicate companies: {len(company_duplicates)} symbols")
        else:
            print("✅ No duplicate companies")
    
    def check_data_ranges(self):
        """Check for data within reasonable ranges"""
        print("\n📊 CHECKING DATA RANGES")
        print("=" * 50)
        
        # 1. Check PE ratios
        self.cursor.execute("""
            SELECT COUNT(*), MIN(trailing_pe), MAX(trailing_pe)
            FROM company_metrics
            WHERE trailing_pe < 0 OR trailing_pe > 1000
        """)
        pe_issues, min_pe, max_pe = self.cursor.fetchone()
        if pe_issues > 0:
            self.warnings.append(f"⚠️ Unusual PE ratios: {pe_issues} records (range: {min_pe} to {max_pe})")
            print(f"⚠️ Unusual PE ratios: {pe_issues} records (range: {min_pe} to {max_pe})")
        else:
            print("✅ All PE ratios are within reasonable ranges")
        
        # 2. Check market cap values
        self.cursor.execute("""
            SELECT COUNT(*), MIN(market_cap), MAX(market_cap)
            FROM company_metrics
            WHERE market_cap <= 0 OR market_cap > 10000000000000
        """)
        mcap_issues, min_mcap, max_mcap = self.cursor.fetchone()
        if mcap_issues > 0:
            self.warnings.append(f"⚠️ Unusual market caps: {mcap_issues} records")
            print(f"⚠️ Unusual market caps: {mcap_issues} records")
        else:
            print("✅ All market caps are within reasonable ranges")
    
    def generate_summary_report(self):
        """Generate summary report"""
        print("\n📋 DATABASE ACCURACY SUMMARY")
        print("=" * 60)
        
        # Get overall stats
        self.cursor.execute("""
            SELECT 
                (SELECT COUNT(*) FROM companies) as total_companies,
                (SELECT COUNT(*) FROM price_history) as total_price_records,
                (SELECT COUNT(DISTINCT company_id) FROM price_history) as companies_with_prices,
                (SELECT COUNT(*) FROM income_statements) as financial_records,
                (SELECT MAX(date) FROM price_history) as latest_price_date,
                (SELECT COUNT(DISTINCT company_id) FROM company_metrics) as companies_with_metrics
        """)
        stats = self.cursor.fetchone()
        
        print(f"Total Companies: {stats[0]:,}")
        print(f"Total Price Records: {stats[1]:,}")
        print(f"Companies with Prices: {stats[2]:,}")
        print(f"Companies with Metrics: {stats[5]:,}")
        print(f"Financial Records: {stats[3]:,}")
        print(f"Latest Price Date: {stats[4]}")
        
        print(f"\n🔍 ISSUES FOUND:")
        print(f"❌ Critical Errors: {len(self.errors)}")
        for error in self.errors:
            print(f"   {error}")
        
        print(f"⚠️ Warnings: {len(self.warnings)}")
        for warning in self.warnings:
            print(f"   {warning}")
        
        if len(self.errors) == 0 and len(self.warnings) == 0:
            print("🎉 EXCELLENT! No critical issues found in the database!")
        elif len(self.errors) == 0:
            print("✅ GOOD! No critical errors, only minor warnings.")
        else:
            print("⚠️ ATTENTION NEEDED! Critical errors require immediate attention.")
        
        return len(self.errors), len(self.warnings)
    
    def run_full_check(self):
        """Run comprehensive database check"""
        print("🚀 COMPREHENSIVE DATABASE ERROR & ACCURACY CHECK")
        print("=" * 80)
        print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        try:
            self.check_data_integrity()
            self.check_data_quality()
            self.check_financial_data_consistency()
            self.check_data_completeness()
            self.check_duplicate_data()
            self.check_data_ranges()
            
            errors, warnings = self.generate_summary_report()
            
            print(f"\n⏰ Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 80)
            
            return errors == 0 and warnings == 0
            
        except Exception as e:
            print(f"❌ Error during database check: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

if __name__ == "__main__":
    checker = DatabaseErrorChecker()
    try:
        checker.run_full_check()
    finally:
        checker.close()
