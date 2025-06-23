
#!/usr/bin/env python3
"""
Data Quality Maintenance Script
Regular maintenance tasks to keep data quality high
"""

import psycopg2
from datetime import datetime, timedelta
from database_config import get_db_connection
import logging

class DataQualityMaintenance:
    def __init__(self):
        self.conn = get_db_connection()
        self.cursor = self.conn.cursor()
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def daily_data_validation(self):
        """Run daily data validation checks"""
        print("🔍 DAILY DATA VALIDATION")
        print("=" * 40)
        
        issues = []
        
        # Check for new balance sheet imbalances
        self.cursor.execute("""
            SELECT COUNT(*) 
            FROM balance_sheets
            WHERE total_assets IS NOT NULL 
              AND total_liabilities IS NOT NULL 
              AND stockholders_equity IS NOT NULL
              AND ABS(total_assets - (total_liabilities + stockholders_equity)) > 1000000
              AND updated_at >= CURRENT_DATE - INTERVAL '1 day'
        """)
        new_imbalances = self.cursor.fetchone()[0]
        
        if new_imbalances > 0:
            issues.append(f"⚠️ {new_imbalances} new balance sheet imbalances")
        
        # Check for missing recent price data
        self.cursor.execute("""
            SELECT COUNT(DISTINCT c.id)
            FROM companies c
            LEFT JOIN price_history ph ON c.id = ph.company_id 
                AND ph.date >= CURRENT_DATE - INTERVAL '7 days'
            WHERE ph.company_id IS NULL
        """)
        missing_prices = self.cursor.fetchone()[0]
        
        if missing_prices > 10:  # More than 10 companies missing recent data
            issues.append(f"⚠️ {missing_prices} companies missing recent price data")
        
        # Check for duplicate records
        self.cursor.execute("""
            SELECT COUNT(*)
            FROM (
                SELECT company_id, date, COUNT(*) 
                FROM price_history 
                GROUP BY company_id, date 
                HAVING COUNT(*) > 1
            ) duplicates
        """)
        duplicate_prices = self.cursor.fetchone()[0]
        
        if duplicate_prices > 0:
            issues.append(f"⚠️ {duplicate_prices} duplicate price records")
        
        if issues:
            print("Issues found:")
            for issue in issues:
                print(f"  {issue}")
            return False
        else:
            print("✅ No data quality issues found")
            return True
    
    def auto_fix_common_issues(self):
        """Automatically fix common, safe issues"""
        print("\n🔧 AUTO-FIXING COMMON ISSUES")
        print("=" * 40)
        
        fixed_count = 0
        
        # Fix obvious high/low price swaps
        self.cursor.execute("""
            UPDATE price_history 
            SET high_price = low_price, low_price = high_price
            WHERE high_price < low_price 
              AND high_price > 0 AND low_price > 0
        """)
        price_swaps = self.cursor.rowcount
        fixed_count += price_swaps
        
        # Remove duplicate price records (keep latest)
        self.cursor.execute("""
            DELETE FROM price_history 
            WHERE id NOT IN (
                SELECT MAX(id) 
                FROM price_history 
                GROUP BY company_id, date
            )
        """)
        duplicates_removed = self.cursor.rowcount
        fixed_count += duplicates_removed
        
        # Update working capital where missing but components exist
        self.cursor.execute("""
            UPDATE balance_sheets 
            SET working_capital = current_assets - current_liabilities,
                updated_at = CURRENT_TIMESTAMP
            WHERE working_capital IS NULL 
              AND current_assets IS NOT NULL 
              AND current_liabilities IS NOT NULL
        """)
        working_capital_fixed = self.cursor.rowcount
        fixed_count += working_capital_fixed
        
        if fixed_count > 0:
            print(f"✅ Auto-fixed {fixed_count} issues:")
            if price_swaps > 0:
                print(f"  - Fixed {price_swaps} high/low price swaps")
            if duplicates_removed > 0:
                print(f"  - Removed {duplicates_removed} duplicate records")
            if working_capital_fixed > 0:
                print(f"  - Calculated {working_capital_fixed} working capital values")
        else:
            print("✅ No common issues found to fix")
        
        return fixed_count
    
    def update_data_quality_metrics(self):
        """Update data quality metrics table"""
        print("\n📊 UPDATING QUALITY METRICS")
        print("=" * 40)
        
        # Create metrics table if it doesn't exist
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_quality_metrics (
                id SERIAL PRIMARY KEY,
                check_date DATE DEFAULT CURRENT_DATE,
                total_companies INTEGER,
                companies_with_prices INTEGER,
                companies_with_financials INTEGER,
                balance_sheet_imbalances INTEGER,
                price_data_issues INTEGER,
                overall_quality_score DECIMAL(5,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Calculate current metrics
        self.cursor.execute("""
            SELECT 
                (SELECT COUNT(*) FROM companies) as total_companies,
                (SELECT COUNT(DISTINCT company_id) FROM price_history) as companies_with_prices,
                (SELECT COUNT(DISTINCT company_id) FROM balance_sheets) as companies_with_financials,
                (SELECT COUNT(*) FROM balance_sheets 
                 WHERE total_assets IS NOT NULL AND total_liabilities IS NOT NULL 
                   AND stockholders_equity IS NOT NULL
                   AND ABS(total_assets - (total_liabilities + stockholders_equity)) > 1000000) as imbalances,
                (SELECT COUNT(*) FROM price_history 
                 WHERE high_price < low_price OR open_price < 0 OR high_price < 0 
                   OR low_price < 0 OR close_price < 0) as price_issues
        """)
        
        metrics = self.cursor.fetchone()
        total_companies, with_prices, with_financials, imbalances, price_issues = metrics
        
        # Calculate quality score
        price_coverage = (with_prices / total_companies * 100) if total_companies > 0 else 0
        financial_coverage = (with_financials / total_companies * 100) if total_companies > 0 else 0
        balance_quality = max(0, 100 - (imbalances / with_financials * 100)) if with_financials > 0 else 0
        price_quality = max(0, 100 - (price_issues / 1000)) if price_issues < 1000 else 0
        
        overall_score = (price_coverage + financial_coverage + balance_quality + price_quality) / 4
        
        # Insert metrics
        self.cursor.execute("""
            INSERT INTO data_quality_metrics (
                total_companies, companies_with_prices, companies_with_financials,
                balance_sheet_imbalances, price_data_issues, overall_quality_score
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (total_companies, with_prices, with_financials, imbalances, price_issues, overall_score))
        
        print(f"✅ Quality metrics updated:")
        print(f"  - Overall score: {overall_score:.1f}%")
        print(f"  - Price coverage: {price_coverage:.1f}%")
        print(f"  - Financial coverage: {financial_coverage:.1f}%")
        print(f"  - Balance sheet quality: {balance_quality:.1f}%")
        
        return overall_score
    
    def run_daily_maintenance(self):
        """Run daily maintenance routine"""
        print("🔄 DAILY DATA QUALITY MAINTENANCE")
        print("=" * 50)
        print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Step 1: Validate data
            is_healthy = self.daily_data_validation()
            
            # Step 2: Auto-fix common issues
            fixes_applied = self.auto_fix_common_issues()
            
            # Step 3: Update metrics
            quality_score = self.update_data_quality_metrics()
            
            # Commit changes
            self.conn.commit()
            
            print(f"\n✅ MAINTENANCE COMPLETE")
            print(f"   Data healthy: {'Yes' if is_healthy else 'No'}")
            print(f"   Fixes applied: {fixes_applied}")
            print(f"   Quality score: {quality_score:.1f}%")
            
        except Exception as e:
            print(f"❌ Error during maintenance: {e}")
            self.conn.rollback()
            raise
            
        finally:
            print(f"⏰ Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

if __name__ == "__main__":
    maintenance = DataQualityMaintenance()
    try:
        maintenance.run_daily_maintenance()
    finally:
        maintenance.close()
