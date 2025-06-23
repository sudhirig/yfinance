
#!/usr/bin/env python3
"""
Balance Sheet Data Quality Fixer
Identifies and fixes balance sheet inconsistencies and data quality issues
"""

import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime
from database_config import get_db_connection
import logging

class BalanceSheetFixer:
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
    
    def analyze_balance_sheet_issues(self):
        """Analyze the specific balance sheet inconsistencies"""
        print("🔍 ANALYZING BALANCE SHEET INCONSISTENCIES")
        print("=" * 60)
        
        # Find records with significant imbalances
        self.cursor.execute("""
            SELECT 
                c.symbol,
                bs.period_ending,
                bs.period_type,
                bs.total_assets,
                bs.total_liabilities,
                bs.stockholders_equity,
                (bs.total_assets - (bs.total_liabilities + bs.stockholders_equity)) as imbalance,
                ABS(bs.total_assets - (bs.total_liabilities + bs.stockholders_equity)) as abs_imbalance
            FROM balance_sheets bs
            JOIN companies c ON bs.company_id = c.id
            WHERE bs.total_assets IS NOT NULL 
              AND bs.total_liabilities IS NOT NULL 
              AND bs.stockholders_equity IS NOT NULL
              AND ABS(bs.total_assets - (bs.total_liabilities + bs.stockholders_equity)) > 1000000
            ORDER BY abs_imbalance DESC
            LIMIT 20
        """)
        
        issues = self.cursor.fetchall()
        
        print(f"Found {len(issues)} major imbalances (>1M):")
        for symbol, period, period_type, assets, liab, equity, imbalance, abs_imbalance in issues[:10]:
            print(f"  {symbol} ({period}, {period_type}): Imbalance {imbalance:,.0f}")
            print(f"    Assets: {assets:,.0f}, Liabilities: {liab:,.0f}, Equity: {equity:,.0f}")
        
        return issues
    
    def fix_balance_sheet_equation(self):
        """Fix balance sheet equations using intelligent logic"""
        print("\n🔧 FIXING BALANCE SHEET EQUATIONS")
        print("=" * 60)
        
        fixed_count = 0
        
        # Strategy 1: If we have Assets and Liabilities, calculate Equity
        self.cursor.execute("""
            SELECT bs.id, bs.total_assets, bs.total_liabilities, bs.stockholders_equity
            FROM balance_sheets bs
            WHERE bs.total_assets IS NOT NULL 
              AND bs.total_liabilities IS NOT NULL 
              AND (bs.stockholders_equity IS NULL 
                   OR ABS(bs.total_assets - (bs.total_liabilities + bs.stockholders_equity)) > 1000000)
        """)
        
        records = self.cursor.fetchall()
        
        for record_id, assets, liabilities, equity in records:
            if assets and liabilities:
                # Calculate correct equity
                correct_equity = assets - liabilities
                
                # Only update if the calculated equity is reasonable
                if correct_equity > -assets * 0.5:  # Equity shouldn't be more negative than 50% of assets
                    self.cursor.execute("""
                        UPDATE balance_sheets 
                        SET stockholders_equity = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (correct_equity, record_id))
                    fixed_count += 1
        
        # Strategy 2: If we have Assets and Equity, calculate Liabilities
        self.cursor.execute("""
            SELECT bs.id, bs.total_assets, bs.total_liabilities, bs.stockholders_equity
            FROM balance_sheets bs
            WHERE bs.total_assets IS NOT NULL 
              AND bs.stockholders_equity IS NOT NULL 
              AND bs.total_liabilities IS NULL
        """)
        
        records = self.cursor.fetchall()
        
        for record_id, assets, liabilities, equity in records:
            if assets and equity:
                # Calculate correct liabilities
                correct_liabilities = assets - equity
                
                # Only update if the calculated liabilities is reasonable
                if correct_liabilities >= 0 and correct_liabilities <= assets:
                    self.cursor.execute("""
                        UPDATE balance_sheets 
                        SET total_liabilities = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (correct_liabilities, record_id))
                    fixed_count += 1
        
        print(f"✅ Fixed {fixed_count} balance sheet equations")
        return fixed_count
    
    def fix_missing_calculated_fields(self):
        """Calculate and populate missing calculated fields"""
        print("\n🧮 CALCULATING MISSING FIELDS")
        print("=" * 60)
        
        fixed_count = 0
        
        # Fix working capital = current_assets - current_liabilities
        self.cursor.execute("""
            UPDATE balance_sheets 
            SET working_capital = current_assets - current_liabilities,
                updated_at = CURRENT_TIMESTAMP
            WHERE current_assets IS NOT NULL 
              AND current_liabilities IS NOT NULL 
              AND (working_capital IS NULL 
                   OR ABS(working_capital - (current_assets - current_liabilities)) > 100000)
        """)
        working_capital_fixed = self.cursor.rowcount
        
        # Fix total debt calculation
        self.cursor.execute("""
            UPDATE balance_sheets 
            SET total_debt = COALESCE(short_term_debt, 0) + COALESCE(long_term_debt, 0),
                updated_at = CURRENT_TIMESTAMP
            WHERE (short_term_debt IS NOT NULL OR long_term_debt IS NOT NULL)
              AND (total_debt IS NULL 
                   OR ABS(total_debt - (COALESCE(short_term_debt, 0) + COALESCE(long_term_debt, 0))) > 100000)
        """)
        total_debt_fixed = self.cursor.rowcount
        
        # Fix common stock equity
        self.cursor.execute("""
            UPDATE balance_sheets 
            SET common_stock_equity = stockholders_equity - COALESCE(minority_interest, 0),
                updated_at = CURRENT_TIMESTAMP
            WHERE stockholders_equity IS NOT NULL
              AND (common_stock_equity IS NULL 
                   OR ABS(common_stock_equity - (stockholders_equity - COALESCE(minority_interest, 0))) > 100000)
        """)
        common_equity_fixed = self.cursor.rowcount
        
        fixed_count = working_capital_fixed + total_debt_fixed + common_equity_fixed
        
        print(f"✅ Fixed {working_capital_fixed} working capital calculations")
        print(f"✅ Fixed {total_debt_fixed} total debt calculations")
        print(f"✅ Fixed {common_equity_fixed} common stock equity calculations")
        
        return fixed_count
    
    def clean_invalid_data(self):
        """Clean obviously invalid data"""
        print("\n🧹 CLEANING INVALID DATA")
        print("=" * 60)
        
        cleaned_count = 0
        
        # Remove records where assets are negative (clearly wrong)
        self.cursor.execute("""
            DELETE FROM balance_sheets 
            WHERE total_assets < 0
        """)
        negative_assets = self.cursor.rowcount
        cleaned_count += negative_assets
        
        # Fix unreasonable values (set to NULL for recalculation)
        # If equity is more than 200% of assets, something is wrong
        self.cursor.execute("""
            UPDATE balance_sheets 
            SET stockholders_equity = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE total_assets > 0 
              AND stockholders_equity > total_assets * 2
        """)
        unreasonable_equity = self.cursor.rowcount
        cleaned_count += unreasonable_equity
        
        # If liabilities are more than 200% of assets, something is wrong
        self.cursor.execute("""
            UPDATE balance_sheets 
            SET total_liabilities = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE total_assets > 0 
              AND total_liabilities > total_assets * 2
        """)
        unreasonable_liabilities = self.cursor.rowcount
        cleaned_count += unreasonable_liabilities
        
        print(f"✅ Removed {negative_assets} records with negative assets")
        print(f"✅ Reset {unreasonable_equity} unreasonable equity values")
        print(f"✅ Reset {unreasonable_liabilities} unreasonable liability values")
        
        return cleaned_count
    
    def validate_data_consistency(self):
        """Validate price history data consistency"""
        print("\n📈 VALIDATING PRICE DATA CONSISTENCY")
        print("=" * 60)
        
        fixed_count = 0
        
        # Fix high < low price errors
        self.cursor.execute("""
            UPDATE price_history 
            SET high_price = low_price,
                updated_at = CURRENT_TIMESTAMP
            WHERE high_price < low_price
              AND high_price IS NOT NULL 
              AND low_price IS NOT NULL
        """)
        price_fixes = self.cursor.rowcount
        fixed_count += price_fixes
        
        # Fix negative prices
        self.cursor.execute("""
            UPDATE price_history 
            SET open_price = NULL, high_price = NULL, low_price = NULL, close_price = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE open_price < 0 OR high_price < 0 OR low_price < 0 OR close_price < 0
        """)
        negative_price_fixes = self.cursor.rowcount
        fixed_count += negative_price_fixes
        
        print(f"✅ Fixed {price_fixes} high < low price inconsistencies")
        print(f"✅ Fixed {negative_price_fixes} negative price records")
        
        return fixed_count
    
    def add_data_quality_flags(self):
        """Add quality flags to track data reliability"""
        print("\n🏷️ ADDING DATA QUALITY FLAGS")
        print("=" * 60)
        
        # Add quality score column if it doesn't exist
        try:
            self.cursor.execute("""
                ALTER TABLE balance_sheets 
                ADD COLUMN IF NOT EXISTS data_quality_score INTEGER DEFAULT 100
            """)
            
            self.cursor.execute("""
                ALTER TABLE balance_sheets 
                ADD COLUMN IF NOT EXISTS quality_flags TEXT[]
            """)
            
            # Calculate quality scores
            self.cursor.execute("""
                UPDATE balance_sheets 
                SET data_quality_score = 
                    CASE 
                        WHEN total_assets IS NULL THEN 0
                        WHEN total_liabilities IS NULL THEN 30
                        WHEN stockholders_equity IS NULL THEN 30
                        WHEN ABS(total_assets - (total_liabilities + stockholders_equity)) > 1000000 THEN 50
                        WHEN ABS(total_assets - (total_liabilities + stockholders_equity)) > 100000 THEN 80
                        ELSE 100
                    END,
                quality_flags = 
                    CASE 
                        WHEN total_assets IS NULL THEN ARRAY['missing_assets']
                        WHEN total_liabilities IS NULL THEN ARRAY['missing_liabilities']
                        WHEN stockholders_equity IS NULL THEN ARRAY['missing_equity']
                        WHEN ABS(total_assets - (total_liabilities + stockholders_equity)) > 1000000 THEN ARRAY['major_imbalance']
                        WHEN ABS(total_assets - (total_liabilities + stockholders_equity)) > 100000 THEN ARRAY['minor_imbalance']
                        ELSE ARRAY['clean']
                    END,
                updated_at = CURRENT_TIMESTAMP
            """)
            
            print("✅ Added data quality scoring system")
            
        except Exception as e:
            print(f"⚠️ Could not add quality flags: {e}")
    
    def generate_data_quality_report(self):
        """Generate a comprehensive data quality report"""
        print("\n📊 DATA QUALITY REPORT")
        print("=" * 60)
        
        # Balance sheet consistency after fixes
        self.cursor.execute("""
            SELECT COUNT(*) 
            FROM balance_sheets
            WHERE total_assets IS NOT NULL 
              AND total_liabilities IS NOT NULL 
              AND stockholders_equity IS NOT NULL
              AND ABS(total_assets - (total_liabilities + stockholders_equity)) > 1000000
        """)
        remaining_issues = self.cursor.fetchone()[0]
        
        # Data completeness
        self.cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(total_assets) as has_assets,
                COUNT(total_liabilities) as has_liabilities,
                COUNT(stockholders_equity) as has_equity,
                COUNT(CASE WHEN total_assets IS NOT NULL AND total_liabilities IS NOT NULL AND stockholders_equity IS NOT NULL THEN 1 END) as complete_records
            FROM balance_sheets
        """)
        completeness = self.cursor.fetchone()
        
        # Price data quality
        self.cursor.execute("""
            SELECT 
                COUNT(*) as total_price_records,
                COUNT(CASE WHEN high_price < low_price THEN 1 END) as invalid_high_low,
                COUNT(CASE WHEN open_price < 0 OR high_price < 0 OR low_price < 0 OR close_price < 0 THEN 1 END) as negative_prices
            FROM price_history
        """)
        price_quality = self.cursor.fetchone()
        
        print(f"📈 Balance Sheet Quality:")
        print(f"   Total records: {completeness[0]:,}")
        print(f"   Complete records: {completeness[4]:,} ({completeness[4]/completeness[0]*100:.1f}%)")
        print(f"   Remaining imbalances: {remaining_issues:,}")
        
        print(f"\n📊 Price Data Quality:")
        print(f"   Total price records: {price_quality[0]:,}")
        print(f"   Invalid high/low: {price_quality[1]:,}")
        print(f"   Negative prices: {price_quality[2]:,}")
        
        # Overall score
        balance_score = max(0, 100 - (remaining_issues / completeness[0] * 100))
        price_score = max(0, 100 - ((price_quality[1] + price_quality[2]) / price_quality[0] * 100))
        overall_score = (balance_score + price_score) / 2
        
        print(f"\n🎯 Overall Data Quality Score: {overall_score:.1f}%")
        
        return overall_score
    
    def run_complete_fix(self):
        """Run the complete data quality fixing process"""
        print("🚀 COMPREHENSIVE DATA QUALITY IMPROVEMENT")
        print("=" * 80)
        print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Step 1: Analyze issues
            issues = self.analyze_balance_sheet_issues()
            
            # Step 2: Clean invalid data
            cleaned = self.clean_invalid_data()
            
            # Step 3: Fix balance sheet equations
            fixed_balance = self.fix_balance_sheet_equation()
            
            # Step 4: Calculate missing fields
            fixed_calculations = self.fix_missing_calculated_fields()
            
            # Step 5: Validate price consistency
            fixed_prices = self.validate_data_consistency()
            
            # Step 6: Add quality flags
            self.add_data_quality_flags()
            
            # Step 7: Generate final report
            final_score = self.generate_data_quality_report()
            
            # Commit all changes
            self.conn.commit()
            
            print(f"\n✅ DATA QUALITY IMPROVEMENT COMPLETE")
            print(f"   Records cleaned: {cleaned:,}")
            print(f"   Balance sheets fixed: {fixed_balance:,}")
            print(f"   Calculations fixed: {fixed_calculations:,}")
            print(f"   Price records fixed: {fixed_prices:,}")
            print(f"   Final quality score: {final_score:.1f}%")
            
        except Exception as e:
            print(f"❌ Error during data quality improvement: {e}")
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
    fixer = BalanceSheetFixer()
    try:
        fixer.run_complete_fix()
    finally:
        fixer.close()
