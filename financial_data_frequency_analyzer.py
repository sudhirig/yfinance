
#!/usr/bin/env python3
"""
Financial Data Frequency Analyzer
Analyzes the distribution of quarterly vs annual data across all financial statements
"""

import psycopg2
import os
import pandas as pd
from datetime import datetime

class FinancialFrequencyAnalyzer:
    def __init__(self):
        self.conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    
    def analyze_income_statements(self):
        """Analyze income statement frequency distribution"""
        query = """
        SELECT 
            period_type,
            COUNT(*) as record_count,
            COUNT(DISTINCT company_id) as unique_companies,
            COUNT(DISTINCT period_ending) as unique_periods,
            MIN(period_ending) as earliest_period,
            MAX(period_ending) as latest_period
        FROM income_statements
        GROUP BY period_type
        ORDER BY period_type;
        """
        return pd.read_sql(query, self.conn)
    
    def analyze_balance_sheets(self):
        """Analyze balance sheet frequency distribution"""
        query = """
        SELECT 
            period_type,
            COUNT(*) as record_count,
            COUNT(DISTINCT company_id) as unique_companies,
            COUNT(DISTINCT period_ending) as unique_periods,
            MIN(period_ending) as earliest_period,
            MAX(period_ending) as latest_period
        FROM balance_sheets
        GROUP BY period_type
        ORDER BY period_type;
        """
        return pd.read_sql(query, self.conn)
    
    def analyze_cash_flow_statements(self):
        """Analyze cash flow statement frequency distribution"""
        query = """
        SELECT 
            period_type,
            COUNT(*) as record_count,
            COUNT(DISTINCT company_id) as unique_companies,
            COUNT(DISTINCT period_ending) as unique_periods,
            MIN(period_ending) as earliest_period,
            MAX(period_ending) as latest_period
        FROM cash_flow_statements
        GROUP BY period_type
        ORDER BY period_type;
        """
        return pd.read_sql(query, self.conn)
    
    def analyze_earnings_data(self):
        """Analyze earnings data (note: earnings don't have period_type)"""
        query = """
        SELECT 
            COUNT(*) as total_earnings_records,
            COUNT(DISTINCT company_id) as unique_companies,
            COUNT(DISTINCT earnings_date) as unique_dates,
            MIN(earnings_date) as earliest_date,
            MAX(earnings_date) as latest_date
        FROM earnings;
        """
        return pd.read_sql(query, self.conn)
    
    def get_company_coverage_by_frequency(self):
        """Get company coverage breakdown by data frequency"""
        query = """
        SELECT 
            'Income Statements' as statement_type,
            SUM(CASE WHEN period_type = 'annual' THEN 1 ELSE 0 END) as annual_records,
            SUM(CASE WHEN period_type = 'quarterly' THEN 1 ELSE 0 END) as quarterly_records,
            COUNT(DISTINCT CASE WHEN period_type = 'annual' THEN company_id END) as companies_with_annual,
            COUNT(DISTINCT CASE WHEN period_type = 'quarterly' THEN company_id END) as companies_with_quarterly,
            COUNT(DISTINCT company_id) as total_companies_with_data
        FROM income_statements
        
        UNION ALL
        
        SELECT 
            'Balance Sheets' as statement_type,
            SUM(CASE WHEN period_type = 'annual' THEN 1 ELSE 0 END) as annual_records,
            SUM(CASE WHEN period_type = 'quarterly' THEN 1 ELSE 0 END) as quarterly_records,
            COUNT(DISTINCT CASE WHEN period_type = 'annual' THEN company_id END) as companies_with_annual,
            COUNT(DISTINCT CASE WHEN period_type = 'quarterly' THEN company_id END) as companies_with_quarterly,
            COUNT(DISTINCT company_id) as total_companies_with_data
        FROM balance_sheets
        
        UNION ALL
        
        SELECT 
            'Cash Flow Statements' as statement_type,
            SUM(CASE WHEN period_type = 'annual' THEN 1 ELSE 0 END) as annual_records,
            SUM(CASE WHEN period_type = 'quarterly' THEN 1 ELSE 0 END) as quarterly_records,
            COUNT(DISTINCT CASE WHEN period_type = 'annual' THEN company_id END) as companies_with_annual,
            COUNT(DISTINCT CASE WHEN period_type = 'quarterly' THEN company_id END) as companies_with_quarterly,
            COUNT(DISTINCT company_id) as total_companies_with_data
        FROM cash_flow_statements;
        """
        return pd.read_sql(query, self.conn)
    
    def get_average_periods_per_company(self):
        """Get average number of periods per company by frequency"""
        query = """
        SELECT 
            'Income Statements' as statement_type,
            'Annual' as frequency,
            AVG(annual_periods) as avg_periods_per_company,
            MAX(annual_periods) as max_periods,
            MIN(annual_periods) as min_periods
        FROM (
            SELECT 
                company_id,
                COUNT(*) as annual_periods
            FROM income_statements 
            WHERE period_type = 'annual'
            GROUP BY company_id
        ) sub
        
        UNION ALL
        
        SELECT 
            'Income Statements' as statement_type,
            'Quarterly' as frequency,
            AVG(quarterly_periods) as avg_periods_per_company,
            MAX(quarterly_periods) as max_periods,
            MIN(quarterly_periods) as min_periods
        FROM (
            SELECT 
                company_id,
                COUNT(*) as quarterly_periods
            FROM income_statements 
            WHERE period_type = 'quarterly'
            GROUP BY company_id
        ) sub
        
        UNION ALL
        
        SELECT 
            'Balance Sheets' as statement_type,
            'Annual' as frequency,
            AVG(annual_periods) as avg_periods_per_company,
            MAX(annual_periods) as max_periods,
            MIN(annual_periods) as min_periods
        FROM (
            SELECT 
                company_id,
                COUNT(*) as annual_periods
            FROM balance_sheets 
            WHERE period_type = 'annual'
            GROUP BY company_id
        ) sub
        
        UNION ALL
        
        SELECT 
            'Balance Sheets' as statement_type,
            'Quarterly' as frequency,
            AVG(quarterly_periods) as avg_periods_per_company,
            MAX(quarterly_periods) as max_periods,
            MIN(quarterly_periods) as min_periods
        FROM (
            SELECT 
                company_id,
                COUNT(*) as quarterly_periods
            FROM balance_sheets 
            WHERE period_type = 'quarterly'
            GROUP BY company_id
        ) sub
        
        UNION ALL
        
        SELECT 
            'Cash Flow Statements' as statement_type,
            'Annual' as frequency,
            AVG(annual_periods) as avg_periods_per_company,
            MAX(annual_periods) as max_periods,
            MIN(annual_periods) as min_periods
        FROM (
            SELECT 
                company_id,
                COUNT(*) as annual_periods
            FROM cash_flow_statements 
            WHERE period_type = 'annual'
            GROUP BY company_id
        ) sub
        
        UNION ALL
        
        SELECT 
            'Cash Flow Statements' as statement_type,
            'Quarterly' as frequency,
            AVG(quarterly_periods) as avg_periods_per_company,
            MAX(quarterly_periods) as max_periods,
            MIN(quarterly_periods) as min_periods
        FROM (
            SELECT 
                company_id,
                COUNT(*) as quarterly_periods
            FROM cash_flow_statements 
            WHERE period_type = 'quarterly'
            GROUP BY company_id
        ) sub
        
        ORDER BY statement_type, frequency;
        """
        return pd.read_sql(query, self.conn)
    
    def generate_frequency_report(self):
        """Generate comprehensive frequency distribution report"""
        print("=" * 80)
        print("📊 FINANCIAL DATA FREQUENCY DISTRIBUTION ANALYSIS")
        print("=" * 80)
        print(f"🕒 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # 1. Overall company coverage by frequency
        print("📈 OVERALL COVERAGE BY FREQUENCY")
        print("-" * 50)
        coverage_data = self.get_company_coverage_by_frequency()
        
        for _, row in coverage_data.iterrows():
            statement_type = row['statement_type']
            annual_records = row['annual_records']
            quarterly_records = row['quarterly_records']
            total_records = annual_records + quarterly_records
            
            companies_annual = row['companies_with_annual']
            companies_quarterly = row['companies_with_quarterly']
            total_companies = row['total_companies_with_data']
            
            if total_records > 0:
                annual_pct = (annual_records / total_records) * 100
                quarterly_pct = (quarterly_records / total_records) * 100
            else:
                annual_pct = quarterly_pct = 0
            
            print(f"\n{statement_type}:")
            print(f"  📊 RECORDS DISTRIBUTION:")
            print(f"    Annual   : {annual_records:>6} records ({annual_pct:>5.1f}%)")
            print(f"    Quarterly: {quarterly_records:>6} records ({quarterly_pct:>5.1f}%)")
            print(f"    Total    : {total_records:>6} records")
            
            print(f"  🏢 COMPANY COVERAGE:")
            print(f"    With Annual Data   : {companies_annual:>3} companies")
            print(f"    With Quarterly Data: {companies_quarterly:>3} companies")
            print(f"    Total Companies    : {total_companies:>3} companies")
        
        print()
        
        # 2. Detailed breakdown by statement type
        print("📋 DETAILED BREAKDOWN BY STATEMENT TYPE")
        print("=" * 60)
        
        # Income Statements
        print("\n💰 INCOME STATEMENTS")
        print("-" * 30)
        income_data = self.analyze_income_statements()
        if not income_data.empty:
            for _, row in income_data.iterrows():
                print(f"  {row['period_type'].upper()}:")
                print(f"    Records: {row['record_count']:>6}")
                print(f"    Companies: {row['unique_companies']:>4}")
                print(f"    Unique Periods: {row['unique_periods']:>4}")
                print(f"    Date Range: {row['earliest_period']} to {row['latest_period']}")
        else:
            print("  ❌ No income statement data found")
        
        # Balance Sheets
        print("\n📊 BALANCE SHEETS")
        print("-" * 30)
        balance_data = self.analyze_balance_sheets()
        if not balance_data.empty:
            for _, row in balance_data.iterrows():
                print(f"  {row['period_type'].upper()}:")
                print(f"    Records: {row['record_count']:>6}")
                print(f"    Companies: {row['unique_companies']:>4}")
                print(f"    Unique Periods: {row['unique_periods']:>4}")
                print(f"    Date Range: {row['earliest_period']} to {row['latest_period']}")
        else:
            print("  ❌ No balance sheet data found")
        
        # Cash Flow Statements
        print("\n💸 CASH FLOW STATEMENTS")
        print("-" * 30)
        cashflow_data = self.analyze_cash_flow_statements()
        if not cashflow_data.empty:
            for _, row in cashflow_data.iterrows():
                print(f"  {row['period_type'].upper()}:")
                print(f"    Records: {row['record_count']:>6}")
                print(f"    Companies: {row['unique_companies']:>4}")
                print(f"    Unique Periods: {row['unique_periods']:>4}")
                print(f"    Date Range: {row['earliest_period']} to {row['latest_period']}")
        else:
            print("  ❌ No cash flow statement data found")
        
        # Earnings Data
        print("\n📈 EARNINGS DATA")
        print("-" * 30)
        earnings_data = self.analyze_earnings_data()
        if not earnings_data.empty:
            row = earnings_data.iloc[0]
            print(f"  Total Records: {row['total_earnings_records']:>6}")
            print(f"  Companies: {row['unique_companies']:>4}")
            print(f"  Unique Dates: {row['unique_dates']:>4}")
            print(f"  Date Range: {row['earliest_date']} to {row['latest_date']}")
            print("  Note: Earnings data is event-based, not quarterly/annual")
        else:
            print("  ❌ No earnings data found")
        
        # 3. Average periods per company
        print("\n📊 AVERAGE PERIODS PER COMPANY")
        print("-" * 50)
        avg_periods = self.get_average_periods_per_company()
        if not avg_periods.empty:
            current_statement = None
            for _, row in avg_periods.iterrows():
                if row['statement_type'] != current_statement:
                    current_statement = row['statement_type']
                    print(f"\n{current_statement}:")
                
                print(f"  {row['frequency']:<10}: {row['avg_periods_per_company']:>4.1f} avg periods "
                      f"(min: {row['min_periods']}, max: {row['max_periods']})")
        
        # 4. Summary statistics
        print("\n🎯 SUMMARY STATISTICS")
        print("-" * 30)
        
        total_financial_records = 0
        total_annual_records = 0
        total_quarterly_records = 0
        
        for _, row in coverage_data.iterrows():
            total_financial_records += (row['annual_records'] + row['quarterly_records'])
            total_annual_records += row['annual_records']
            total_quarterly_records += row['quarterly_records']
        
        if total_financial_records > 0:
            overall_annual_pct = (total_annual_records / total_financial_records) * 100
            overall_quarterly_pct = (total_quarterly_records / total_financial_records) * 100
            
            print(f"Total Financial Statement Records: {total_financial_records:,}")
            print(f"Annual Records: {total_annual_records:,} ({overall_annual_pct:.1f}%)")
            print(f"Quarterly Records: {total_quarterly_records:,} ({overall_quarterly_pct:.1f}%)")
            
            if overall_annual_pct > 60:
                print("✅ Good annual data coverage")
            elif overall_quarterly_pct > 60:
                print("✅ Good quarterly data coverage")
            else:
                print("⚠️ Mixed frequency distribution")
        
        print("=" * 80)
    
    def close(self):
        """Close database connection"""
        self.conn.close()

if __name__ == "__main__":
    analyzer = FinancialFrequencyAnalyzer()
    try:
        analyzer.generate_frequency_report()
    finally:
        analyzer.close()
