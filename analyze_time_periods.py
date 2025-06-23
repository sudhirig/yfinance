
#!/usr/bin/env python3
"""
Analyze Time Periods Covered by Financial Data
Shows date ranges for different types of data in the database
"""

import psycopg2
import pandas as pd
from datetime import datetime
from database_config import get_db_connection

def analyze_time_periods():
    """Analyze the time periods covered by different data types"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("=" * 60)
    print("FINANCIAL DATA TIME PERIOD ANALYSIS")
    print("=" * 60)
    
    # Price History Analysis
    print("\n1. PRICE HISTORY DATA:")
    print("-" * 30)
    cursor.execute("""
        SELECT 
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            COUNT(DISTINCT date) as total_trading_days,
            COUNT(DISTINCT company_id) as companies_with_data
        FROM price_history
    """)
    price_data = cursor.fetchone()
    if price_data and price_data[0]:
        print(f"Date Range: {price_data[0]} to {price_data[1]}")
        print(f"Total Trading Days: {price_data[2]:,}")
        print(f"Companies with Price Data: {price_data[3]}")
        
        # Calculate years of data
        years = (price_data[1] - price_data[0]).days / 365.25
        print(f"Years of Historical Data: {years:.1f} years")
    
    # Sample companies with their date ranges
    print("\n   Sample Company Date Ranges:")
    cursor.execute("""
        SELECT 
            c.symbol,
            c.long_name,
            MIN(ph.date) as start_date,
            MAX(ph.date) as end_date,
            COUNT(ph.date) as trading_days
        FROM companies c
        JOIN price_history ph ON c.id = ph.company_id
        WHERE c.symbol IN ('RELIANCE.NS', 'TCS.NS', 'INFY.NS', 'HDFCBANK.NS', 'ICICIBANK.NS')
        GROUP BY c.id, c.symbol, c.long_name
        ORDER BY c.symbol
    """)
    
    for row in cursor.fetchall():
        years = (row[3] - row[2]).days / 365.25
        print(f"   {row[0]}: {row[2]} to {row[3]} ({years:.1f} years, {row[4]:,} days)")
    
    # Financial Statements Analysis
    print("\n\n2. FINANCIAL STATEMENTS DATA:")
    print("-" * 30)
    
    # Income Statements
    cursor.execute("""
        SELECT 
            MIN(period_ending) as earliest_period,
            MAX(period_ending) as latest_period,
            COUNT(DISTINCT period_ending) as unique_periods,
            COUNT(DISTINCT company_id) as companies_with_data,
            COUNT(*) as total_records
        FROM income_statements
        WHERE period_type = 'annual'
    """)
    income_annual = cursor.fetchone()
    
    cursor.execute("""
        SELECT 
            MIN(period_ending) as earliest_period,
            MAX(period_ending) as latest_period,
            COUNT(DISTINCT period_ending) as unique_periods,
            COUNT(DISTINCT company_id) as companies_with_data,
            COUNT(*) as total_records
        FROM income_statements
        WHERE period_type = 'quarterly'
    """)
    income_quarterly = cursor.fetchone()
    
    print("   Income Statements (Annual):")
    if income_annual and income_annual[0]:
        print(f"     Period Range: {income_annual[0]} to {income_annual[1]}")
        print(f"     Unique Periods: {income_annual[2]}")
        print(f"     Companies: {income_annual[3]}")
        print(f"     Total Records: {income_annual[4]:,}")
    
    print("   Income Statements (Quarterly):")
    if income_quarterly and income_quarterly[0]:
        print(f"     Period Range: {income_quarterly[0]} to {income_quarterly[1]}")
        print(f"     Unique Periods: {income_quarterly[2]}")
        print(f"     Companies: {income_quarterly[3]}")
        print(f"     Total Records: {income_quarterly[4]:,}")
    
    # Balance Sheets
    cursor.execute("""
        SELECT 
            MIN(period_ending) as earliest_period,
            MAX(period_ending) as latest_period,
            COUNT(DISTINCT period_ending) as unique_periods,
            COUNT(DISTINCT company_id) as companies_with_data
        FROM balance_sheets
        WHERE period_type = 'annual'
    """)
    balance_data = cursor.fetchone()
    
    print("   Balance Sheets (Annual):")
    if balance_data and balance_data[0]:
        print(f"     Period Range: {balance_data[0]} to {balance_data[1]}")
        print(f"     Unique Periods: {balance_data[2]}")
        print(f"     Companies: {balance_data[3]}")
    
    # Cash Flow Statements
    cursor.execute("""
        SELECT 
            MIN(period_ending) as earliest_period,
            MAX(period_ending) as latest_period,
            COUNT(DISTINCT period_ending) as unique_periods,
            COUNT(DISTINCT company_id) as companies_with_data
        FROM cash_flow_statements
        WHERE period_type = 'annual'
    """)
    cashflow_data = cursor.fetchone()
    
    print("   Cash Flow Statements (Annual):")
    if cashflow_data and cashflow_data[0]:
        print(f"     Period Range: {cashflow_data[0]} to {cashflow_data[1]}")
        print(f"     Unique Periods: {cashflow_data[2]}")
        print(f"     Companies: {cashflow_data[3]}")
    
    # Data Currency Analysis
    print("\n\n3. DATA CURRENCY:")
    print("-" * 30)
    
    cursor.execute("""
        SELECT 
            MAX(date) as latest_price_date,
            COUNT(DISTINCT company_id) as companies_updated
        FROM price_history
        WHERE date >= CURRENT_DATE - INTERVAL '30 days'
    """)
    recent_data = cursor.fetchone()
    
    if recent_data and recent_data[0]:
        days_old = (datetime.now().date() - recent_data[0]).days
        print(f"Latest Price Data: {recent_data[0]} ({days_old} days ago)")
        print(f"Companies with Recent Data: {recent_data[1]}")
    
    # Sector Coverage Analysis
    print("\n\n4. SECTOR COVERAGE:")
    print("-" * 30)
    
    cursor.execute("""
        SELECT 
            c.sector,
            COUNT(*) as company_count,
            COUNT(CASE WHEN ph.company_id IS NOT NULL THEN 1 END) as with_price_data,
            COUNT(CASE WHEN i.company_id IS NOT NULL THEN 1 END) as with_financials
        FROM companies c
        LEFT JOIN (SELECT DISTINCT company_id FROM price_history) ph ON c.id = ph.company_id
        LEFT JOIN (SELECT DISTINCT company_id FROM income_statements) i ON c.id = i.company_id
        WHERE c.sector IS NOT NULL
        GROUP BY c.sector
        ORDER BY company_count DESC
    """)
    
    print("   Sector | Companies | Price Data | Financials")
    print("   " + "-" * 45)
    for row in cursor.fetchall():
        print(f"   {row[0][:15]:<15} | {row[1]:>3} | {row[2]:>6} | {row[3]:>6}")
    
    # Summary Statistics
    print("\n\n5. SUMMARY STATISTICS:")
    print("-" * 30)
    
    cursor.execute("SELECT COUNT(*) FROM companies")
    total_companies = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT company_id) FROM price_history")
    companies_with_prices = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT company_id) FROM income_statements")
    companies_with_financials = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM price_history")
    total_price_records = cursor.fetchone()[0]
    
    print(f"Total Companies in Database: {total_companies:,}")
    print(f"Companies with Price Data: {companies_with_prices:,} ({companies_with_prices/total_companies*100:.1f}%)")
    print(f"Companies with Financial Data: {companies_with_financials:,} ({companies_with_financials/total_companies*100:.1f}%)")
    print(f"Total Price Records: {total_price_records:,}")
    
    conn.close()
    print("\n" + "=" * 60)

if __name__ == "__main__":
    analyze_time_periods()
