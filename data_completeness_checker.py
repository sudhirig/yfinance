
#!/usr/bin/env python3
"""
Data Completeness Checker for NSE Stock Database
Analyzes completeness of all data points across 145 companies
"""

import psycopg2
import os
import pandas as pd
from datetime import datetime, timedelta
import json

class DataCompletenessChecker:
    def __init__(self):
        self.conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    
    def get_overall_statistics(self):
        """Get overall database statistics"""
        query = """
        SELECT 
            'Companies' as table_name,
            COUNT(*) as total_records,
            COUNT(DISTINCT symbol) as unique_symbols
        FROM companies
        
        UNION ALL
        
        SELECT 
            'Price History' as table_name,
            COUNT(*) as total_records,
            COUNT(DISTINCT company_id) as unique_companies
        FROM price_history
        
        UNION ALL
        
        SELECT 
            'Company Metrics' as table_name,
            COUNT(*) as total_records,
            COUNT(DISTINCT company_id) as unique_companies
        FROM company_metrics
        
        UNION ALL
        
        SELECT 
            'Income Statements' as table_name,
            COUNT(*) as total_records,
            COUNT(DISTINCT company_id) as unique_companies
        FROM income_statements
        
        UNION ALL
        
        SELECT 
            'Balance Sheets' as table_name,
            COUNT(*) as total_records,
            COUNT(DISTINCT company_id) as unique_companies
        FROM balance_sheets
        
        UNION ALL
        
        SELECT 
            'Cash Flow Statements' as table_name,
            COUNT(*) as total_records,
            COUNT(DISTINCT company_id) as unique_companies
        FROM cash_flow_statements
        
        UNION ALL
        
        SELECT 
            'Corporate Actions' as table_name,
            COUNT(*) as total_records,
            COUNT(DISTINCT company_id) as unique_companies
        FROM corporate_actions
        
        UNION ALL
        
        SELECT 
            'Earnings' as table_name,
            COUNT(*) as total_records,
            COUNT(DISTINCT company_id) as unique_companies
        FROM earnings
        
        UNION ALL
        
        SELECT 
            'Holders' as table_name,
            COUNT(*) as total_records,
            COUNT(DISTINCT company_id) as unique_companies
        FROM holders;
        """
        return pd.read_sql(query, self.conn)
    
    def check_company_basic_info_completeness(self):
        """Check completeness of basic company information"""
        query = """
        SELECT 
            symbol,
            CASE WHEN long_name IS NOT NULL THEN 1 ELSE 0 END as has_long_name,
            CASE WHEN sector IS NOT NULL THEN 1 ELSE 0 END as has_sector,
            CASE WHEN industry IS NOT NULL THEN 1 ELSE 0 END as has_industry,
            CASE WHEN country IS NOT NULL THEN 1 ELSE 0 END as has_country,
            CASE WHEN exchange IS NOT NULL THEN 1 ELSE 0 END as has_exchange,
            CASE WHEN website IS NOT NULL THEN 1 ELSE 0 END as has_website,
            CASE WHEN full_time_employees IS NOT NULL THEN 1 ELSE 0 END as has_employees,
            CASE WHEN long_business_summary IS NOT NULL THEN 1 ELSE 0 END as has_summary
        FROM companies
        ORDER BY symbol;
        """
        df = pd.read_sql(query, self.conn)
        
        # Calculate completion percentages
        completion_stats = {
            'Total Companies': len(df),
            'Long Name': f"{df['has_long_name'].sum()}/{len(df)} ({df['has_long_name'].mean()*100:.1f}%)",
            'Sector': f"{df['has_sector'].sum()}/{len(df)} ({df['has_sector'].mean()*100:.1f}%)",
            'Industry': f"{df['has_industry'].sum()}/{len(df)} ({df['has_industry'].mean()*100:.1f}%)",
            'Country': f"{df['has_country'].sum()}/{len(df)} ({df['has_country'].mean()*100:.1f}%)",
            'Exchange': f"{df['has_exchange'].sum()}/{len(df)} ({df['has_exchange'].mean()*100:.1f}%)",
            'Website': f"{df['has_website'].sum()}/{len(df)} ({df['has_website'].mean()*100:.1f}%)",
            'Employees': f"{df['has_employees'].sum()}/{len(df)} ({df['has_employees'].mean()*100:.1f}%)",
            'Business Summary': f"{df['has_summary'].sum()}/{len(df)} ({df['has_summary'].mean()*100:.1f}%)"
        }
        
        return completion_stats, df
    
    def check_price_history_completeness(self):
        """Check price history data completeness"""
        query = """
        SELECT 
            c.symbol,
            COUNT(ph.id) as price_records,
            MIN(ph.date) as earliest_date,
            MAX(ph.date) as latest_date,
            COUNT(CASE WHEN ph.volume > 0 THEN 1 END) as days_with_volume,
            COUNT(CASE WHEN ph.dividends > 0 THEN 1 END) as dividend_days,
            COUNT(CASE WHEN ph.stock_splits > 0 THEN 1 END) as split_days
        FROM companies c
        LEFT JOIN price_history ph ON c.id = ph.company_id
        GROUP BY c.id, c.symbol
        ORDER BY price_records DESC;
        """
        return pd.read_sql(query, self.conn)
    
    def check_financial_statements_completeness(self):
        """Check financial statements data completeness"""
        query = """
        SELECT 
            c.symbol,
            COUNT(DISTINCT i.period_ending) FILTER (WHERE i.period_type = 'annual') as annual_income_statements,
            COUNT(DISTINCT i.period_ending) FILTER (WHERE i.period_type = 'quarterly') as quarterly_income_statements,
            COUNT(DISTINCT b.period_ending) FILTER (WHERE b.period_type = 'annual') as annual_balance_sheets,
            COUNT(DISTINCT b.period_ending) FILTER (WHERE b.period_type = 'quarterly') as quarterly_balance_sheets,
            COUNT(DISTINCT cf.period_ending) FILTER (WHERE cf.period_type = 'annual') as annual_cashflow,
            COUNT(DISTINCT cf.period_ending) FILTER (WHERE cf.period_type = 'quarterly') as quarterly_cashflow,
            MAX(i.period_ending) FILTER (WHERE i.period_type = 'annual') as latest_annual_report,
            MAX(i.period_ending) FILTER (WHERE i.period_type = 'quarterly') as latest_quarterly_report
        FROM companies c
        LEFT JOIN income_statements i ON c.id = i.company_id
        LEFT JOIN balance_sheets b ON c.id = b.company_id
        LEFT JOIN cash_flow_statements cf ON c.id = cf.company_id
        GROUP BY c.id, c.symbol
        ORDER BY c.symbol;
        """
        return pd.read_sql(query, self.conn)
    
    def check_company_metrics_completeness(self):
        """Check company metrics data completeness"""
        query = """
        SELECT 
            c.symbol,
            CASE WHEN cm.market_cap IS NOT NULL THEN 1 ELSE 0 END as has_market_cap,
            CASE WHEN cm.trailing_pe IS NOT NULL THEN 1 ELSE 0 END as has_pe_ratio,
            CASE WHEN cm.dividend_yield IS NOT NULL THEN 1 ELSE 0 END as has_dividend_yield,
            CASE WHEN cm.beta IS NOT NULL THEN 1 ELSE 0 END as has_beta,
            CASE WHEN cm.book_value IS NOT NULL THEN 1 ELSE 0 END as has_book_value,
            CASE WHEN cm.fifty_two_week_high IS NOT NULL THEN 1 ELSE 0 END as has_52w_high,
            CASE WHEN cm.fifty_two_week_low IS NOT NULL THEN 1 ELSE 0 END as has_52w_low,
            CASE WHEN cm.total_revenue IS NOT NULL THEN 1 ELSE 0 END as has_revenue,
            CASE WHEN cm.operating_cashflow IS NOT NULL THEN 1 ELSE 0 END as has_operating_cf,
            CASE WHEN cm.free_cashflow IS NOT NULL THEN 1 ELSE 0 END as has_free_cf
        FROM companies c
        LEFT JOIN company_metrics cm ON c.id = cm.company_id
        ORDER BY c.symbol;
        """
        df = pd.read_sql(query, self.conn)
        
        metrics_stats = {
            'Market Cap': f"{df['has_market_cap'].sum()}/{len(df)} ({df['has_market_cap'].mean()*100:.1f}%)",
            'P/E Ratio': f"{df['has_pe_ratio'].sum()}/{len(df)} ({df['has_pe_ratio'].mean()*100:.1f}%)",
            'Dividend Yield': f"{df['has_dividend_yield'].sum()}/{len(df)} ({df['has_dividend_yield'].mean()*100:.1f}%)",
            'Beta': f"{df['has_beta'].sum()}/{len(df)} ({df['has_beta'].mean()*100:.1f}%)",
            'Book Value': f"{df['has_book_value'].sum()}/{len(df)} ({df['has_book_value'].mean()*100:.1f}%)",
            '52W High': f"{df['has_52w_high'].sum()}/{len(df)} ({df['has_52w_high'].mean()*100:.1f}%)",
            '52W Low': f"{df['has_52w_low'].sum()}/{len(df)} ({df['has_52w_low'].mean()*100:.1f}%)",
            'Revenue': f"{df['has_revenue'].sum()}/{len(df)} ({df['has_revenue'].mean()*100:.1f}%)",
            'Operating CF': f"{df['has_operating_cf'].sum()}/{len(df)} ({df['has_operating_cf'].mean()*100:.1f}%)",
            'Free CF': f"{df['has_free_cf'].sum()}/{len(df)} ({df['has_free_cf'].mean()*100:.1f}%)"
        }
        
        return metrics_stats, df
    
    def check_corporate_actions_completeness(self):
        """Check corporate actions data completeness"""
        query = """
        SELECT 
            c.symbol,
            COUNT(ca.id) FILTER (WHERE ca.action_type = 'dividend') as dividend_records,
            COUNT(ca.id) FILTER (WHERE ca.action_type = 'stock_split') as split_records,
            MAX(ca.action_date) FILTER (WHERE ca.action_type = 'dividend') as last_dividend,
            MAX(ca.action_date) FILTER (WHERE ca.action_type = 'stock_split') as last_split
        FROM companies c
        LEFT JOIN corporate_actions ca ON c.id = ca.company_id
        GROUP BY c.id, c.symbol
        ORDER BY c.symbol;
        """
        return pd.read_sql(query, self.conn)
    
    def check_earnings_data_completeness(self):
        """Check earnings data completeness"""
        query = """
        SELECT 
            c.symbol,
            COUNT(e.id) as earnings_records,
            COUNT(CASE WHEN e.reported_eps IS NOT NULL THEN 1 END) as reported_eps_count,
            COUNT(CASE WHEN e.eps_estimate IS NOT NULL THEN 1 END) as eps_estimate_count,
            COUNT(CASE WHEN e.surprise_percent IS NOT NULL THEN 1 END) as surprise_count,
            MAX(e.earnings_date) as latest_earnings_date
        FROM companies c
        LEFT JOIN earnings e ON c.id = e.company_id
        GROUP BY c.id, c.symbol
        ORDER BY c.symbol;
        """
        return pd.read_sql(query, self.conn)
    
    def find_data_gaps(self):
        """Identify major data gaps"""
        gaps = {}
        
        # Companies without price history
        query1 = """
        SELECT c.symbol FROM companies c
        LEFT JOIN price_history ph ON c.id = ph.company_id
        WHERE ph.id IS NULL;
        """
        gaps['no_price_history'] = pd.read_sql(query1, self.conn)['symbol'].tolist()
        
        # Companies without metrics
        query2 = """
        SELECT c.symbol FROM companies c
        LEFT JOIN company_metrics cm ON c.id = cm.company_id
        WHERE cm.id IS NULL;
        """
        gaps['no_metrics'] = pd.read_sql(query2, self.conn)['symbol'].tolist()
        
        # Companies without financial statements
        query3 = """
        SELECT c.symbol FROM companies c
        LEFT JOIN income_statements i ON c.id = i.company_id
        WHERE i.id IS NULL;
        """
        gaps['no_financials'] = pd.read_sql(query3, self.conn)['symbol'].tolist()
        
        # Companies with old price data (older than 30 days)
        query4 = """
        SELECT c.symbol, MAX(ph.date) as latest_price_date
        FROM companies c
        JOIN price_history ph ON c.id = ph.company_id
        GROUP BY c.id, c.symbol
        HAVING MAX(ph.date) < CURRENT_DATE - INTERVAL '30 days'
        ORDER BY latest_price_date;
        """
        stale_data = pd.read_sql(query4, self.conn)
        gaps['stale_price_data'] = stale_data.to_dict('records')
        
        return gaps
    
    def generate_comprehensive_report(self):
        """Generate comprehensive data completeness report"""
        print("=" * 80)
        print("📊 COMPREHENSIVE DATA COMPLETENESS ANALYSIS")
        print("=" * 80)
        print(f"🕒 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # 1. Overall statistics
        print("📈 OVERALL DATABASE STATISTICS")
        print("-" * 50)
        overall_stats = self.get_overall_statistics()
        for _, row in overall_stats.iterrows():
            print(f"{row['table_name']:<20} {row['total_records']:>8} records  {row['unique_symbols']:>3} companies")
        print()
        
        # 2. Basic company info completeness
        print("🏢 BASIC COMPANY INFORMATION COMPLETENESS")
        print("-" * 50)
        basic_stats, _ = self.check_company_basic_info_completeness()
        for field, stats in basic_stats.items():
            print(f"{field:<20} {stats}")
        print()
        
        # 3. Price history completeness
        print("💹 PRICE HISTORY DATA ANALYSIS")
        print("-" * 50)
        price_stats = self.check_price_history_completeness()
        total_companies = len(price_stats)
        companies_with_data = len(price_stats[price_stats['price_records'] > 0])
        avg_records = price_stats['price_records'].mean()
        
        print(f"Companies with price data: {companies_with_data}/{total_companies} ({companies_with_data/total_companies*100:.1f}%)")
        print(f"Average price records per company: {avg_records:.0f}")
        
        # Handle date range safely
        earliest_dates = price_stats['earliest_date'].dropna()
        latest_dates = price_stats['latest_date'].dropna()
        if not earliest_dates.empty and not latest_dates.empty:
            print(f"Date range: {earliest_dates.min()} to {latest_dates.max()}")
        else:
            print("Date range: No valid dates found")
            
        print(f"Companies with dividend data: {len(price_stats[price_stats['dividend_days'] > 0])}")
        print(f"Companies with stock split data: {len(price_stats[price_stats['split_days'] > 0])}")
        print()
        
        # 4. Financial statements completeness
        print("📊 FINANCIAL STATEMENTS COMPLETENESS")
        print("-" * 50)
        financial_stats = self.check_financial_statements_completeness()
        companies_with_annual = len(financial_stats[financial_stats['annual_income_statements'] > 0])
        companies_with_quarterly = len(financial_stats[financial_stats['quarterly_income_statements'] > 0])
        avg_annual_years = financial_stats['annual_income_statements'].mean()
        avg_quarterly_periods = financial_stats['quarterly_income_statements'].mean()
        
        print(f"Companies with annual financials: {companies_with_annual}/{total_companies} ({companies_with_annual/total_companies*100:.1f}%)")
        print(f"Companies with quarterly financials: {companies_with_quarterly}/{total_companies} ({companies_with_quarterly/total_companies*100:.1f}%)")
        print(f"Average annual periods per company: {avg_annual_years:.1f}")
        print(f"Average quarterly periods per company: {avg_quarterly_periods:.1f}")
        
        if not financial_stats['latest_annual_report'].isna().all():
            latest_annual = financial_stats['latest_annual_report'].max()
            print(f"Latest annual report date: {latest_annual}")
        if not financial_stats['latest_quarterly_report'].isna().all():
            latest_quarterly = financial_stats['latest_quarterly_report'].max()
            print(f"Latest quarterly report date: {latest_quarterly}")
        print()
        
        # 5. Company metrics completeness
        print("🔢 COMPANY METRICS COMPLETENESS")
        print("-" * 50)
        metrics_stats, _ = self.check_company_metrics_completeness()
        for metric, stats in metrics_stats.items():
            print(f"{metric:<15} {stats}")
        print()
        
        # 6. Corporate actions completeness
        print("📋 CORPORATE ACTIONS ANALYSIS")
        print("-" * 50)
        actions_stats = self.check_corporate_actions_completeness()
        companies_with_dividends = len(actions_stats[actions_stats['dividend_records'] > 0])
        companies_with_splits = len(actions_stats[actions_stats['split_records'] > 0])
        total_dividends = actions_stats['dividend_records'].sum()
        total_splits = actions_stats['split_records'].sum()
        
        print(f"Companies with dividend history: {companies_with_dividends}/{total_companies} ({companies_with_dividends/total_companies*100:.1f}%)")
        print(f"Companies with stock split history: {companies_with_splits}/{total_companies} ({companies_with_splits/total_companies*100:.1f}%)")
        print(f"Total dividend records: {total_dividends}")
        print(f"Total stock split records: {total_splits}")
        print()
        
        # 7. Earnings data completeness
        print("📈 EARNINGS DATA ANALYSIS")
        print("-" * 50)
        earnings_stats = self.check_earnings_data_completeness()
        companies_with_earnings = len(earnings_stats[earnings_stats['earnings_records'] > 0])
        total_earnings_records = earnings_stats['earnings_records'].sum()
        
        print(f"Companies with earnings data: {companies_with_earnings}/{total_companies} ({companies_with_earnings/total_companies*100:.1f}%)")
        print(f"Total earnings records: {total_earnings_records}")
        print()
        
        # 8. Data gaps analysis
        print("🚨 DATA GAPS IDENTIFIED")
        print("-" * 50)
        gaps = self.find_data_gaps()
        
        if gaps['no_price_history']:
            print(f"❌ Companies without price history: {len(gaps['no_price_history'])}")
            if len(gaps['no_price_history']) <= 10:
                print(f"   {', '.join(gaps['no_price_history'])}")
        
        if gaps['no_metrics']:
            print(f"❌ Companies without metrics: {len(gaps['no_metrics'])}")
            if len(gaps['no_metrics']) <= 10:
                print(f"   {', '.join(gaps['no_metrics'])}")
        
        if gaps['no_financials']:
            print(f"❌ Companies without financial statements: {len(gaps['no_financials'])}")
            if len(gaps['no_financials']) <= 10:
                print(f"   {', '.join(gaps['no_financials'])}")
        
        if gaps['stale_price_data']:
            print(f"⚠️ Companies with stale price data (>30 days old): {len(gaps['stale_price_data'])}")
            for item in gaps['stale_price_data'][:5]:
                print(f"   {item['symbol']}: {item['latest_price_date']}")
            if len(gaps['stale_price_data']) > 5:
                print(f"   ... and {len(gaps['stale_price_data']) - 5} more")
        
        print()
        
        # 9. Data quality score
        print("🏆 OVERALL DATA QUALITY SCORE")
        print("-" * 50)
        
        # Calculate weighted quality score
        basic_info_score = (companies_with_data / total_companies) * 0.2
        price_data_score = (companies_with_data / total_companies) * 0.3
        financial_score = (companies_with_annual / total_companies) * 0.3
        metrics_score = ((total_companies - len(gaps['no_metrics'])) / total_companies) * 0.2
        
        overall_score = (basic_info_score + price_data_score + financial_score + metrics_score) * 100
        
        print(f"Basic Information Score: {basic_info_score*100:.1f}%")
        print(f"Price Data Score: {price_data_score*100:.1f}%")
        print(f"Financial Statements Score: {financial_score*100:.1f}%")
        print(f"Metrics Score: {metrics_score*100:.1f}%")
        print(f"OVERALL DATA QUALITY: {overall_score:.1f}%")
        
        if overall_score >= 80:
            print("✅ EXCELLENT data completeness!")
        elif overall_score >= 60:
            print("✅ GOOD data completeness")
        elif overall_score >= 40:
            print("⚠️ FAIR data completeness - some gaps identified")
        else:
            print("❌ POOR data completeness - significant gaps need attention")
        
        print("=" * 80)
    
    def close(self):
        """Close database connection"""
        self.conn.close()

# Run the completeness check
if __name__ == "__main__":
    checker = DataCompletenessChecker()
    checker.generate_comprehensive_report()
    checker.close()
