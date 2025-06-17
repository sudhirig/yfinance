
#!/usr/bin/env python3
"""
Financial Analysis Dashboard for NSE Stocks
Comprehensive analysis using the loaded database
"""

import psycopg2
import os
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns

class FinancialDashboard:
    def __init__(self):
        self.conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    
    def get_top_companies_by_market_cap(self, limit=20):
        """Get top companies by market capitalization"""
        query = """
        SELECT c.symbol, c.long_name, c.sector, cm.market_cap/10000000 as market_cap_cr,
               cm.trailing_pe, cm.dividend_yield, cm.beta
        FROM companies c
        JOIN company_metrics cm ON c.id = cm.company_id
        WHERE cm.market_cap IS NOT NULL
        ORDER BY cm.market_cap DESC LIMIT %s;
        """
        return pd.read_sql(query, self.conn, params=[limit])
    
    def get_sector_analysis(self):
        """Analyze performance by sector"""
        query = """
        SELECT c.sector,
               COUNT(*) as company_count,
               AVG(cm.trailing_pe) as avg_pe,
               AVG(cm.dividend_yield) as avg_dividend_yield,
               SUM(cm.market_cap)/10000000 as total_market_cap_cr
        FROM companies c
        JOIN company_metrics cm ON c.id = cm.company_id
        WHERE c.sector IS NOT NULL AND cm.market_cap IS NOT NULL
        GROUP BY c.sector
        ORDER BY total_market_cap_cr DESC;
        """
        return pd.read_sql(query, self.conn)
    
    def get_financial_health_analysis(self):
        """Get companies with strong financial health indicators"""
        query = """
        SELECT c.symbol, c.long_name, c.sector,
               ROUND(i.net_income::numeric / i.total_revenue * 100, 2) as profit_margin,
               ROUND(b.stockholders_equity::numeric / b.total_assets * 100, 2) as equity_ratio,
               ROUND(cf.free_cash_flow::numeric / 1000000, 2) as free_cashflow_millions,
               i.period_ending
        FROM companies c
        JOIN income_statements i ON c.id = i.company_id
        JOIN balance_sheets b ON c.id = b.company_id AND b.period_ending = i.period_ending
        JOIN cash_flow_statements cf ON c.id = cf.company_id AND cf.period_ending = i.period_ending
        WHERE i.period_type = 'annual' AND i.total_revenue > 0 AND i.net_income > 0
        AND i.period_ending = (SELECT MAX(period_ending) FROM income_statements WHERE company_id = c.id AND period_type = 'annual')
        ORDER BY profit_margin DESC;
        """
        return pd.read_sql(query, self.conn)
    
    def get_dividend_champions(self):
        """Find companies with consistent dividend payments"""
        query = """
        SELECT c.symbol, c.long_name, cm.dividend_yield, cm.dividend_rate,
               COUNT(ca.id) as dividend_payments,
               AVG(ca.amount) as avg_dividend,
               MAX(ca.action_date) as last_dividend_date
        FROM companies c
        JOIN company_metrics cm ON c.id = cm.company_id
        LEFT JOIN corporate_actions ca ON c.id = ca.company_id AND ca.action_type = 'dividend'
        WHERE cm.dividend_yield > 0
        GROUP BY c.id, c.symbol, c.long_name, cm.dividend_yield, cm.dividend_rate
        ORDER BY cm.dividend_yield DESC LIMIT 15;
        """
        return pd.read_sql(query, self.conn)
    
    def get_price_performance(self, days=30):
        """Get price performance over specified days"""
        query = """
        WITH price_performance AS (
            SELECT c.symbol, c.long_name, c.sector,
                   ph1.close_price as current_price,
                   ph1.date as current_date,
                   ph2.close_price as past_price,
                   ROUND(((ph1.close_price - ph2.close_price) / ph2.close_price * 100), 2) as return_pct
            FROM companies c
            JOIN price_history ph1 ON c.id = ph1.company_id
            JOIN price_history ph2 ON c.id = ph2.company_id
            WHERE ph1.date = (SELECT MAX(date) FROM price_history WHERE company_id = c.id)
            AND ph2.date >= (SELECT MAX(date) FROM price_history WHERE company_id = c.id) - INTERVAL '%s days'
            AND ph2.date = (SELECT MAX(date) FROM price_history 
                           WHERE company_id = c.id 
                           AND date <= (SELECT MAX(date) FROM price_history WHERE company_id = c.id) - INTERVAL '%s days')
        )
        SELECT * FROM price_performance 
        WHERE return_pct IS NOT NULL
        ORDER BY return_pct DESC;
        """ % (days, days)
        return pd.read_sql(query, self.conn)
    
    def get_undervalued_stocks(self):
        """Find potentially undervalued stocks"""
        query = """
        SELECT c.symbol, c.long_name, c.sector,
               cm.trailing_pe, cm.price_to_book, cm.dividend_yield,
               cm.market_cap/10000000 as market_cap_cr
        FROM companies c
        JOIN company_metrics cm ON c.id = cm.company_id
        WHERE cm.trailing_pe IS NOT NULL AND cm.trailing_pe > 0 AND cm.trailing_pe < 15
        AND cm.price_to_book IS NOT NULL AND cm.price_to_book > 0 AND cm.price_to_book < 3
        ORDER BY cm.trailing_pe ASC;
        """
        return pd.read_sql(query, self.conn)
    
    def generate_report(self):
        """Generate comprehensive financial analysis report"""
        print("=" * 80)
        print("🏦 COMPREHENSIVE NSE FINANCIAL ANALYSIS DASHBOARD")
        print("=" * 80)
        print(f"📅 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # 1. Top companies by market cap
        print("📈 TOP 15 COMPANIES BY MARKET CAPITALIZATION")
        print("-" * 80)
        top_companies = self.get_top_companies_by_market_cap(15)
        for _, row in top_companies.iterrows():
            print(f"{row['symbol']:<12} {row['long_name']:<35} ₹{row['market_cap_cr']:>8.0f} Cr  PE:{row['trailing_pe']:>6.1f}")
        print()
        
        # 2. Sector analysis
        print("🏭 SECTOR-WISE ANALYSIS")
        print("-" * 80)
        sectors = self.get_sector_analysis()
        for _, row in sectors.iterrows():
            print(f"{row['sector']:<25} {row['company_count']:>3} companies  ₹{row['total_market_cap_cr']:>8.0f} Cr  Avg PE:{row['avg_pe']:>6.1f}")
        print()
        
        # 3. Price performance
        print("📊 TOP PRICE PERFORMERS (Last 30 Days)")
        print("-" * 80)
        performers = self.get_price_performance(30).head(10)
        for _, row in performers.iterrows():
            print(f"{row['symbol']:<12} {row['long_name']:<35} ₹{row['current_price']:>8.2f}  {row['return_pct']:>+6.1f}%")
        print()
        
        # 4. Dividend champions
        print("💰 TOP DIVIDEND YIELDING STOCKS")
        print("-" * 80)
        dividends = self.get_dividend_champions().head(10)
        for _, row in dividends.iterrows():
            print(f"{row['symbol']:<12} {row['long_name']:<35} {row['dividend_yield']:>6.2f}%  Payments:{row['dividend_payments']:>3}")
        print()
        
        # 5. Undervalued stocks
        print("💎 POTENTIALLY UNDERVALUED STOCKS (PE < 15, PB < 3)")
        print("-" * 80)
        undervalued = self.get_undervalued_stocks().head(10)
        for _, row in undervalued.iterrows():
            print(f"{row['symbol']:<12} {row['long_name']:<30} PE:{row['trailing_pe']:>6.1f}  PB:{row['price_to_book']:>5.1f}")
        print()
        
        # 6. Financial health
        print("💪 COMPANIES WITH STRONG FINANCIALS (Top Profit Margins)")
        print("-" * 80)
        financial_health = self.get_financial_health_analysis().head(10)
        for _, row in financial_health.iterrows():
            print(f"{row['symbol']:<12} {row['long_name']:<30} Margin:{row['profit_margin']:>6.1f}%  FCF:₹{row['free_cashflow_millions']:>6.0f}M")
        
        print("=" * 80)
        print("📊 Analysis complete! Database contains comprehensive data for investment decisions.")
        print("💡 Use the individual methods to get detailed DataFrames for further analysis.")
        print("=" * 80)
    
    def close(self):
        """Close database connection"""
        self.conn.close()

# Run the dashboard
if __name__ == "__main__":
    dashboard = FinancialDashboard()
    dashboard.generate_report()
    dashboard.close()
