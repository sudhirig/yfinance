
#!/usr/bin/env python3
"""
Interactive Query Builder for NSE Financial Data
"""

import psycopg2
import os
import pandas as pd

class QueryBuilder:
    def __init__(self):
        self.conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    
    def run_custom_query(self, query):
        """Execute custom SQL query and return results"""
        try:
            df = pd.read_sql(query, self.conn)
            return df
        except Exception as e:
            print(f"Error executing query: {e}")
            return None
    
    def show_available_tables(self):
        """Show all available tables and views"""
        query = """
        SELECT table_name, table_type 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        ORDER BY table_type, table_name;
        """
        return pd.read_sql(query, self.conn)
    
    def describe_table(self, table_name):
        """Show columns in a specific table"""
        query = """
        SELECT column_name, data_type, is_nullable 
        FROM information_schema.columns 
        WHERE table_name = %s 
        ORDER BY ordinal_position;
        """
        return pd.read_sql(query, self.conn, params=[table_name])

# Example usage functions
def example_queries():
    """Show example queries for common analysis"""
    
    queries = {
        "1. Top 10 Stocks by Volume": """
        SELECT c.symbol, c.long_name, ph.volume, ph.close_price, ph.date
        FROM companies c
        JOIN price_history ph ON c.id = ph.company_id
        WHERE ph.date = (SELECT MAX(date) FROM price_history WHERE company_id = c.id)
        ORDER BY ph.volume DESC LIMIT 10;
        """,
        
        "2. Revenue Growth Leaders": """
        WITH revenue_growth AS (
            SELECT c.symbol, c.long_name,
                   i.total_revenue,
                   LAG(i.total_revenue) OVER (PARTITION BY c.id ORDER BY i.period_ending) as prev_revenue,
                   ROUND(((i.total_revenue - LAG(i.total_revenue) OVER (PARTITION BY c.id ORDER BY i.period_ending))::numeric 
                         / LAG(i.total_revenue) OVER (PARTITION BY c.id ORDER BY i.period_ending) * 100), 2) as growth_pct
            FROM companies c
            JOIN income_statements i ON c.id = i.company_id
            WHERE i.period_type = 'annual'
        )
        SELECT symbol, long_name, total_revenue, growth_pct
        FROM revenue_growth 
        WHERE growth_pct IS NOT NULL
        ORDER BY growth_pct DESC LIMIT 10;
        """,
        
        "3. High Cash Flow Companies": """
        SELECT c.symbol, c.long_name, cf.free_cash_flow/1000000 as fcf_millions,
               cf.operating_cash_flow/1000000 as ocf_millions
        FROM companies c
        JOIN cash_flow_statements cf ON c.id = cf.company_id
        WHERE cf.period_type = 'annual' AND cf.free_cash_flow > 0
        ORDER BY cf.free_cash_flow DESC LIMIT 15;
        """,
        
        "4. Price Volatility Analysis": """
        WITH daily_returns AS (
            SELECT company_id, 
                   STDDEV((close_price - LAG(close_price) OVER (PARTITION BY company_id ORDER BY date)) 
                         / LAG(close_price) OVER (PARTITION BY company_id ORDER BY date)) * 100 as volatility
            FROM price_history
            WHERE date >= CURRENT_DATE - INTERVAL '1 year'
            GROUP BY company_id
        )
        SELECT c.symbol, c.long_name, ROUND(dr.volatility, 2) as volatility_percent
        FROM companies c
        JOIN daily_returns dr ON c.id = dr.company_id
        WHERE dr.volatility IS NOT NULL
        ORDER BY dr.volatility LIMIT 10;
        """
    }
    
    qb = QueryBuilder()
    
    print("🔍 EXAMPLE FINANCIAL ANALYSIS QUERIES")
    print("=" * 60)
    
    for title, query in queries.items():
        print(f"\n{title}")
        print("-" * 50)
        try:
            result = qb.run_custom_query(query)
            if result is not None:
                print(result.to_string(index=False))
        except Exception as e:
            print(f"Error: {e}")
        print()

if __name__ == "__main__":
    example_queries()
