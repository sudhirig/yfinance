
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

class YFinanceHistoricalMetricsFetcher:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def get_historical_financials(self, symbol, years_back=5):
        """
        Fetch comprehensive historical financial data directly from yfinance
        """
        try:
            ticker = yf.Ticker(symbol)
            
            # Get historical price data
            end_date = datetime.now()
            start_date = end_date - timedelta(days=years_back*365)
            
            hist_data = ticker.history(start=start_date, end=end_date, interval="1d")
            
            # Get financial statements (annual and quarterly)
            financials = {
                'income_stmt_annual': ticker.financials,
                'income_stmt_quarterly': ticker.quarterly_financials,
                'balance_sheet_annual': ticker.balance_sheet,
                'balance_sheet_quarterly': ticker.quarterly_balance_sheet,
                'cashflow_annual': ticker.cashflow,
                'cashflow_quarterly': ticker.quarterly_cashflow
            }
            
            # Get current info for context
            info = ticker.info
            
            return {
                'price_history': hist_data,
                'financial_statements': financials,
                'info': info,
                'dividends': ticker.dividends,
                'splits': ticker.splits,
                'actions': ticker.actions
            }
            
        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            return None
    
    def calculate_historical_metrics(self, symbol, years_back=5):
        """
        Calculate historical metrics from yfinance data
        """
        data = self.get_historical_financials(symbol, years_back)
        if not data:
            return None
        
        hist_data = data['price_history']
        financials = data['financial_statements']
        info = data['info']
        
        metrics_timeline = []
        
        # Process quarterly data
        quarterly_income = financials['income_stmt_quarterly']
        quarterly_balance = financials['balance_sheet_quarterly']
        
        if not quarterly_income.empty and not quarterly_balance.empty:
            for date in quarterly_income.columns:
                try:
                    # Get price data around this date
                    date_prices = hist_data[hist_data.index.date <= date.date()]
                    if date_prices.empty:
                        continue
                    
                    latest_price = date_prices['Close'].iloc[-1]
                    market_cap = latest_price * info.get('sharesOutstanding', 0)
                    
                    # Calculate metrics
                    metrics = {
                        'date': date,
                        'period_type': 'quarterly',
                        'close_price': latest_price,
                        'market_cap': market_cap,
                    }
                    
                    # Revenue and earnings
                    total_revenue = quarterly_income.loc['Total Revenue', date] if 'Total Revenue' in quarterly_income.index else None
                    net_income = quarterly_income.loc['Net Income', date] if 'Net Income' in quarterly_income.index else None
                    
                    # Balance sheet items
                    total_assets = quarterly_balance.loc['Total Assets', date] if 'Total Assets' in quarterly_balance.index else None
                    shareholders_equity = quarterly_balance.loc['Stockholders Equity', date] if 'Stockholders Equity' in quarterly_balance.index else None
                    
                    # Calculate ratios
                    if net_income and latest_price and info.get('sharesOutstanding'):
                        eps = net_income / info.get('sharesOutstanding', 1)
                        pe_ratio = latest_price / eps if eps > 0 else None
                        metrics['trailing_pe'] = pe_ratio
                    
                    if net_income and shareholders_equity and shareholders_equity > 0:
                        metrics['return_on_equity'] = net_income / shareholders_equity
                    
                    if total_revenue:
                        metrics['total_revenue'] = total_revenue
                        if market_cap:
                            metrics['price_to_sales'] = market_cap / total_revenue
                    
                    if shareholders_equity and info.get('sharesOutstanding'):
                        book_value_per_share = shareholders_equity / info.get('sharesOutstanding', 1)
                        metrics['book_value_per_share'] = book_value_per_share
                        if latest_price:
                            metrics['price_to_book'] = latest_price / book_value_per_share
                    
                    metrics_timeline.append(metrics)
                    
                except Exception as e:
                    self.logger.warning(f"Error calculating metrics for {date}: {e}")
                    continue
        
        return pd.DataFrame(metrics_timeline).sort_values('date')
    
    def get_price_based_metrics(self, symbol, period="5y"):
        """
        Get price-based historical metrics like PE, dividend yield over time
        """
        try:
            ticker = yf.Ticker(symbol)
            
            # Get historical data with longer period for more context
            hist = ticker.history(period=period)
            
            # Get current info for shares outstanding
            info = ticker.info
            shares_outstanding = info.get('sharesOutstanding', info.get('impliedSharesOutstanding', 0))
            
            # Add calculated metrics to historical data
            hist['market_cap'] = hist['Close'] * shares_outstanding
            
            # Get dividends and calculate yield
            dividends = ticker.dividends
            if not dividends.empty:
                # Calculate trailing 12-month dividend yield
                hist['dividend_yield'] = 0.0
                for date in hist.index:
                    twelve_months_ago = date - timedelta(days=365)
                    recent_divs = dividends[(dividends.index > twelve_months_ago) & (dividends.index <= date)]
                    if not recent_divs.empty and hist.loc[date, 'Close'] > 0:
                        annual_dividend = recent_divs.sum()
                        hist.loc[date, 'dividend_yield'] = annual_dividend / hist.loc[date, 'Close']
            
            return hist
            
        except Exception as e:
            self.logger.error(f"Error fetching price metrics for {symbol}: {e}")
            return None
    
    def compare_historical_metrics(self, symbols, metric='trailing_pe', years=3):
        """
        Compare a specific metric across multiple symbols historically
        """
        comparison_data = {}
        
        for symbol in symbols:
            metrics_df = self.calculate_historical_metrics(symbol, years)
            if metrics_df is not None and metric in metrics_df.columns:
                comparison_data[symbol] = metrics_df[['date', metric]].set_index('date')[metric]
        
        if comparison_data:
            return pd.DataFrame(comparison_data)
        return None

# Example usage
if __name__ == "__main__":
    fetcher = YFinanceHistoricalMetricsFetcher()
    
    # Get historical metrics for a single stock
    print("Fetching historical metrics for RELIANCE.NS...")
    metrics = fetcher.calculate_historical_metrics("RELIANCE.NS", years_back=3)
    if metrics is not None:
        print(f"Found {len(metrics)} quarterly metrics")
        print(metrics.tail())
    
    # Get price-based metrics
    print("\nFetching price-based metrics...")
    price_metrics = fetcher.get_price_based_metrics("RELIANCE.NS", period="2y")
    if price_metrics is not None:
        print(f"Price data from {price_metrics.index[0]} to {price_metrics.index[-1]}")
        print(price_metrics[['Close', 'market_cap', 'dividend_yield']].tail())
    
    # Compare multiple stocks
    print("\nComparing historical PE ratios...")
    comparison = fetcher.compare_historical_metrics(
        ['RELIANCE.NS', 'TCS.NS', 'INFY.NS'], 
        metric='trailing_pe', 
        years=2
    )
    if comparison is not None:
        print(comparison.tail())
