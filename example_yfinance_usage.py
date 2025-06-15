import yfinance as yf

# Example 1: Download historical market data for Apple (AAPL)
ticker = yf.Ticker("AAPL")
data = ticker.history(period="1mo")  # Last 1 month of daily data
print("AAPL last 1 month data:")
print(data)

# Example 2: Get current info for Apple (AAPL)
info = ticker.info
print("\nAAPL info:")
print(info)

# Example 3: Download data for multiple tickers
multi_data = yf.download(["AAPL", "MSFT", "GOOGL"], period="5d")
print("\nMultiple tickers (AAPL, MSFT, GOOGL) last 5 days:")
print(multi_data)

# Example 4: Download historical data for Indian stocks (NSE & BSE)
# NSE Example: Reliance Industries
nse_ticker = yf.Ticker("RELIANCE.NS")

# Download all available historical data
df_all = nse_ticker.history(period="max")
print("\nNSE - Reliance Industries (RELIANCE.NS) ALL available data:")
print(df_all.head())
print(df_all.tail())

# Save to CSV
df_all.to_csv("RELIANCE_NS_all_history.csv")

# Last 1 month data (for reference)
nse_data = nse_ticker.history(period="1mo")
print("\nNSE - Reliance Industries (RELIANCE.NS) last 1 month:")
print(nse_data)

# Example 6: Download and save all major Yahoo Finance data for RELIANCE.NS
import csv
import pandas as pd

# 1. Financial Statements
print("Saving annual and quarterly financial statements...")
try:
    nse_ticker.income_stmt.to_csv("RELIANCE_NS_income_stmt_annual.csv")
    nse_ticker.quarterly_income_stmt.to_csv("RELIANCE_NS_income_stmt_quarterly.csv")
    nse_ticker.balance_sheet.to_csv("RELIANCE_NS_balance_sheet_annual.csv")
    nse_ticker.quarterly_balance_sheet.to_csv("RELIANCE_NS_balance_sheet_quarterly.csv")
    nse_ticker.cashflow.to_csv("RELIANCE_NS_cashflow_annual.csv")
    nse_ticker.quarterly_cashflow.to_csv("RELIANCE_NS_cashflow_quarterly.csv")
    print("Financial statements saved.")
except Exception as e:
    print(f"Error saving financial statements: {e}")

# 2. Other Corporate Data
print("Saving other corporate data...")
try:
    # Earnings Calendar
    try:
        # Convert dict to DataFrame if needed
        cal = nse_ticker.calendar
        if isinstance(cal, dict):
            cal = pd.DataFrame([cal])
        elif not hasattr(cal, 'to_csv'):
            cal = pd.DataFrame(cal)
        cal.to_csv("RELIANCE_NS_earnings_calendar.csv")
    except Exception as e:
        print(f"Earnings calendar not available: {e}")
    # Earnings Dates
    try:
        # Convert dict to DataFrame if needed
        edates = nse_ticker.earnings_dates
        if isinstance(edates, dict):
            edates = pd.DataFrame([edates])
        elif not hasattr(edates, 'to_csv'):
            edates = pd.DataFrame(edates)
        edates.to_csv("RELIANCE_NS_earnings_dates.csv")
    except Exception as e:
        print(f"Earnings dates not available: {e}")
    # SEC Filings (may not be available for Indian stocks)
    try:
        # Convert dict to DataFrame if needed
        sec = nse_ticker.sec_filings
        if isinstance(sec, dict):
            sec = pd.DataFrame([sec])
        elif not hasattr(sec, 'to_csv'):
            sec = pd.DataFrame(sec)
        sec.to_csv("RELIANCE_NS_sec_filings.csv")
    except Exception as e:
        print(f"SEC filings not available: {e}")
    # Corporate Actions
    try:
        # Convert dict to DataFrame if needed
        actions = nse_ticker.actions
        if isinstance(actions, dict):
            actions = pd.DataFrame([actions])
        elif not hasattr(actions, 'to_csv'):
            actions = pd.DataFrame(actions)
        actions.to_csv("RELIANCE_NS_corporate_actions.csv")
    except Exception as e:
        print(f"Corporate actions not available: {e}")
    # Dividends
    try:
        # Convert dict to DataFrame if needed
        dividends = nse_ticker.dividends
        if isinstance(dividends, dict):
            dividends = pd.DataFrame([dividends])
        elif not hasattr(dividends, 'to_csv'):
            dividends = pd.DataFrame(dividends)
        dividends.to_csv("RELIANCE_NS_dividends.csv")
    except Exception as e:
        print(f"Dividends not available: {e}")
    # Splits
    try:
        # Convert dict to DataFrame if needed
        splits = nse_ticker.splits
        if isinstance(splits, dict):
            splits = pd.DataFrame([splits])
        elif not hasattr(splits, 'to_csv'):
            splits = pd.DataFrame(splits)
        splits.to_csv("RELIANCE_NS_splits.csv")
    except Exception as e:
        print(f"Splits not available: {e}")
    # Institutional Holders
    try:
        # Convert dict to DataFrame if needed
        inst = nse_ticker.institutional_holders
        if isinstance(inst, dict):
            inst = pd.DataFrame([inst])
        elif not hasattr(inst, 'to_csv'):
            inst = pd.DataFrame(inst)
        inst.to_csv("RELIANCE_NS_institutional_holders.csv")
    except Exception as e:
        print(f"Institutional holders not available: {e}")
    # Major Holders
    try:
        # Convert dict to DataFrame if needed
        majors = nse_ticker.major_holders
        if isinstance(majors, dict):
            majors = pd.DataFrame([majors])
        elif not hasattr(majors, 'to_csv'):
            majors = pd.DataFrame(majors)
        majors.to_csv("RELIANCE_NS_major_holders.csv")
    except Exception as e:
        print(f"Major holders not available: {e}")
    print("Other corporate data saved.")
except Exception as e:
    print(f"Error saving other corporate data: {e}")

# Get latest news
try:
    news = nse_ticker.news
    print("\nLatest news for RELIANCE.NS:")
    for item in news[:5]:  # Show up to 5 news items
        print(f"- {item.get('title')}: {item.get('link')}")
except Exception as e:
    print(f"Could not fetch news: {e}")

# BSE Example: Reliance Industries (BSE code: 500325)
bse_ticker = yf.Ticker("500325.BO")
bse_data = bse_ticker.history(period="1mo")
print("\nBSE - Reliance Industries (500325.BO) last 1 month:")
print(bse_data)
