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

# Example 5: Extract full set of commonly available yfinance data points and save to CSV for RELIANCE.NS
import csv

info = nse_ticker.info

fields = [
    'symbol', 'shortName', 'longName', 'displayName',
    'sector', 'industry', 'exchange', 'exchangeTimezoneName', 'exchangeTimezoneShortName', 'fullExchangeName', 'market', 'quoteType', 'region', 'language', 'country', 'city', 'address1', 'phone', 'website',
    'marketCap', 'sharesOutstanding', 'floatShares', 'impliedSharesOutstanding',
    'previousClose', 'open', 'regularMarketOpen', 'regularMarketPrice', 'regularMarketDayHigh', 'regularMarketDayLow', 'regularMarketVolume', 'averageVolume', 'averageVolume10days', 'averageDailyVolume10Day',
    'bid', 'ask', 'bidSize', 'askSize',
    'fiftyTwoWeekLow', 'fiftyTwoWeekHigh', 'fiftyTwoWeekRange', 'fiftyDayAverage', 'twoHundredDayAverage', 'fiftyDayAverageChange', 'twoHundredDayAverageChange', 'fiftyDayAverageChangePercent', 'twoHundredDayAverageChangePercent', 'fiftyTwoWeekLowChange', 'fiftyTwoWeekHighChange', 'fiftyTwoWeekLowChangePercent', 'fiftyTwoWeekHighChangePercent', 'fiftyTwoWeekChange', 'fiftyTwoWeekChangePercent',
    'trailingPE', 'forwardPE', 'trailingPegRatio', 'priceToBook', 'priceToSalesTrailing12Months', 'priceEpsCurrentYear', 'trailingEps', 'epsTrailingTwelveMonths', 'forwardEps', 'epsCurrentYear', 'beta',
    'bookValue', 'revenuePerShare', 'totalRevenue', 'grossProfits', 'grossMargins', 'profitMargins', 'operatingMargins', 'ebitda', 'ebitdaMargins', 'operatingCashflow', 'freeCashflow', 'enterpriseValue', 'enterpriseToRevenue', 'enterpriseToEbitda', 'netIncomeToCommon', 'returnOnAssets', 'returnOnEquity', 'debtToEquity', 'totalCash', 'totalCashPerShare', 'totalDebt', 'quickRatio', 'currentRatio',
    'dividendYield', 'dividendRate', 'trailingAnnualDividendRate', 'trailingAnnualDividendYield', 'fiveYearAvgDividendYield', 'payoutRatio', 'exDividendDate', 'lastDividendValue', 'lastDividendDate',
    'lastSplitFactor', 'lastSplitDate', 'sharesShort', 'sharesShortPreviousMonthDate', 'sharesShortPriorMonth', 'shortRatio', 'shortPercentOfFloat', 'heldPercentInsiders', 'heldPercentInstitutions',
    'earningsTimestamp', 'earningsTimestampStart', 'earningsTimestampEnd', 'earningsCallTimestampStart', 'earningsCallTimestampEnd', 'isEarningsDateEstimate', 'earningsQuarterlyGrowth', 'earningsGrowth', 'revenueGrowth', 'mostRecentQuarter', 'lastFiscalYearEnd', 'nextFiscalYearEnd',
    'analystTargetPrice', 'targetHighPrice', 'targetLowPrice', 'targetMeanPrice', 'targetMedianPrice', 'recommendationKey', 'recommendationMean', 'numberOfAnalystOpinions', 'averageAnalystRating',
    'fullTimeEmployees', 'auditRisk', 'boardRisk', 'compensationRisk', 'shareHolderRightsRisk', 'overallRisk', 'governanceEpochDate', 'compensationAsOfEpochDate',
    'longBusinessSummary', 'companyOfficers', 'executiveTeam', 'irWebsite', 'messageBoardId',
    'quoteSourceName', 'triggerable', 'customPriceAlertConfidence', 'cryptoTradeable', 'hasPrePostMarketData', 'tradeable', 'esgPopulated', 'esgScores', 'esgPerformance', 'esgRawScores', 'esgPercentile',
    'currency', 'financialCurrency', 'underlyingSymbol', 'underlyingExchangeSymbol', 'underlyingExchange', 'typeDisp', 'marketState', 'sourceInterval', 'exchangeDataDelayedBy', 'firstTradeDateMilliseconds',
    'corporateActions', 'corporateEvents', 'corporateGovernance', 'corporateProfile', 'corporateCalendar', 'corporateFilings'
]

# Extract the data, fill missing with None
row = {field: info.get(field, None) for field in fields}

# Add up to 5 latest news headlines and URLs
try:
    news = nse_ticker.news
    for i in range(5):
        if i < len(news):
            row[f'news_title_{i+1}'] = news[i].get('title')
            row[f'news_link_{i+1}'] = news[i].get('link')
        else:
            row[f'news_title_{i+1}'] = None
            row[f'news_link_{i+1}'] = None
except Exception as e:
    for i in range(5):
        row[f'news_title_{i+1}'] = None
        row[f'news_link_{i+1}'] = None

# Save to CSV
with open("RELIANCE_NS_full_data.csv", "w", newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=list(row.keys()))
    writer.writeheader()
    writer.writerow(row)

print("\nFull data for RELIANCE.NS saved to RELIANCE_NS_full_data.csv:")
for field in row:
    print(f"{field}: {row[field]}")

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
