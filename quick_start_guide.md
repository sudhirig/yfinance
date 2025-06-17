
# NSE Stock Data Download - Quick Start Guide

This project allows you to download and store comprehensive financial data for all NSE (National Stock Exchange) stocks directly into a PostgreSQL database.

## Quick Start

### Option 1: Download All NSE Stocks (Recommended)
```bash
python main_data_loader.py --download-nse
```

This will:
- Fetch 100+ major NSE stock symbols
- Download 5 years of historical price data
- Download financial statements (annual & quarterly)
- Download company information and metrics
- Store everything in the database

### Option 2: Load from CSV Files
```bash
python main_data_loader.py --csv
```

This loads data from existing CSV files in the `attached_assets` directory.

## What Data Gets Downloaded

For each NSE stock, the system downloads:

1. **Company Information**
   - Basic company details (name, sector, industry)
   - Business summary and contact information
   - Exchange and trading information

2. **Price History** (5 years)
   - Daily OHLCV data
   - Dividends and stock splits
   - Adjusted close prices

3. **Financial Statements**
   - Income statements (annual & quarterly)
   - Balance sheets (annual & quarterly)
   - Cash flow statements (annual & quarterly)

4. **Company Metrics**
   - Market cap, P/E ratios, financial ratios
   - Analyst recommendations
   - Risk metrics

5. **Corporate Actions**
   - Dividend payments
   - Stock splits

## Database Schema

The data is stored in these main tables:
- `companies` - Company master data
- `price_history` - Historical price data
- `company_metrics` - Current financial metrics
- `income_statements` - Income statement data
- `balance_sheets` - Balance sheet data
- `cash_flow_statements` - Cash flow data
- `corporate_actions` - Dividends and splits

## Example Queries

### Top 10 companies by market cap
```sql
SELECT symbol, long_name, sector, market_cap 
FROM company_overview 
ORDER BY market_cap DESC LIMIT 10;
```

### Latest prices for technology stocks
```sql
SELECT symbol, long_name, close_price, volume, date
FROM latest_price_data 
WHERE sector = 'Technology' 
ORDER BY close_price DESC;
```

### Annual financials for a specific company
```sql
SELECT period_ending, total_revenue, net_income, total_assets
FROM annual_financials 
WHERE symbol = 'RELIANCE.NS'
ORDER BY period_ending DESC;
```

## Features

- **Batch Processing**: Downloads stocks in batches with rate limiting
- **Error Handling**: Continues downloading even if some stocks fail
- **Progress Tracking**: Detailed logging of download progress
- **Data Validation**: Validates and cleans data before storing
- **Incremental Updates**: Can update existing data without duplicates

## Configuration

You can customize the download process by modifying `yfinance_nse_downloader.py`:

- `batch_size = 10` - Number of stocks to process per batch
- `delay_between_stocks = 1` - Seconds to wait between each stock
- `delay_between_batches = 5` - Seconds to wait between batches

## Monitoring Progress

The download process creates a log file `yfinance_nse_downloader.log` with detailed progress information. You can monitor it in real-time:

```bash
tail -f yfinance_nse_downloader.log
```

## Database Views

Several views are created for easy data access:
- `company_overview` - Combined company and latest metrics
- `latest_price_data` - Most recent price data for all stocks
- `annual_financials` - Combined annual financial statements
- `quarterly_financials` - Combined quarterly financial statements

Start with the download command and let the system fetch all NSE data automatically!
