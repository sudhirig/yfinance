
# YFinance Database Loader

This project provides a comprehensive solution to load yfinance data from CSV files into a PostgreSQL database for analysis.

## Features

- **Complete Database Schema**: Handles all yfinance data types including company info, price history, financial statements, corporate actions, and more
- **CSV Import**: Automatically imports data from CSV files downloaded from yfinance
- **Data Validation**: Includes data type conversion and validation
- **PostgreSQL Optimized**: Uses proper indexes and views for efficient querying

## Quick Start

### 1. Setup PostgreSQL Database in Replit

1. Click the "Tools" button in the sidebar
2. Select "Database" 
3. Click "Create Database" to set up PostgreSQL
4. The database connection details will be automatically configured

### 2. Run the Data Loader

Simply click the **Run** button or execute:

```bash
python main_data_loader.py
```

This will:
- Check database connection
- Create the database schema
- Load all CSV files from `attached_assets/` directory
- Verify the data load

### 3. Query Your Data

After loading, you can query your data using the SQL explorer in Replit or connect programmatically:

```python
import psycopg2
import os

# Connect to database
conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cursor = conn.cursor()

# Example query
cursor.execute("SELECT * FROM company_overview LIMIT 5;")
results = cursor.fetchall()
for row in results:
    print(row)
```

## Database Schema

### Main Tables

1. **companies** - Company master data (symbol, name, industry, etc.)
2. **price_history** - Daily OHLCV data with dividends and splits
3. **company_metrics** - Current financial metrics (PE, market cap, etc.)
4. **income_statements** - Income statement data (annual/quarterly)
5. **balance_sheets** - Balance sheet data (annual/quarterly)
6. **cash_flow_statements** - Cash flow data (annual/quarterly)
7. **corporate_actions** - Dividends and stock splits
8. **earnings** - Earnings estimates and actuals
9. **holders** - Institutional and major holders

### Views

- **company_overview** - Combined company and current metrics
- **latest_price_data** - Most recent price data for all companies
- **annual_financials** - Combined annual financial statements
- **quarterly_financials** - Combined quarterly financial statements

## Example Queries

See `example_queries.sql` for comprehensive analysis examples including:

- Financial health analysis
- Price performance tracking
- Dividend analysis
- Valuation comparisons
- Revenue growth trends
- Cash flow analysis
- Volatility calculations

## File Structure

```
├── enhanced_yfinance_schema.sql    # Database schema
├── csv_to_database_loader.py       # Main loader class
├── database_config.py              # Database configuration
├── main_data_loader.py             # Main execution script
├── example_queries.sql             # Example analysis queries
└── attached_assets/                # CSV files directory
    ├── RELIANCE_NS_all_history_*.csv
    ├── RELIANCE_NS_full_data_*.csv
    └── ... (other CSV files)
```

## Supported CSV File Types

The loader automatically detects and processes:

- `*_all_info_*.csv` - Complete company information
- `*_full_data_*.csv` - Extended company data with metrics
- `*_all_history_*.csv` - Historical price data
- `*_income_stmt_annual_*.csv` - Annual income statements
- `*_income_stmt_quarterly_*.csv` - Quarterly income statements
- `*_balance_sheet_annual_*.csv` - Annual balance sheets
- `*_balance_sheet_quarterly_*.csv` - Quarterly balance sheets
- `*_cashflow_annual_*.csv` - Annual cash flow statements
- `*_cashflow_quarterly_*.csv` - Quarterly cash flow statements
- `*_corporate_actions_*.csv` - Dividends and stock splits
- `*_earnings_dates_*.csv` - Earnings calendar data

## Configuration

The system automatically uses Replit's PostgreSQL environment variables:
- `DATABASE_URL`
- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`

For custom configurations, modify `database_config.py`.

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Ensure PostgreSQL database is created in Replit
   - Check environment variables are set

2. **CSV Files Not Found**
   - Ensure CSV files are in `attached_assets/` directory
   - Check file naming conventions match expected patterns

3. **Data Loading Errors**
   - Check CSV file formats and headers
   - Review logs in `yfinance_loader.log`

### Data Quality

The loader includes extensive data validation:
- Automatic type conversion (int, float, date)
- NULL handling for missing values
- Duplicate prevention with unique constraints
- Error logging for debugging

## Performance

- Bulk inserts for large datasets
- Optimized indexes for common queries
- Connection pooling ready
- Efficient data types and constraints

## Next Steps

After loading your data, you can:

1. **Build Dashboards**: Connect to visualization tools
2. **Create Reports**: Use the example queries as templates
3. **Add More Data**: Load additional stocks or time periods
4. **Automate Updates**: Schedule regular data refreshes

For more advanced usage, see the example queries and modify the loader to suit your specific needs.
