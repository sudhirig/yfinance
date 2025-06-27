
<img src="./doc/yfinance-gh-logo-dark.webp#gh-dark-mode-only" height="100">
<img src="./doc/yfinance-gh-logo-light.webp#gh-light-mode-only" height="100">

# YFinance Financial Data Analysis Platform

<a target="new" href="https://pypi.python.org/pypi/yfinance"><img border=0 src="https://img.shields.io/badge/python-3.8+-blue.svg?style=flat" alt="Python version"></a>
<a target="new" href="https://pypi.python.org/pypi/yfinance"><img border=0 src="https://img.shields.io/pypi/v/yfinance.svg?maxAge=60%" alt="PyPi version"></a>
<a target="new" href="https://github.com/ranaroussi/yfinance"><img border=0 src="https://img.shields.io/github/stars/ranaroussi/yfinance.svg?style=social&label=Star&maxAge=60" alt="Star this repo"></a>

**A comprehensive financial data analysis platform** built on top of yfinance, specifically designed for Indian stock markets (NSE/BSE) with PostgreSQL database integration and web-based visualization.

---

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- PostgreSQL database
- Internet connection for data downloads

### Setup in Windsurf IDE

1. **Clone and Open**: Open this project in Windsurf IDE
2. **Install Dependencies**: 
   ```bash
   pip install -r requirements.txt
   ```
3. **Database Setup**: Follow the [Database Setup Guide](database_setup_instructions.md)
4. **Initialize Data**: Run the "Setup Database" workflow
5. **Launch Application**: Use the "Start YFinance App" workflow

### Available Workflows

| Workflow | Purpose | Commands |
|----------|---------|----------|
| **Setup Database** | Initialize database and download initial NSE data | `python main_data_loader.py --download-nse` |
| **Download All NSE Stocks** | Complete NSE universe download | `python yfinance_nse_downloader.py` |
| **Download Missing Companies** | Incremental updates for missing data | `python check_missing_companies.py` + data loader |
| **Start YFinance App** | Launch Flask web interface | `python app.py` |

---

## 🏗️ Architecture

### Core Components

- **Flask Web Application** (`app.py`): Main dashboard and API endpoints
- **Data Management**: Automated downloaders and database loaders
- **PostgreSQL Database**: Structured financial data storage
- **Analysis Tools**: Comprehensive financial metrics and reporting

### Database Schema

```sql
-- Key Tables
- company_overview: Company metadata and basic information
- latest_price_data: Current and historical price data
- annual_financials: Income statements, balance sheets, cash flows
- historical_metrics: Calculated financial ratios and metrics
- market_data: Trading volumes, market cap, etc.
```

### API Endpoints

- `GET /`: Main dashboard interface
- `GET /api/stocks`: Stock listing and search
- `GET /api/company/{symbol}`: Detailed company information
- `GET /api/metrics/{symbol}`: Financial metrics and ratios

---

## 📊 Features

### Data Coverage
- **NSE (National Stock Exchange)**: Complete universe coverage
- **BSE (Bombay Stock Exchange)**: Major listings
- **Real-time Data**: Live price feeds and market data
- **Historical Data**: Up to 20+ years of historical information

### Financial Metrics
- Fundamental ratios (P/E, P/B, ROE, ROA, etc.)
- Technical indicators
- Growth metrics and trends
- Comparative analysis tools

### Visualization
- Interactive charts and graphs
- Portfolio tracking capabilities
- Market screening and filtering
- Custom dashboard creation

---

## 🛠️ Development

### Project Structure

```
├── app.py                          # Main Flask application
├── main_data_loader.py             # Core data loading system
├── yfinance_nse_downloader.py      # NSE data downloader
├── financial_dashboard.py          # Analysis and visualization
├── database_config.py              # Database configuration
├── yfinance/                       # Core yfinance library
├── doc/                           # Documentation
└── tests/                         # Test suite
```

### Key Scripts

| Script | Purpose |
|--------|---------|
| `data_completeness_checker.py` | Validate data integrity |
| `historical_metrics_calculator.py` | Calculate financial ratios |
| `query_builder.py` | Interactive database queries |
| `financial_dashboard.py` | Generate analysis reports |

### Database Management

```bash
# Check data status
python data_completeness_checker.py

# Export database
python export_database.py

# Import database backup
python import_database.py
```

---

## 📚 Documentation

- [Database Setup Instructions](database_setup_instructions.md)
- [Quick Start Guide](quick_start_guide.md)
- [API Documentation](example_queries.sql)
- [Development Guide](CONTRIBUTING.md)

---

## 🔧 Configuration

### Environment Variables
Create a `.env` file for database configuration:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=yfinance_db
DB_USER=your_username
DB_PASSWORD=your_password
```

### Windsurf IDE Setup
This project is optimized for Windsurf IDE with:
- Pre-configured workflows for common tasks
- Integrated database management
- Built-in debugging and testing tools
- Auto-completion for financial data APIs

---

## 📈 Example Usage

### Fetch NSE Stock Data

```python
import yfinance as yf

# NSE Example: Reliance Industries
nse_ticker = yf.Ticker("RELIANCE.NS")
nse_data = nse_ticker.history(period="1y")
print("NSE - Reliance Industries last 1 year:")
print(nse_data)

# BSE Example: Reliance Industries
bse_ticker = yf.Ticker("500325.BO")
bse_data = bse_ticker.history(period="1y")
print("BSE - Reliance Industries last 1 year:")
print(bse_data)
```

### Database Queries

```python
from database_config import get_db_connection

# Get top companies by market cap
query = """
SELECT symbol, long_name, sector, market_cap 
FROM company_overview 
WHERE market_cap IS NOT NULL 
ORDER BY market_cap DESC 
LIMIT 10;
"""

with get_db_connection() as conn:
    result = pd.read_sql(query, conn)
    print(result)
```

---

## 🚀 Deployment

This application is configured for deployment on Replit with:
- Gunicorn WSGI server for production
- PostgreSQL database integration
- Environment-based configuration
- Automatic scaling capabilities

```bash
# Production deployment
gunicorn --bind 0.0.0.0:5000 app:app
```

---

## 📋 Requirements

### System Requirements
- Python 3.8+
- PostgreSQL 12+
- 4GB+ RAM (for large datasets)
- Stable internet connection

### Python Dependencies
All dependencies are listed in [requirements.txt](requirements.txt), including:
- yfinance (latest)
- pandas, numpy
- Flask, psycopg2
- matplotlib, seaborn
- requests, beautifulsoup4

---

## ⚖️ Legal Notice

> [!IMPORTANT]  
> **Yahoo!, Y!Finance, and Yahoo! finance are registered trademarks of Yahoo, Inc.**
>
> This application is **not** affiliated, endorsed, or vetted by Yahoo, Inc. It's an open-source tool that uses Yahoo's publicly available APIs, and is intended for research and educational purposes.
> 
> **You should refer to Yahoo!'s terms of use** for details on your rights to use the actual data downloaded.
>
> **The Yahoo! finance API is intended for personal use only.**

---

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `python -m unittest discover -s tests`
5. Submit a pull request

---

## 📞 Support

- **Documentation**: [Full Documentation](https://ranaroussi.github.io/yfinance)
- **Issues**: [GitHub Issues](https://github.com/ranaroussi/yfinance/issues)
- **Discussions**: [GitHub Discussions](https://github.com/ranaroussi/yfinance/discussions)

---

**Built with ❤️ for the financial analysis community**

*This platform provides institutional-grade financial data analysis capabilities for individual investors, researchers, and developers.*
