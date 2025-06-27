
# YFinance Financial Data Analysis Platform

## Project Context

This project is a comprehensive financial data analysis platform specifically designed for Indian stock markets (NSE/BSE). It uses the yfinance library to fetch financial data from Yahoo Finance and stores it in a PostgreSQL database for analysis.

## Architecture

### Backend Components
- **Flask Web Application** (`app.py`): Main web interface
- **Data Loaders**: Multiple Python scripts for downloading and processing financial data
- **PostgreSQL Database**: Structured storage for all financial data
- **Analysis Tools**: Various utilities for financial analysis and reporting

### Key Features
- Real-time NSE stock data downloading
- Comprehensive financial metrics calculation
- Historical data analysis
- Web-based dashboard for visualization
- Database-driven architecture for scalability

### Database Schema
The application uses a sophisticated PostgreSQL schema with tables for:
- Company overview and metadata
- Daily price data
- Financial statements (income, balance sheet, cash flow)
- Historical metrics and ratios
- Market data and trading information

### Data Sources
- Primary: Yahoo Finance API via yfinance library
- Focus: NSE (National Stock Exchange of India)
- Secondary: BSE (Bombay Stock Exchange) support

## Development Workflow

1. **Database Setup**: Run database initialization scripts
2. **Data Population**: Use downloader scripts to fetch financial data
3. **Web Interface**: Start Flask application for data access
4. **Analysis**: Use various analysis tools and queries

## File Organization

- Root level: Main application files and configuration
- `yfinance/`: Core yfinance library (if modified)
- `doc/`: Documentation and guides
- `tests/`: Test suite for validation
- Various Python scripts for specific data operations

This is a production-ready financial analysis platform suitable for investment research, market analysis, and educational purposes.
