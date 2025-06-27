
# Windsurf IDE Setup Guide

## Overview
This guide will help you set up the YFinance Financial Data Analysis Platform in Windsurf IDE.

## Prerequisites
- Windsurf IDE installed
- Python 3.8+ available
- PostgreSQL database access

## Step-by-Step Setup

### 1. Project Import
1. Open Windsurf IDE
2. Clone or import this repository
3. Wait for Windsurf to index the project

### 2. Environment Setup
```bash
# Install Python dependencies
pip install -r requirements.txt
```

### 3. Database Configuration
1. Ensure PostgreSQL is running
2. Create database:
   ```sql
   CREATE DATABASE yfinance_db;
   ```
3. Update `database_config.py` with your credentials

### 4. Initial Data Setup
Use the built-in workflows:
1. Click on "Workflows" panel
2. Run "Setup Database" workflow
3. Monitor progress in the terminal

### 5. Launch Application
1. Run "Start YFinance App" workflow
2. Access the web interface at `http://localhost:5000`

## Windsurf-Specific Features

### Workflows
The project includes pre-configured workflows accessible from the Windsurf sidebar:
- **Setup Database**: Initialize database with NSE data
- **Download All NSE Stocks**: Complete market data download
- **Download Missing Companies**: Incremental updates
- **Start YFinance App**: Launch web interface

### Integrated Tools
- **Database Explorer**: Browse PostgreSQL tables directly
- **Python Console**: Interactive data analysis
- **File Explorer**: Navigate complex project structure
- **Integrated Terminal**: Run scripts and commands

### Development Features
- **Auto-completion**: Financial data APIs and database queries
- **Debugging**: Set breakpoints in data processing scripts
- **Version Control**: Git integration for collaboration
- **Extensions**: Python, PostgreSQL, and Flask support

## Quick Commands

### Data Management
```bash
# Check data completeness
python data_completeness_checker.py

# Download specific stock data
python main_data_loader.py --symbol RELIANCE.NS

# Calculate financial metrics
python historical_metrics_calculator.py
```

### Database Operations
```bash
# Export database
python export_database.py

# Run analysis queries
python query_builder.py

# Generate reports
python financial_dashboard.py
```

## Troubleshooting

### Common Issues

**Database Connection Error**
- Check PostgreSQL service status
- Verify credentials in `database_config.py`
- Ensure database exists

**Module Import Errors**
- Run `pip install -r requirements.txt`
- Check Python version compatibility

**Data Download Issues**
- Verify internet connection
- Check Yahoo Finance API status
- Review rate limiting settings

### Performance Optimization
- Use Windsurf's indexing for faster code navigation
- Enable PostgreSQL query optimization
- Configure appropriate memory settings for large datasets

## Best Practices

### Code Organization
- Use Windsurf's project structure for navigation
- Leverage code folding for large files
- Utilize search functionality for quick access

### Database Management
- Regular backup exports
- Monitor disk space usage
- Use query builder for complex analyses

### Development Workflow
1. Use workflows for routine operations
2. Test changes with small datasets first
3. Monitor application logs
4. Regular code commits

## Advanced Features

### Custom Workflows
Create additional workflows in Windsurf for:
- Automated testing
- Data validation
- Report generation
- Backup operations

### Integration Options
- Connect to external databases
- API integrations for additional data sources
- Export capabilities for other analysis tools

## Support
For Windsurf-specific issues:
- Check Windsurf documentation
- Use built-in help system
- Community forums and support

For application-specific issues:
- Review application logs
- Check database status
- Consult main README.md
