
#!/usr/bin/env python3
"""
Script to refresh company metrics directly from yfinance
"""

import sys
import logging
from historical_metrics_calculator import HistoricalMetricsCalculator
from database_config import get_db_connection

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def refresh_single_company(symbol):
    """Refresh metrics for a single company"""
    logger = setup_logging()
    
    try:
        # Get company ID
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM companies WHERE symbol = %s", (symbol,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not result:
            logger.error(f"Company {symbol} not found in database")
            return False
        
        company_id = result[0]
        
        # Refresh metrics
        calculator = HistoricalMetricsCalculator()
        calculator.connect_db()
        calculator.populate_historical_metrics_for_company(company_id, symbol)
        calculator.close_db()
        
        logger.info(f"✓ Successfully refreshed metrics for {symbol}")
        return True
        
    except Exception as e:
        logger.error(f"✗ Error refreshing metrics for {symbol}: {e}")
        return False

def refresh_all_companies():
    """Refresh current metrics for all companies"""
    logger = setup_logging()
    
    try:
        calculator = HistoricalMetricsCalculator()
        calculator.populate_all_current_metrics()
        logger.info("✓ Successfully refreshed all current metrics")
        return True
        
    except Exception as e:
        logger.error(f"✗ Error refreshing all metrics: {e}")
        return False

def main():
    """Main function"""
    logger = setup_logging()
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python refresh_yfinance_metrics.py <symbol>     # Refresh specific company")
        print("  python refresh_yfinance_metrics.py --all        # Refresh all companies")
        print("")
        print("Examples:")
        print("  python refresh_yfinance_metrics.py RELIANCE.NS")
        print("  python refresh_yfinance_metrics.py --all")
        return
    
    if sys.argv[1] == "--all":
        logger.info("=== Refreshing All Company Metrics ===")
        success = refresh_all_companies()
    else:
        symbol = sys.argv[1].upper()
        logger.info(f"=== Refreshing Metrics for {symbol} ===")
        success = refresh_single_company(symbol)
    
    if success:
        logger.info("=== Refresh Completed Successfully ===")
    else:
        logger.error("=== Refresh Failed ===")
        sys.exit(1)

if __name__ == "__main__":
    main()
