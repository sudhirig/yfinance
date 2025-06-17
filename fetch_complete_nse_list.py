
#!/usr/bin/env python3
"""
Fetch complete list of all NSE stocks
"""

import requests
import pandas as pd
import time
import logging
from nse_symbols_fetcher import NSESymbolsFetcher

def fetch_all_nse_stocks():
    """Fetch complete NSE stock universe"""
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    fetcher = NSESymbolsFetcher()
    
    # Get complete NSE universe
    logger.info("Fetching complete NSE stock universe...")
    all_symbols = fetcher.get_all_nse_symbols(complete_universe=True)
    
    # Save to file
    fetcher.save_symbols_to_file(all_symbols, "nse_complete_universe.txt")
    
    # Print summary
    print(f"\n🎯 **COMPLETE NSE STOCK UNIVERSE**")
    print(f"📊 Total NSE Stocks: {len(all_symbols)}")
    print(f"📁 Saved to: nse_complete_universe.txt")
    
    # Show sample
    print(f"\n📋 Sample stocks:")
    for i, symbol in enumerate(all_symbols[:20]):
        print(f"  {i+1:2d}. {symbol}")
    
    if len(all_symbols) > 20:
        print(f"   ... and {len(all_symbols) - 20} more stocks")
    
    return all_symbols

if __name__ == "__main__":
    symbols = fetch_all_nse_stocks()
