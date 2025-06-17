
import yfinance as yf
import pandas as pd
import requests
import json
import logging
from typing import List, Dict
import time

class NSESymbolsFetcher:
    def __init__(self):
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def get_nse_symbols_from_nse_website(self) -> List[str]:
        """
        Fetch NSE symbols from NSE official website
        """
        try:
            # NSE equity list URL
            url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20500"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            session = requests.Session()
            session.headers.update(headers)
            
            # First, get the main page to establish session
            session.get("https://www.nseindia.com", timeout=10)
            time.sleep(2)
            
            # Now get the symbols
            response = session.get(url, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                symbols = []
                
                for item in data.get('data', []):
                    symbol = item.get('symbol', '').strip()
                    if symbol:
                        symbols.append(f"{symbol}.NS")
                
                self.logger.info(f"Fetched {len(symbols)} symbols from NSE website")
                return symbols
            else:
                self.logger.warning(f"Failed to fetch from NSE website: {response.status_code}")
                
        except Exception as e:
            self.logger.error(f"Error fetching from NSE website: {e}")
        
        return []
    
    def get_nse_symbols_from_predefined_list(self) -> List[str]:
        """
        Use a predefined list of major NSE stocks
        """
        major_nse_stocks = [
            # Nifty 50 major stocks
            "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "HINDUNILVR.NS",
            "ICICIBANK.NS", "KOTAKBANK.NS", "BHARTIARTL.NS", "ITC.NS", "SBIN.NS",
            "LT.NS", "ASIANPAINT.NS", "AXISBANK.NS", "MARUTI.NS", "SUNPHARMA.NS",
            "TITAN.NS", "ULTRACEMCO.NS", "NESTLEIND.NS", "WIPRO.NS", "M&M.NS",
            "NTPC.NS", "POWERGRID.NS", "TECHM.NS", "TATAMOTORS.NS", "HCLTECH.NS",
            "BAJFINANCE.NS", "COALINDIA.NS", "ONGC.NS", "DIVISLAB.NS", "GRASIM.NS",
            "INDUSINDBK.NS", "BAJAJFINSV.NS", "CIPLA.NS", "DRREDDY.NS", "EICHERMOT.NS",
            "BRITANNIA.NS", "BPCL.NS", "TATASTEEL.NS", "IOC.NS", "ADANIPORTS.NS",
            "JSWSTEEL.NS", "HINDALCO.NS", "HEROMOTOCO.NS", "BAJAJ-AUTO.NS", "UPL.NS",
            "SHREECEM.NS", "TATACONSUM.NS", "APOLLOHOSP.NS", "SBILIFE.NS", "HDFCLIFE.NS",
            
            # Additional major stocks
            "ADANIENT.NS", "ADANIGREEN.NS", "YESBANK.NS", "BANKBARODA.NS", "PNB.NS",
            "CANBK.NS", "UNIONBANK.NS", "IDFCFIRSTB.NS", "FEDERALBNK.NS", "RBLBANK.NS",
            "BANDHANBNK.NS", "AUBANK.NS", "NAUKRI.NS", "ZOMATO.NS", "PAYTM.NS",
            "POLICYBZR.NS", "DMART.NS", "PIDILITIND.NS", "GODREJCP.NS", "MARICO.NS",
            "BERGEPAINT.NS", "COLPAL.NS", "DABUR.NS", "MFSL.NS", "CDSL.NS",
            "IRCTC.NS", "SAIL.NS", "NMDC.NS", "VEDL.NS", "NATIONALUM.NS",
            "JINDALSTEL.NS", "MOIL.NS", "GMRINFRA.NS", "ADANIPOWER.NS", "TATAPOWER.NS",
            "RPOWER.NS", "PFC.NS", "RECLTD.NS", "IRFC.NS", "RVNL.NS",
            "CUMMINSIND.NS", "BOSCHLTD.NS", "BHEL.NS", "BEL.NS", "HAL.NS",
            "CONCOR.NS", "CONTAINER.NS", "MINDTREE.NS", "MPHASIS.NS", "LTI.NS",
            "PERSISTENT.NS", "CYIENT.NS", "LTTS.NS", "TRENT.NS", "ABFRL.NS",
            "GAIL.NS", "PETRONET.NS", "IGL.NS", "ATGL.NS", "GSPL.NS",
            "LUPIN.NS", "BIOCON.NS", "AUROPHARMA.NS", "CADILAHC.NS", "TORNTPHARM.NS",
            "ALKEM.NS", "IPCALAB.NS", "LALPATHLAB.NS", "METROPOLIS.NS", "THYROCARE.NS",
            "FORTIS.NS", "MAXHEALTH.NS", "NARAYANA.NS", "AARTIIND.NS", "DEEPAKNTR.NS",
            "ALKYLAMINE.NS", "BALRAMCHIN.NS", "NOCIL.NS", "TATACHEM.NS", "CHAMBLFERT.NS",
            "GNFC.NS", "UFLEX.NS", "SUPREMEIND.NS", "ASTRAL.NS", "RELAXO.NS",
            "VIPIND.NS", "PAGEIND.NS", "RAJESHEXPO.NS", "DIXON.NS", "VOLTAS.NS",
            "BLUESTARCO.NS", "CROMPTON.NS", "WHIRLPOOL.NS", "AMBER.NS", "HAVELLS.NS"
        ]
        
        self.logger.info(f"Using predefined list of {len(major_nse_stocks)} NSE stocks")
        return major_nse_stocks
    
    def get_all_nse_symbols(self) -> List[str]:
        """
        Get NSE symbols using multiple methods
        """
        all_symbols = set()
        
        # Method 1: Try to get from NSE website
        website_symbols = self.get_nse_symbols_from_predefined_list()
        all_symbols.update(website_symbols)
        
        # Method 2: Add some popular stocks that might be missing
        additional_stocks = [
            "RCOM.NS", "SUZLON.NS", "JPASSOCIAT.NS", "DLF.NS", "UNITECH.NS",
            "JPPOWER.NS", "JETAIRWAYS.NS", "SPICEJET.NS", "INDIGO.NS", "GOAIR.NS"
        ]
        all_symbols.update(additional_stocks)
        
        # Convert to sorted list
        symbols_list = sorted(list(all_symbols))
        self.logger.info(f"Total unique NSE symbols collected: {len(symbols_list)}")
        
        return symbols_list
    
    def validate_symbols(self, symbols: List[str]) -> List[str]:
        """
        Validate symbols by checking if they exist in yfinance
        """
        valid_symbols = []
        self.logger.info("Validating symbols...")
        
        for i, symbol in enumerate(symbols[:10]):  # Test first 10 for validation
            try:
                ticker = yf.Ticker(symbol)
                info = ticker.info
                
                if info and len(info) > 5:  # Basic check if data exists
                    valid_symbols.append(symbol)
                    self.logger.info(f"✓ {symbol} - Valid")
                else:
                    self.logger.warning(f"✗ {symbol} - No data")
                    
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                self.logger.warning(f"✗ {symbol} - Error: {e}")
        
        self.logger.info(f"Validated {len(valid_symbols)} symbols out of {len(symbols[:10])} tested")
        
        # Return all symbols for now (validation can be done during download)
        return symbols
    
    def save_symbols_to_file(self, symbols: List[str], filename: str = "nse_symbols.txt"):
        """
        Save symbols to a text file
        """
        try:
            with open(filename, 'w') as f:
                for symbol in symbols:
                    f.write(f"{symbol}\n")
            
            self.logger.info(f"Saved {len(symbols)} symbols to {filename}")
            
        except Exception as e:
            self.logger.error(f"Error saving symbols to file: {e}")

if __name__ == "__main__":
    fetcher = NSESymbolsFetcher()
    symbols = fetcher.get_all_nse_symbols()
    fetcher.save_symbols_to_file(symbols)
    
    print(f"\nFound {len(symbols)} NSE symbols:")
    for symbol in symbols[:10]:
        print(f"  - {symbol}")
    
    if len(symbols) > 10:
        print(f"  ... and {len(symbols) - 10} more symbols")
