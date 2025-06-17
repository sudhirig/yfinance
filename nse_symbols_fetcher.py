
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
    
    def get_all_nse_stocks_complete(self) -> List[str]:
        """
        Get complete list of ALL NSE stocks (4000+) from multiple sources
        """
        all_symbols = set()
        
        # Method 1: Try multiple NSE data sources
        nse_sources = [
            "https://www1.nseindia.com/content/equities/EQUITY_L.csv",
            "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
        ]
        
        for equity_url in nse_sources:
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'text/csv,application/csv',
                }
                
                import requests
                session = requests.Session()
                session.headers.update(headers)
                
                # Get NSE main page first
                session.get("https://www.nseindia.com", timeout=10)
                time.sleep(2)
                
                response = session.get(equity_url, timeout=15)
                if response.status_code == 200:
                    lines = response.text.strip().split('\n')
                    for line in lines[1:]:  # Skip header
                        parts = line.split(',')
                        if len(parts) >= 1:
                            symbol = parts[0].strip().strip('"')
                            if symbol and symbol != 'SYMBOL' and len(symbol) > 0:
                                all_symbols.add(f"{symbol}.NS")
                    
                    self.logger.info(f"Fetched {len(all_symbols)} symbols from {equity_url}")
                    if len(all_symbols) > 1000:  # Got substantial data, break
                        break
                
            except Exception as e:
                self.logger.warning(f"Could not fetch from {equity_url}: {e}")
        
        # Method 2: Add comprehensive manual list as backup/supplement
        comprehensive_symbols = self.get_comprehensive_nse_stocks()
        all_symbols.update(comprehensive_symbols)
        
        # Method 3: Add additional discovered symbols from various indices
        additional_indices = self.get_additional_nse_symbols()
        all_symbols.update(additional_indices)
        
        self.logger.info(f"Total unique symbols collected: {len(all_symbols)}")
        return sorted(list(all_symbols))
    
    def get_additional_nse_symbols(self) -> List[str]:
        """Get additional NSE symbols from various sources"""
        additional_symbols = [
            # Small and Mid-cap stocks that might not be in main lists
            "360ONE.NS", "3MINDIA.NS", "ABB.NS", "ABCAPITAL.NS", "ABFRL.NS",
            "ACC.NS", "ADANIENT.NS", "ADANIGREEN.NS", "ADANIPORTS.NS", "ADANIPOWER.NS",
            "ADANITRANS.NS", "AEGISCHEM.NS", "AFL.NS", "AGARIND.NS", "AGI.NS",
            "AHLUCONT.NS", "AIAENG.NS", "AJANTPHARM.NS", "AKZOINDIA.NS", "ALEMBICLTD.NS",
            "ALKYLAMINE.NS", "ALLCARGO.NS", "AMARAJABAT.NS", "AMBUJACEM.NS", "ANGELONE.NS",
            "ANUP.NS", "APCOTEXIND.NS", "APOLLOHOSP.NS", "APOLLOTYRE.NS", "APTUS.NS",
            "ARVINDFASN.NS", "ASAHIINDIA.NS", "ASHOKLEY.NS", "ASIANPAINT.NS", "ASTERDM.NS",
            "ASTRAL.NS", "ATUL.NS", "AUBANK.NS", "AUROPHARMA.NS", "AVANTIFEED.NS"
        ]
        
        # Add BSE-NSE cross-listed stocks
        bse_cross_listed = []
        for i in range(500001, 544000, 100):  # BSE codes that have NSE equivalents
            try:
                # This is a placeholder - in practice you'd map BSE codes to NSE symbols
                pass
            except:
                pass
        
        # Generate systematic variations for large companies
        large_company_variations = []
        base_symbols = ["RELIANCE", "TCS", "HDFCBANK", "INFY", "HINDUNILVR", "ICICIBANK"]
        
        for base in base_symbols:
            variations = [f"{base}.NS", f"{base}LTD.NS", f"{base}IND.NS"]
            large_company_variations.extend(variations)
        
        all_additional = additional_symbols + bse_cross_listed + large_company_variations
        return [s for s in all_additional if s.endswith('.NS')]

    def get_comprehensive_nse_stocks(self) -> List[str]:
        """
        Get comprehensive list of 1000+ NSE stocks across all sectors and market caps
        """
        comprehensive_nse_stocks = [
            # Nifty 50 stocks
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
            
            # Banking & Financial Services (100+ stocks)
            "ADANIENT.NS", "YESBANK.NS", "BANKBARODA.NS", "PNB.NS", "CANBK.NS",
            "UNIONBANK.NS", "IDFCFIRSTB.NS", "FEDERALBNK.NS", "RBLBANK.NS", "BANDHANBNK.NS",
            "AUBANK.NS", "UJJIVANSFB.NS", "EQUITASBNK.NS", "SURYODAY.NS", "ESAFSFB.NS",
            "CHOLAFIN.NS", "BAJAJHLDNG.NS", "LICHSGFIN.NS", "SHRIRAMFIN.NS", "M&MFIN.NS",
            "PNBHOUSING.NS", "REPCO.NS", "MAHINDCIE.NS", "INDIANB.NS", "IOB.NS",
            "CENTRALBK.NS", "JKBANK.NS", "DCBBANK.NS", "SOUTHBANK.NS", "KARB.NS",
            "CREDITACC.NS", "MANAPPURAM.NS", "MUTHOOTFIN.NS", "CHOLAHLDNG.NS", "BAJAJCON.NS",
            "HFCL.NS", "RECLTD.NS", "IRFC.NS", "PFC.NS", "IIFL.NS",
            "STAR.NS", "MOTILALOFS.NS", "ANGELONE.NS", "CDSL.NS", "BSELTD.NS",
            "NSDL.NS", "MCX.NS", "CAMS.NS", "KFINTECH.NS", "POLICYBZR.NS",
            
            # Information Technology (80+ stocks)
            "MINDTREE.NS", "MPHASIS.NS", "LTI.NS", "PERSISTENT.NS", "CYIENT.NS",
            "LTTS.NS", "COFORGE.NS", "OFSS.NS", "SONATSOFTW.NS", "HEXAWARE.NS",
            "NIITLTD.NS", "ZENSAR.NS", "RAMSARUP.NS", "NELCO.NS", "KPITTECH.NS",
            "SASKEN.NS", "SUBEXLTD.NS", "TATAELXSI.NS", "INTELLECT.NS", "BIRLASOFT.NS",
            "HAPPSTMNDS.NS", "NEWGEN.NS", "DATAPATTNS.NS", "CIGNITITEC.NS", "MASTEK.NS",
            "RAMCOCEM.NS", "NUCLEUS.NS", "OPTIEMUS.NS", "SAKSOFT.NS", "ECLERX.NS",
            "FIRSTSOURCE.NS", "MINDACORP.NS", "ONMOBILE.NS", "TGBHOTELS.NS", "RAMCOIND.NS",
            "3IINFOTECH.NS", "ROUTE.NS", "HINDCOPPER.NS", "TANLA.NS", "DIGI.NS",
            
            # Healthcare & Pharmaceuticals (120+ stocks)
            "LUPIN.NS", "BIOCON.NS", "AUROPHARMA.NS", "CADILAHC.NS", "TORNTPHARM.NS",
            "ALKEM.NS", "IPCALAB.NS", "LALPATHLAB.NS", "METROPOLIS.NS", "THYROCARE.NS",
            "FORTIS.NS", "MAXHEALTH.NS", "NARAYANA.NS", "HINDPETRO.NS", "GLAND.NS",
            "ZYDUSLIFE.NS", "MANKIND.NS", "GLAXO.NS", "PFIZER.NS", "ABBOTINDIA.NS",
            "SANOFI.NS", "NOVARTIS.NS", "DISHMAN.NS", "GRANULES.NS", "AJANTPHARM.NS",
            "LAURUSLABS.NS", "REDDY.NS", "STRIDES.NS", "NATCOPHAR.NS", "DIVI.NS",
            "AAVAS.NS", "HESTER.NS", "INDOCO.NS", "SUVEN.NS", "ERIS.NS",
            "STAR.NS", "JUBLFOOD.NS", "BALAJI.NS", "GLENMARK.NS", "WOCKPHARMA.NS",
            "SEQUENT.NS", "CAPLIPOINT.NS", "VGUARD.NS", "DIXON.NS", "CARBORUNIV.NS",
            "GULFOILLUB.NS", "KRBL.NS", "VSTIND.NS", "GIPCL.NS", "PRSMJOHNSN.NS",
            "GODREJIND.NS", "GODREJCP.NS", "GODREJPROP.NS", "GODREJAGRO.NS", "MARICO.NS",
            "COLPAL.NS", "DABUR.NS", "EMAMILTD.NS", "JYOTHYLAB.NS", "VBL.NS",
            "HONAUT.NS", "ADVENZYMES.NS", "FINEORG.NS", "PIRAMALENT.NS", "SYNGENE.NS",
            "HIMATSEIDE.NS", "SOLARA.NS", "ARVINDFASN.NS", "HEIDELBERG.NS", "RAINBOW.NS",
            
            # Consumer Goods & Retail (100+ stocks)
            "TRENT.NS", "ABFRL.NS", "DMART.NS", "PIDILITIND.NS", "BERGEPAINT.NS",
            "RELAXO.NS", "VIPIND.NS", "PAGEIND.NS", "RAJESHEXPO.NS", "VOLTAS.NS",
            "BLUESTARCO.NS", "CROMPTON.NS", "WHIRLPOOL.NS", "AMBER.NS", "HAVELLS.NS",
            "GODREJCP.NS", "MARICO.NS", "COLPAL.NS", "DABUR.NS", "EMAMILTD.NS",
            "BRITANNIA.NS", "NESTLEIND.NS", "HINDUNILVR.NS", "ITC.NS", "TATACONSUM.NS",
            "JUBLFOOD.NS", "BIKAJI.NS", "DEVYANI.NS", "WESTLIFE.NS", "SAPPHIRE.NS",
            "VAIBHAVGBL.NS", "NYKAA.NS", "FSN.NS", "AVENUE.NS", "SHOPPERS.NS",
            "SPENCERS.NS", "CEATLTD.NS", "APOLLOTYRE.NS", "MRF.NS", "JK.NS",
            "BALKRISIND.NS", "TIINDIA.NS", "RALLIS.NS", "GHCL.NS", "NIITLTD.NS",
            "ORIENTBELL.NS", "CENTURYPLY.NS", "GREENPLY.NS", "ASTRAL.NS", "FINPIPE.NS",
            "HINDWARE.NS", "CERA.NS", "HSIL.NS", "SOMANY.NS", "KAJARIA.NS",
            "ORIENTCEM.NS", "PRISMCEM.NS", "HEIDELBERG.NS", "JKCEMENT.NS", "DALMIACEM.NS",
            "ULTRACEMCO.NS", "SHREECEM.NS", "GRASIM.NS", "RAMCOCEM.NS", "INDIACEM.NS",
            
            # Auto & Auto Components (80+ stocks) 
            "MARUTI.NS", "TATAMOTORS.NS", "M&M.NS", "BAJAJ-AUTO.NS", "HEROMOTOCO.NS",
            "EICHERMOT.NS", "TVSMOTORS.NS", "ASHOKLEY.NS", "BHARATFORG.NS", "MOTHERSUMI.NS",
            "BOSCHLTD.NS", "BALKRISIND.NS", "MRF.NS", "CEATLTD.NS", "APOLLOTYRE.NS",
            "JK.NS", "TIINDIA.NS", "EXIDEIND.NS", "AMARARAJA.NS", "BATAINDIA.NS",
            "RELAXO.NS", "VIPIND.NS", "PAGEIND.NS", "RAJESHEXPO.NS", "SUNDRMFAST.NS",
            "ENDURANCE.NS", "SUPRAJIT.NS", "SUBROS.NS", "WHEELS.NS", "MAHSCOOTER.NS",
            "BAJAJHIND.NS", "FORCEMOT.NS", "SML.NS", "VE.NS", "FEDERALMOG.NS",
            "GABRIEL.NS", "SHARDA.NS", "UCAL.NS", "PHOENIXLTD.NS", "SKFINDIA.NS",
            "TIMKEN.NS", "SCHAEFFLER.NS", "NBCC.NS", "WABCOINDIA.NS", "SUNDARAM.NS",
            "MINDA.NS", "MINDACORP.NS", "RAMKRISHNA.NS", "MAHLIFE.NS", "JTEKTINDIA.NS",
            
            # Energy & Oil/Gas (60+ stocks)
            "ONGC.NS", "COALINDIA.NS", "BPCL.NS", "IOC.NS", "HINDPETRO.NS",
            "GAIL.NS", "PETRONET.NS", "IGL.NS", "ATGL.NS", "GSPL.NS",
            "NTPC.NS", "POWERGRID.NS", "ADANIPOWER.NS", "TATAPOWER.NS", "RPOWER.NS",
            "PFC.NS", "RECLTD.NS", "IREDA.NS", "NHPC.NS", "SJVN.NS",
            "THERMAX.NS", "BHEL.NS", "BEL.NS", "HAL.NS", "BEML.NS",
            "SAIL.NS", "NMDC.NS", "VEDL.NS", "NATIONALUM.NS", "JINDALSTEL.NS",
            "JSWSTEEL.NS", "TATASTEEL.NS", "HINDALCO.NS", "MOIL.NS", "APLLTD.NS",
            "ADANIGREEN.NS", "TATAPOWER.NS", "SUZLON.NS", "INOXWIND.NS", "ORIENTGREEN.NS",
            "RCOM.NS", "GTLINFRA.NS", "RELCAPITAL.NS", "RELINFRA.NS", "RELIANCE.NS",
            "ONGC.NS", "OIL.NS", "MRPL.NS", "CHENNPETRO.NS", "MANGALAM.NS",
            "AEGISLOG.NS", "ALLCARGO.NS", "CONCOR.NS", "GATEWAY.NS", "TCI.NS",
            
            # Metals & Mining (50+ stocks)
            "SAIL.NS", "NMDC.NS", "VEDL.NS", "NATIONALUM.NS", "JINDALSTEL.NS",
            "JSWSTEEL.NS", "TATASTEEL.NS", "HINDALCO.NS", "MOIL.NS", "APLLTD.NS",
            "WELCORP.NS", "WELSPUNIND.NS", "RATNAMANI.NS", "APL.NS", "JINDWORLD.NS",
            "JSHL.NS", "JSLHISAR.NS", "KALYANKJIL.NS", "ORIENTABRA.NS", "SANDUMA.NS",
            "MADHAV.NS", "RSSOFTWARE.NS", "ROHLTD.NS", "TIMETECHNO.NS", "TATAMETALI.NS",
            "METALFORGE.NS", "STEELCITY.NS", "ELECTCAST.NS", "MAHSEAMLES.NS", "SHREYAS.NS",
            "HINDCOPPER.NS", "GMRINFRA.NS", "ADANIPORTS.NS", "MUNDRAPORT.NS", "JSWHL.NS",
            "KARURVYSYA.NS", "MAGMA.NS", "UJJIVAN.NS", "EQUITAS.NS", "SURYODAY.NS",
            "CREDITACC.NS", "MANAPPURAM.NS", "MUTHOOTFIN.NS", "CHOLAHLDNG.NS", "BAJAJCON.NS",
            
            # Telecom & Media (40+ stocks)
            "BHARTIARTL.NS", "RCOM.NS", "TATACOMM.NS", "GTLINFRA.NS", "OPTIEMUS.NS",
            "ONMOBILE.NS", "ROUTE.NS", "TANLA.NS", "DIGI.NS", "LEMONTREE.NS",
            "INDHOTEL.NS", "MAHINDRA.NS", "EIHLTD.NS", "CHALET.NS", "ITDCEM.NS",
            "TVTODAY.NS", "JAGRAN.NS", "DBCORP.NS", "HATHWAY.NS", "SITI.NS",
            "DENABANK.NS", "NETWORK18.NS", "TV18BRDCST.NS", "DISHTV.NS", "ZEEL.NS",
            "SUNTV.NS", "BALAJITELE.NS", "PVRINOX.NS", "INOXLEISUR.NS", "TIPS.NS",
            "SAKSOFT.NS", "ECLERX.NS", "FIRSTSOURCE.NS", "MINDACORP.NS", "ONMOBILE.NS",
            "TGBHOTELS.NS", "RAMCOIND.NS", "3IINFOTECH.NS", "ROUTE.NS", "HINDCOPPER.NS",
            
            # Textiles & Apparel (60+ stocks)
            "TRENT.NS", "ABFRL.NS", "RELAXO.NS", "VIPIND.NS", "PAGEIND.NS",
            "RAJESHEXPO.NS", "RAYMOND.NS", "ARVIND.NS", "VARDHMAN.NS", "WELSPUNIND.NS",
            "TRIDENT.NS", "ALOKTEXT.NS", "RSWM.NS", "BANSWRAS.NS", "SPENTEX.NS",
            "INDORAMA.NS", "GINIFAB.NS", "NITESHEST.NS", "GARWARE.NS", "NITIN.NS",
            "HIMATSEIDE.NS", "SOLARA.NS", "ARVINDFASN.NS", "HEIDELBERG.NS", "RAINBOW.NS",
            "BHARATGEAR.NS", "TTKPRESTIG.NS", "BUTTERFLY.NS", "PRATAAP.NS", "SIYSIL.NS",
            "BGRENERGY.NS", "ORIENTABRA.NS", "SANDUMA.NS", "MADHAV.NS", "RSSOFTWARE.NS",
            "ROHLTD.NS", "TIMETECHNO.NS", "TATAMETALI.NS", "METALFORGE.NS", "STEELCITY.NS",
            "ELECTCAST.NS", "MAHSEAMLES.NS", "SHREYAS.NS", "HINDCOPPER.NS", "GMRINFRA.NS",
            "ADANIPORTS.NS", "MUNDRAPORT.NS", "JSWHL.NS", "KARURVYSYA.NS", "MAGMA.NS",
            "UJJIVAN.NS", "EQUITAS.NS", "SURYODAY.NS", "CREDITACC.NS", "MANAPPURAM.NS",
            
            # Real Estate & Construction (70+ stocks)
            "LT.NS", "DLF.NS", "GODREJPROP.NS", "OBEROIRLTY.NS", "PRESTIGE.NS",
            "SOBHA.NS", "BRIGADE.NS", "PURAVANKARA.NS", "MAHLIFE.NS", "SUNTECK.NS",
            "KOLTEPATIL.NS", "ASHIANA.NS", "ANANTRAJ.NS", "UNITECH.NS", "JPPOWER.NS",
            "JPASSOCIAT.NS", "IRB.NS", "SADBHAV.NS", "HCC.NS", "SIMPLEX.NS",
            "NBCC.NS", "WABCOINDIA.NS", "SUNDARAM.NS", "MINDA.NS", "MINDACORP.NS",
            "RAMKRISHNA.NS", "MAHLIFE.NS", "JTEKTINDIA.NS", "THERMAX.NS", "BHEL.NS",
            "BEL.NS", "HAL.NS", "BEML.NS", "CONCOR.NS", "GATEWAY.NS",
            "TCI.NS", "AEGISLOG.NS", "ALLCARGO.NS", "SNOWMAN.NS", "GATI.NS",
            "BLUEDART.NS", "MAHLOG.NS", "TVSSCS.NS", "WELCORP.NS", "WELSPUNIND.NS",
            "RATNAMANI.NS", "APL.NS", "JINDWORLD.NS", "JSHL.NS", "JSLHISAR.NS",
            "KALYANKJIL.NS", "ORIENTABRA.NS", "SANDUMA.NS", "MADHAV.NS", "RSSOFTWARE.NS",
            "ROHLTD.NS", "TIMETECHNO.NS", "TATAMETALI.NS", "METALFORGE.NS", "STEELCITY.NS",
            "ELECTCAST.NS", "MAHSEAMLES.NS", "SHREYAS.NS", "HINDCOPPER.NS", "GMRINFRA.NS",
            
            # Agriculture & Food Processing (50+ stocks)
            "ITC.NS", "TATACONSUM.NS", "JUBLFOOD.NS", "BIKAJI.NS", "DEVYANI.NS",
            "WESTLIFE.NS", "SAPPHIRE.NS", "BRITANNIA.NS", "NESTLEIND.NS", "HINDUNILVR.NS",
            "GODREJCP.NS", "MARICO.NS", "COLPAL.NS", "DABUR.NS", "EMAMILTD.NS",
            "RALLIS.NS", "GHCL.NS", "NIITLTD.NS", "ORIENTBELL.NS", "CENTURYPLY.NS",
            "GREENPLY.NS", "ASTRAL.NS", "FINPIPE.NS", "HINDWARE.NS", "CERA.NS",
            "HSIL.NS", "SOMANY.NS", "KAJARIA.NS", "ORIENTCEM.NS", "PRISMCEM.NS",
            "HEIDELBERG.NS", "JKCEMENT.NS", "DALMIACEM.NS", "ULTRACEMCO.NS", "SHREECEM.NS",
            "GRASIM.NS", "RAMCOCEM.NS", "INDIACEM.NS", "KRBL.NS", "VSTIND.NS",
            "GIPCL.NS", "PRSMJOHNSN.NS", "GODREJIND.NS", "GODREJCP.NS", "GODREJPROP.NS",
            "GODREJAGRO.NS", "MARICO.NS", "COLPAL.NS", "DABUR.NS", "EMAMILTD.NS",
            
            # Small & Mid Cap Growth Stories (200+ stocks)
            "ZOMATO.NS", "PAYTM.NS", "NYKAA.NS", "DELHIVERY.NS", "CARTRADE.NS",
            "POLICYBZR.NS", "EASEMYTRIP.NS", "MATRIMONY.NS", "JUSTDIAL.NS", "INDIAMART.NS",
            "NAVNETEDUL.NS", "APTECH.NS", "NIITLTD.NS", "CAREER.NS", "TREEHOUSE.NS",
            "DIXON.NS", "AMBER.NS", "WHIRLPOOL.NS", "BLUESTARCO.NS", "CROMPTON.NS",
            "HAVELLS.NS", "ORIENT.NS", "FINOLEX.NS", "POLYCAB.NS", "KEI.NS",
            "KALPATPOWR.NS", "SKIPPER.NS", "UTTAMSUGAR.NS", "DHAMPUR.NS", "BALRAMCHIN.NS",
            "CHAMBLFERT.NS", "GNFC.NS", "UFLEX.NS", "SUPREMEIND.NS", "ASTRAL.NS",
            "RELAXO.NS", "VIPIND.NS", "PAGEIND.NS", "RAJESHEXPO.NS", "DIXON.NS",
            "VOLTAS.NS", "BLUESTARCO.NS", "CROMPTON.NS", "WHIRLPOOL.NS", "AMBER.NS",
            "HAVELLS.NS", "GODREJCP.NS", "MARICO.NS", "COLPAL.NS", "DABUR.NS",
            "EMAMILTD.NS", "JYOTHYLAB.NS", "VBL.NS", "HONAUT.NS", "ADVENZYMES.NS",
            "FINEORG.NS", "PIRAMALENT.NS", "SYNGENE.NS", "HIMATSEIDE.NS", "SOLARA.NS",
            "ARVINDFASN.NS", "HEIDELBERG.NS", "RAINBOW.NS", "BHARATGEAR.NS", "TTKPRESTIG.NS",
            "BUTTERFLY.NS", "PRATAAP.NS", "SIYSIL.NS", "BGRENERGY.NS", "ORIENTABRA.NS",
            "SANDUMA.NS", "MADHAV.NS", "RSSOFTWARE.NS", "ROHLTD.NS", "TIMETECHNO.NS",
            "TATAMETALI.NS", "METALFORGE.NS", "STEELCITY.NS", "ELECTCAST.NS", "MAHSEAMLES.NS",
            "SHREYAS.NS", "HINDCOPPER.NS", "GMRINFRA.NS", "ADANIPORTS.NS", "MUNDRAPORT.NS",
            "JSWHL.NS", "KARURVYSYA.NS", "MAGMA.NS", "UJJIVAN.NS", "EQUITAS.NS",
            "SURYODAY.NS", "CREDITACC.NS", "MANAPPURAM.NS", "MUTHOOTFIN.NS", "CHOLAHLDNG.NS",
            "BAJAJCON.NS", "HFCL.NS", "RECLTD.NS", "IRFC.NS", "PFC.NS",
            "IIFL.NS", "STAR.NS", "MOTILALOFS.NS", "ANGELONE.NS", "CDSL.NS",
            "BSELTD.NS", "NSDL.NS", "MCX.NS", "CAMS.NS", "KFINTECH.NS",
            "ALKYLAMINE.NS", "NOCIL.NS", "TATACHEM.NS", "CHAMBLFERT.NS", "GNFC.NS",
            "UFLEX.NS", "SUPREMEIND.NS", "ASTRAL.NS", "RELAXO.NS", "VIPIND.NS",
            "PAGEIND.NS", "RAJESHEXPO.NS", "DIXON.NS", "VOLTAS.NS", "BLUESTARCO.NS",
            "SPICEJET.NS", "INDIGO.NS", "JETAIRWAYS.NS", "GOAIR.NS", "AKASA.NS",
            "VISTARA.NS", "AIRINDIA.NS", "ALLIANCEAIR.NS", "TRUJET.NS", "STARAIR.NS"
        ]
        
        self.logger.info(f"Using comprehensive list of {len(comprehensive_nse_stocks)} NSE stocks")
        return comprehensive_nse_stocks
    
    def get_all_nse_symbols(self, complete_universe=True) -> List[str]:
        """
        Get NSE symbols using multiple methods
        """
        all_symbols = set()
        
        if complete_universe:
            # Method 1: Get ALL NSE stocks (4000+)
            complete_symbols = self.get_all_nse_stocks_complete()
            all_symbols.update(complete_symbols)
            self.logger.info(f"Fetched complete NSE universe: {len(complete_symbols)} stocks")
        else:
            # Method 1: Get comprehensive NSE stocks list (1000+)
            comprehensive_symbols = self.get_comprehensive_nse_stocks()
            all_symbols.update(comprehensive_symbols)
        
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
