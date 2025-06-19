
#!/usr/bin/env python3
"""
Fix corrupted NSE symbols file by removing invalid entries
"""

import re
import os
from nse_symbols_fetcher import NSESymbolsFetcher

def clean_symbols_file(input_file="nse_complete_universe.txt", output_file="nse_complete_universe_clean.txt"):
    """Clean corrupted symbols file"""
    print("🔧 Cleaning corrupted symbols file...")
    
    valid_symbols = []
    invalid_symbols = []
    
    if not os.path.exists(input_file):
        print(f"❌ File {input_file} not found")
        return False
    
    with open(input_file, 'r') as f:
        lines = f.readlines()
    
    print(f"📄 Processing {len(lines)} lines from {input_file}")
    
    for line_num, line in enumerate(lines, 1):
        symbol = line.strip()
        
        if not symbol:
            continue
            
        # Ensure .NS suffix
        if not symbol.endswith('.NS'):
            symbol = f"{symbol}.NS"
        
        # Get base symbol (without .NS)
        base_symbol = symbol.replace('.NS', '')
        
        # Validate symbol
        is_valid = True
        
        # Check length
        if len(base_symbol) > 20 or len(base_symbol) < 1:
            is_valid = False
        
        # Check for HTML/JavaScript fragments
        invalid_chars = ['<', '>', '"', "'", '(', ')', '{', '}', '[', ']', '=', '!', '/']
        if any(char in base_symbol for char in invalid_chars):
            is_valid = False
        
        # Check if it contains only valid characters
        if not re.match(r'^[A-Z0-9&\-\.]+$', base_symbol):
            is_valid = False
        
        if is_valid:
            valid_symbols.append(symbol)
        else:
            invalid_symbols.append(symbol)
            print(f"❌ Line {line_num}: Invalid symbol '{symbol[:50]}{'...' if len(symbol) > 50 else ''}'")
    
    # Remove duplicates and sort
    valid_symbols = sorted(list(set(valid_symbols)))
    
    print(f"✅ Found {len(valid_symbols)} valid symbols")
    print(f"❌ Found {len(invalid_symbols)} invalid symbols")
    
    # Write clean symbols to new file
    with open(output_file, 'w') as f:
        for symbol in valid_symbols:
            f.write(f"{symbol}\n")
    
    print(f"💾 Clean symbols saved to {output_file}")
    
    # Show first few valid symbols
    print("\n📋 First 10 valid symbols:")
    for symbol in valid_symbols[:10]:
        print(f"  ✓ {symbol}")
    
    # Show first few invalid symbols
    if invalid_symbols:
        print(f"\n❌ First 5 invalid symbols:")
        for symbol in invalid_symbols[:5]:
            display_symbol = symbol[:50] + '...' if len(symbol) > 50 else symbol
            print(f"  ✗ {display_symbol}")
    
    return True

def regenerate_clean_symbols():
    """Regenerate symbols from scratch using NSE fetcher"""
    print("🔄 Regenerating clean symbols from NSE sources...")
    
    try:
        fetcher = NSESymbolsFetcher()
        symbols = fetcher.get_all_nse_symbols(complete_universe=True)
        
        # Additional cleaning
        clean_symbols = []
        for symbol in symbols:
            if symbol and len(symbol.replace('.NS', '')) <= 20:
                base = symbol.replace('.NS', '')
                if re.match(r'^[A-Z0-9&\-\.]+$', base):
                    clean_symbols.append(symbol)
        
        # Save to file
        output_file = "nse_complete_universe_regenerated.txt"
        with open(output_file, 'w') as f:
            for symbol in sorted(set(clean_symbols)):
                f.write(f"{symbol}\n")
        
        print(f"✅ Regenerated {len(clean_symbols)} clean symbols to {output_file}")
        return True
        
    except Exception as e:
        print(f"❌ Error regenerating symbols: {e}")
        return False

if __name__ == "__main__":
    print("🧹 NSE Symbols File Cleaner")
    print("=" * 40)
    
    # First try to clean existing file
    if clean_symbols_file():
        print("\n✅ Symbol file cleaned successfully!")
        print("💡 You can now replace nse_complete_universe.txt with nse_complete_universe_clean.txt")
    
    print("\n" + "=" * 40)
    
    # Also regenerate from scratch as backup
    if regenerate_clean_symbols():
        print("\n✅ Fresh symbols regenerated successfully!")
    
    print("\n📋 NEXT STEPS:")
    print("1. Replace the corrupted file:")
    print("   mv nse_complete_universe_clean.txt nse_complete_universe.txt")
    print("2. Restart the download:")
    print("   python main_data_loader.py --download-nse")
