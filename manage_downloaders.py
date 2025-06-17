
#!/usr/bin/env python3
"""
Process Management Script for NSE Downloaders
"""

import subprocess
import os
import sys
import time
import logging

class ProcessManager:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def stop_all_downloaders(self):
        """Stop all running downloader processes"""
        print("🛑 Stopping all downloader processes...")
        
        try:
            # Find running downloader processes
            result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
            lines = result.stdout.split('\n')
            
            downloader_pids = []
            for line in lines:
                if 'yfinance_nse_downloader' in line or 'main_data_loader' in line:
                    parts = line.split()
                    if len(parts) > 1:
                        pid = parts[1]
                        downloader_pids.append(pid)
                        print(f"Found downloader process: PID {pid}")
            
            # Kill the processes
            for pid in downloader_pids:
                try:
                    subprocess.run(['kill', pid], check=True)
                    print(f"✅ Stopped process {pid}")
                except subprocess.CalledProcessError:
                    print(f"❌ Failed to stop process {pid}")
            
            if not downloader_pids:
                print("ℹ️  No downloader processes found running")
            
            time.sleep(2)  # Give processes time to stop
            
        except Exception as e:
            print(f"❌ Error stopping processes: {e}")
    
    def start_complete_universe_downloader(self):
        """Start the complete universe downloader"""
        print("🚀 Starting complete universe downloader...")
        
        # Ensure we have the complete universe file
        if not os.path.exists('nse_complete_universe.txt'):
            print("📋 Generating complete universe file...")
            subprocess.run([sys.executable, 'fetch_complete_nse_list.py'])
        
        # Start the downloader
        try:
            print("▶️  Starting yfinance_nse_downloader.py...")
            # Run in background
            subprocess.Popen([sys.executable, 'yfinance_nse_downloader.py'])
            print("✅ Downloader started successfully")
            print("📊 Monitor progress with: python comprehensive_status_checker.py")
            print("📝 View logs with: tail -f yfinance_nse_downloader.log")
            
        except Exception as e:
            print(f"❌ Error starting downloader: {e}")
    
    def restart_downloaders(self):
        """Restart all downloaders with correct configuration"""
        print("🔄 Restarting downloaders with complete universe...")
        self.stop_all_downloaders()
        time.sleep(3)
        self.start_complete_universe_downloader()
    
    def status_check(self):
        """Check current status"""
        print("📊 Current system status:")
        subprocess.run([sys.executable, 'comprehensive_status_checker.py'])

def main():
    manager = ProcessManager()
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python manage_downloaders.py stop          # Stop all downloaders")
        print("  python manage_downloaders.py start         # Start complete universe downloader")
        print("  python manage_downloaders.py restart       # Restart with correct config")
        print("  python manage_downloaders.py status        # Check status")
        return
    
    command = sys.argv[1].lower()
    
    if command == "stop":
        manager.stop_all_downloaders()
    elif command == "start":
        manager.start_complete_universe_downloader()
    elif command == "restart":
        manager.restart_downloaders()
    elif command == "status":
        manager.status_check()
    else:
        print(f"Unknown command: {command}")

if __name__ == "__main__":
    main()
