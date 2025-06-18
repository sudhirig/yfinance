
#!/usr/bin/env python3
"""
Real-time progress monitor for NSE data download
"""

import psycopg2
import os
import time
from datetime import datetime
from database_config import get_database_config

class ProgressMonitor:
    def __init__(self):
        self.db_config = get_database_config()
        
    def get_current_stats(self):
        """Get current database statistics"""
        try:
            database_url = os.getenv('DATABASE_URL')
            if database_url:
                conn = psycopg2.connect(database_url)
            else:
                conn = psycopg2.connect(**self.db_config)
            
            cursor = conn.cursor()
            
            # Get company counts
            cursor.execute("SELECT COUNT(*) FROM companies")
            total_companies = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT company_id) FROM price_history")
            with_price_data = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT company_id) FROM company_metrics")
            with_metrics = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT company_id) FROM income_statements")
            with_financials = cursor.fetchone()[0]
            
            # Get latest update
            cursor.execute("""
                SELECT MAX(update_timestamp) FROM data_updates 
                WHERE update_timestamp >= NOW() - INTERVAL '10 minutes'
            """)
            latest_update = cursor.fetchone()[0]
            
            cursor.close()
            conn.close()
            
            return {
                'total_companies': total_companies,
                'with_price_data': with_price_data,
                'with_metrics': with_metrics,
                'with_financials': with_financials,
                'latest_update': latest_update,
                'timestamp': datetime.now()
            }
            
        except Exception as e:
            print(f"Error getting stats: {e}")
            return None
    
    def monitor_progress(self, duration_minutes=30, check_interval=60):
        """Monitor progress for specified duration"""
        print(f"🔍 Monitoring NSE download progress for {duration_minutes} minutes...")
        print(f"📊 Checking every {check_interval} seconds")
        print("=" * 60)
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        previous_stats = None
        
        while time.time() < end_time:
            current_stats = self.get_current_stats()
            
            if current_stats:
                print(f"\n⏰ {current_stats['timestamp'].strftime('%H:%M:%S')}")
                print(f"📈 Total Companies: {current_stats['total_companies']}")
                print(f"💹 With Price Data: {current_stats['with_price_data']}")
                print(f"📊 With Metrics: {current_stats['with_metrics']}")
                print(f"📋 With Financials: {current_stats['with_financials']}")
                
                if current_stats['latest_update']:
                    print(f"🔄 Latest Update: {current_stats['latest_update']}")
                else:
                    print("⚠️ No recent updates (last 10 minutes)")
                
                # Show progress if we have previous stats
                if previous_stats:
                    company_increase = current_stats['total_companies'] - previous_stats['total_companies']
                    if company_increase > 0:
                        print(f"📈 Progress: +{company_increase} companies since last check")
                        # Calculate rate
                        time_diff = (current_stats['timestamp'] - previous_stats['timestamp']).total_seconds() / 60
                        rate = company_increase / time_diff if time_diff > 0 else 0
                        print(f"🚀 Rate: {rate:.1f} companies/minute")
                    else:
                        print("⚠️ No progress since last check")
                
                previous_stats = current_stats
                print("-" * 40)
            
            time.sleep(check_interval)
        
        print(f"\n✅ Monitoring completed after {duration_minutes} minutes")

if __name__ == "__main__":
    monitor = ProgressMonitor()
    
    # Show current status
    print("📊 Current Status:")
    stats = monitor.get_current_stats()
    if stats:
        print(f"Total Companies: {stats['total_companies']}")
        print(f"With Price Data: {stats['with_price_data']}")
        print(f"With Metrics: {stats['with_metrics']}")
        print(f"With Financials: {stats['with_financials']}")
    
    # Start monitoring
    monitor.monitor_progress(duration_minutes=30, check_interval=30)
