
#!/usr/bin/env python3
"""
Database sharing utility for yfinance data
"""

import os
import sys
import psycopg2
from database_config import get_database_url
import subprocess

def create_database_backup():
    """Create a backup of the current database"""
    try:
        database_url = get_database_url()
        if not database_url:
            print("❌ No database URL found. Make sure PostgreSQL is set up.")
            return False
        
        # Create backup using pg_dump
        backup_file = "yfinance_database_backup.sql"
        
        print("🔄 Creating database backup...")
        result = subprocess.run([
            "pg_dump", 
            database_url,
            "-f", backup_file,
            "--no-owner",
            "--no-privileges"
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ Database backup created: {backup_file}")
            print(f"📁 File size: {os.path.getsize(backup_file) / 1024 / 1024:.2f} MB")
            return True
        else:
            print(f"❌ Backup failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error creating backup: {e}")
        return False

def restore_database_from_backup(backup_file="yfinance_database_backup.sql"):
    """Restore database from backup file"""
    try:
        if not os.path.exists(backup_file):
            print(f"❌ Backup file not found: {backup_file}")
            return False
        
        database_url = get_database_url()
        if not database_url:
            print("❌ No database URL found. Make sure PostgreSQL is set up.")
            return False
        
        print("🔄 Restoring database from backup...")
        result = subprocess.run([
            "psql",
            database_url,
            "-f", backup_file
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Database restored successfully!")
            return True
        else:
            print(f"❌ Restore failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error restoring database: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python share_database.py backup    - Create backup")
        print("  python share_database.py restore   - Restore from backup")
        sys.exit(1)
    
    action = sys.argv[1].lower()
    
    if action == "backup":
        create_database_backup()
    elif action == "restore":
        restore_database_from_backup()
    else:
        print("❌ Invalid action. Use 'backup' or 'restore'")
