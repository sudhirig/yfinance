
import os
from typing import Dict

def get_database_config() -> Dict[str, str]:
    """
    Get database configuration from environment variables
    """
    return {
        'host': os.getenv('PGHOST', 'localhost'),
        'database': os.getenv('PGDATABASE', 'yfinance_db'),
        'user': os.getenv('PGUSER', 'postgres'),
        'password': os.getenv('PGPASSWORD', ''),
        'port': int(os.getenv('PGPORT', 5432))
    }

# For Replit PostgreSQL, these environment variables are automatically set:
# DATABASE_URL, PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
