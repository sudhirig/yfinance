
import os
from typing import Dict

def get_database_config() -> Dict[str, str]:
    """
    Get database configuration from environment variables
    """
    return {
        'host': os.getenv('PGHOST', '0.0.0.0'),
        'database': os.getenv('PGDATABASE', 'yfinance_db'),
        'user': os.getenv('PGUSER', 'postgres'),
        'password': os.getenv('PGPASSWORD', ''),
        'port': int(os.getenv('PGPORT', 5432))
    }

def get_database_url():
    """
    Get the database URL, prioritizing DATABASE_URL environment variable
    """
    return os.getenv('DATABASE_URL')

def get_db_connection():
    """
    Get a database connection using psycopg2
    """
    import psycopg2
    database_url = get_database_url()
    if database_url:
        return psycopg2.connect(database_url)
    else:
        config = get_database_config()
        return psycopg2.connect(**config)

# For Replit PostgreSQL, these environment variables are automatically set:
# DATABASE_URL, PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
