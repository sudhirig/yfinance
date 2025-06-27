
import os
from typing import Dict

def get_database_config() -> Dict[str, str]:
    """
    Get database configuration from environment variables
    Configure your database by setting these environment variables:
    - PGHOST: Database host (default: localhost)
    - PGDATABASE: Database name (default: yfinance_db)
    - PGUSER: Database username (default: postgres)
    - PGPASSWORD: Database password (required)
    - PGPORT: Database port (default: 5432)
    
    For production, use Replit's PostgreSQL service which auto-configures these.
    For local development, set these in your .env file or Replit Secrets.
    """
    return {
        'host': os.getenv('PGHOST', 'localhost'),
        'database': os.getenv('PGDATABASE', 'yfinance_db'),
        'user': os.getenv('PGUSER', 'postgres'),
        'password': os.getenv('PGPASSWORD', 'your_password_here'),
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
