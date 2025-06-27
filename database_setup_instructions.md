
# Database Setup Instructions

## Quick Setup for Development

### Option 1: Use Replit PostgreSQL (Recommended)
1. Go to the Dependencies tool in Replit
2. Add PostgreSQL from the database section
3. Replit will automatically set all required environment variables
4. No manual configuration needed!

### Option 2: External Database
If using an external PostgreSQL database, set these environment variables in Replit Secrets:

```
PGHOST=your_database_host
PGDATABASE=yfinance_db
PGUSER=your_username
PGPASSWORD=your_password
PGPORT=5432
```

### Option 3: Local Development
Create a `.env` file in your project root:

```env
PGHOST=localhost
PGDATABASE=yfinance_db
PGUSER=postgres
PGPASSWORD=your_password
PGPORT=5432
```

## Security Note
Never commit database passwords to Git. Always use environment variables or Replit Secrets.

## Testing Connection
Run this to test your database connection:
```bash
python -c "from database_config import get_db_connection; print('✅ Database connected!' if get_db_connection() else '❌ Connection failed')"
```
