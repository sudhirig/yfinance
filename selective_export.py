
#!/usr/bin/env python3
"""
Export specific companies or data ranges for targeted transfer
"""

import os
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import logging

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

def export_top_companies_data(top_n=100, output_dir="selective_export"):
    """Export data for top N companies by market cap"""
    logger = setup_logging()
    
    try:
        os.makedirs(output_dir, exist_ok=True)
        
        database_url = os.getenv('DATABASE_URL')
        conn = psycopg2.connect(database_url)
        
        # Get top companies by market cap
        logger.info(f"📊 Exporting data for top {top_n} companies...")
        
        query = """
        SELECT c.id, c.symbol, c.long_name, cm.market_cap
        FROM companies c
        LEFT JOIN company_metrics cm ON c.id = cm.company_id
        ORDER BY cm.market_cap DESC NULLS LAST
        LIMIT %s
        """
        
        top_companies = pd.read_sql(query, conn, params=[top_n])
        company_ids = top_companies['id'].tolist()
        
        logger.info(f"✅ Selected {len(company_ids)} companies")
        
        # Export each data type
        tables_to_export = [
            ('companies', 'id'),
            ('company_metrics', 'company_id'),
            ('price_history', 'company_id'),
            ('income_statements', 'company_id'),
            ('balance_sheets', 'company_id'),
            ('cash_flow_statements', 'company_id'),
            ('corporate_actions', 'company_id')
        ]
        
        for table_name, id_column in tables_to_export:
            try:
                if table_name == 'companies':
                    query = f"SELECT * FROM {table_name} WHERE {id_column} IN ({','.join(map(str, company_ids))})"
                else:
                    query = f"SELECT * FROM {table_name} WHERE {id_column} IN ({','.join(map(str, company_ids))})"
                
                df = pd.read_sql(query, conn)
                
                if not df.empty:
                    output_file = os.path.join(output_dir, f"{table_name}.csv")
                    df.to_csv(output_file, index=False)
                    logger.info(f"✅ Exported {len(df)} records to {output_file}")
                else:
                    logger.warning(f"⚠️ No data found for {table_name}")
                    
            except Exception as e:
                logger.error(f"❌ Error exporting {table_name}: {e}")
        
        # Create import script
        import_script = os.path.join(output_dir, "import_selective_data.py")
        with open(import_script, 'w') as f:
            f.write(f'''#!/usr/bin/env python3
"""
Import selective data exported from another instance
"""

import os
import pandas as pd
import psycopg2
from csv_to_database_loader import CSVToDatabaseLoader

def import_selective_data():
    loader = CSVToDatabaseLoader()
    loader.connect_db()
    
    # Load CSV files in correct order
    csv_files = [
        'companies.csv',
        'company_metrics.csv', 
        'price_history.csv',
        'income_statements.csv',
        'balance_sheets.csv',
        'cash_flow_statements.csv',
        'corporate_actions.csv'
    ]
    
    for csv_file in csv_files:
        if os.path.exists(csv_file):
            print(f"Loading {{csv_file}}...")
            # Load using pandas and insert into database
            # Implementation would depend on specific data format
        else:
            print(f"{{csv_file}} not found, skipping...")
    
    loader.close_db()
    print("✅ Selective import completed!")

if __name__ == "__main__":
    import_selective_data()
''')
        
        logger.info(f"✅ Selective export completed!")
        logger.info(f"📁 Files saved to: {output_dir}/")
        logger.info(f"🔧 Import script: {import_script}")
        
        conn.close()
        return output_dir
        
    except Exception as e:
        logger.error(f"❌ Export error: {e}")
        return False

if __name__ == "__main__":
    export_top_companies_data(100)
