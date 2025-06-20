from flask import Flask, render_template, jsonify, request
import psycopg2
from database_config import get_db_connection
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/api/stocks')
def get_stocks():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT c.symbol, c.long_name, c.sector, c.industry
            FROM companies c
            ORDER BY c.symbol 
            LIMIT 50
        """)
        stocks = cursor.fetchall()
        conn.close()

        return jsonify([{
            'symbol': stock[0],
            'name': stock[1] if stock[1] else stock[0],
            'sector': stock[2] if stock[2] else 'N/A',
            'industry': stock[3] if stock[3] else 'N/A'
        } for stock in stocks])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stock/<symbol>')
def get_stock_data(symbol):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get latest price data
    cursor.execute("""
        SELECT date, open_price, high_price, low_price, close_price, volume
        FROM price_history 
        WHERE company_id = (SELECT id FROM companies WHERE symbol = %s)
        ORDER BY date DESC 
        LIMIT 30
    """, (symbol,))

    price_data = cursor.fetchall()
    conn.close()

    return jsonify([{
        'date': row[0].strftime('%Y-%m-%d'),
        'open': float(row[1]) if row[1] else None,
        'high': float(row[2]) if row[2] else None,
        'low': float(row[3]) if row[3] else None,
        'close': float(row[4]) if row[4] else None,
        'volume': int(row[5]) if row[5] else None
    } for row in price_data])

@app.route('/api/search')
def search_stocks():
    query = request.args.get('q', '').upper()
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT symbol, long_name, sector 
        FROM companies 
        WHERE symbol ILIKE %s OR long_name ILIKE %s
        LIMIT 10
    """, (f'%{query}%', f'%{query}%'))

    results = cursor.fetchall()
    conn.close()

    return jsonify([{
        'symbol': result[0],
        'name': result[1],
        'sector': result[2]
    } for result in results])

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')

@app.route('/admin')
def admin():
    return render_template('admin.html')

@app.route('/admin/share', methods=['POST'])
def admin_share():
    """Generate shareable database export"""
    try:
        import subprocess
        import os

        # Run the export script
        result = subprocess.run(['python', 'export_database.py'], 
                              capture_output=True, text=True, timeout=300)

        if result.returncode == 0:
            # Look for generated SQL file
            sql_files = [f for f in os.listdir('.') if f.startswith('yfinance_export_') and f.endswith('.sql')]
            if sql_files:
                latest_file = max(sql_files, key=os.path.getctime)
                return jsonify({
                    'success': True, 
                    'message': f'Database exported successfully to {latest_file}',
                    'filename': latest_file
                })
            else:
                return jsonify({'success': False, 'message': 'Export completed but no file found'})
        else:
            return jsonify({'success': False, 'message': f'Export failed: {result.stderr}'})

    except Exception as e:
        return jsonify({'success': False, 'message': f'Export error: {str(e)}'})

@app.route('/admin/export', methods=['POST'])
def admin_export():
    """Export database to downloadable format"""
    try:
        import subprocess
        import os
        from datetime import datetime

        # Run selective export
        result = subprocess.run(['python', 'selective_export.py'], 
                              capture_output=True, text=True, timeout=300)

        if result.returncode == 0:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            export_file = f'yfinance_selective_export_{timestamp}.sql'

            return jsonify({
                'success': True, 
                'message': f'Selective export completed: {export_file}',
                'filename': export_file
            })
        else:
            return jsonify({'success': False, 'message': f'Export failed: {result.stderr}'})

    except Exception as e:
        return jsonify({'success': False, 'message': f'Export error: {str(e)}'})

@app.route('/admin/logs')
def admin_logs():
    """Get system logs for monitoring"""
    try:
        import os
        logs = []

        # Check for log files
        log_files = ['yfinance_loader.log', 'yfinance_nse_downloader.log']

        for log_file in log_files:
            if os.path.exists(log_file):
                try:
                    with open(log_file, 'r') as f:
                        # Get last 50 lines
                        lines = f.readlines()
                        recent_lines = lines[-50:] if len(lines) > 50 else lines
                        logs.append({
                            'file': log_file,
                            'content': ''.join(recent_lines),
                            'size': len(lines)
                        })
                except Exception as e:
                    logs.append({
                        'file': log_file,
                        'content': f'Error reading log: {str(e)}',
                        'size': 0
                    })
            else:
                logs.append({
                    'file': log_file,
                    'content': 'Log file not found',
                    'size': 0
                })

        return jsonify({'success': True, 'logs': logs})

    except Exception as e:
        return jsonify({'success': False, 'message': f'Error reading logs: {str(e)}'})

@app.route('/admin/stats')
def admin_stats():
    """Get database statistics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get table counts
        stats = {}
        tables = ['companies', 'price_history', 'company_metrics', 
                 'income_statements', 'balance_sheets', 'cash_flow_statements']

        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                stats[table] = count
            except Exception as e:
                stats[table] = f"Error: {str(e)}"

        # Get latest update info
        try:
            cursor.execute("""
                SELECT MAX(date) as latest_date, COUNT(DISTINCT company_id) as companies_with_data
                FROM price_history
            """)
            result = cursor.fetchone()
            stats['latest_price_date'] = result[0].strftime('%Y-%m-%d') if result[0] else 'No data'
            stats['companies_with_price_data'] = result[1]
        except Exception as e:
            stats['latest_price_date'] = f"Error: {str(e)}"
            stats['companies_with_price_data'] = 0

        conn.close()
        return jsonify({'success': True, 'stats': stats})

    except Exception as e:
        return jsonify({'success': False, 'message': f'Error getting stats: {str(e)}'})

@app.route('/admin/download', methods=['POST'])
def admin_download():
    """Start background download process"""
    try:
        import subprocess

        # Start the NSE downloader in background
        process = subprocess.Popen(['python', 'yfinance_nse_downloader.py'], 
                                 stdout=subprocess.PIPE, 
                                 stderr=subprocess.PIPE)

        return jsonify({
            'success': True, 
            'message': f'Download process started with PID: {process.pid}',
            'pid': process.pid
        })

    except Exception as e:
        return jsonify({'success': False, 'message': f'Failed to start download: {str(e)}'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)