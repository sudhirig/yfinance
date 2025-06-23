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

@app.route('/api/stock/<symbol>/info')
def get_stock_info(symbol):
    """Get comprehensive company information"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT c.*, cm.*
            FROM companies c
            LEFT JOIN company_metrics cm ON c.id = cm.company_id
            WHERE c.symbol = %s
        """, (symbol,))
        
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return jsonify({'error': 'Stock not found'}), 404
            
        # Map database columns to response
        return jsonify({
            'symbol': result[1],
            'long_name': result[2],
            'sector': result[3],
            'industry': result[4],
            'business_summary': result[5],
            'website': result[6],
            'full_time_employees': result[7],
            'market_cap': result[14] if len(result) > 14 else None,
            'trailing_pe': result[15] if len(result) > 15 else None,
            'price_to_book': result[16] if len(result) > 16 else None,
            'dividend_yield': result[17] if len(result) > 17 else None,
            'beta': result[18] if len(result) > 18 else None
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stock/<symbol>/metrics')
def get_stock_metrics(symbol):
    """Get financial metrics for a stock"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT cm.*
            FROM company_metrics cm
            JOIN companies c ON c.id = cm.company_id
            WHERE c.symbol = %s
        """, (symbol,))
        
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return jsonify({'error': 'Metrics not found'}), 404
            
        return jsonify({
            'market_cap': result[2],
            'trailing_pe': result[3],
            'forward_pe': result[4],
            'price_to_book': result[5],
            'dividend_yield': result[6],
            'dividend_rate': result[7],
            'beta': result[8],
            'fifty_two_week_high': result[9],
            'fifty_two_week_low': result[10],
            'price_to_sales': result[11],
            'enterprise_value': result[12],
            'profit_margin': result[13],
            'operating_margin': result[14],
            'return_on_assets': result[15],
            'return_on_equity': result[16],
            'revenue_per_share': result[17],
            'debt_to_equity': result[18],
            'current_ratio': result[19],
            'book_value': result[20],
            'operating_cash_flow': result[21],
            'levered_free_cash_flow': result[22]
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stock/<symbol>/financials')
def get_stock_financials(symbol):
    """Get financial statements for a stock"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get income statements with proper column mapping
        cursor.execute("""
            SELECT i.period_ending, i.period_type, i.total_revenue, i.cost_of_revenue, 
                   i.gross_profit, i.operating_income, i.net_income, i.earnings_per_share,
                   i.research_development, i.selling_general_admin, i.interest_expense,
                   i.income_before_tax, i.income_tax_expense, i.other_income_expense
            FROM income_statements i
            JOIN companies c ON c.id = i.company_id
            WHERE c.symbol = %s AND i.period_type = 'annual'
            ORDER BY i.period_ending DESC
            LIMIT 5
        """, (symbol,))
        income_data = cursor.fetchall()
        
        # Get balance sheets with proper column mapping
        cursor.execute("""
            SELECT b.period_ending, b.period_type, b.total_assets, b.current_assets,
                   b.total_liabilities, b.current_liabilities, b.stockholders_equity,
                   b.total_debt, b.cash_and_equivalents, b.accounts_receivable,
                   b.inventory, b.property_plant_equipment, b.accounts_payable,
                   b.long_term_debt, b.retained_earnings
            FROM balance_sheets b
            JOIN companies c ON c.id = b.company_id
            WHERE c.symbol = %s AND b.period_type = 'annual'
            ORDER BY b.period_ending DESC
            LIMIT 5
        """, (symbol,))
        balance_data = cursor.fetchall()
        
        # Get cash flow statements with proper column mapping
        cursor.execute("""
            SELECT cf.period_ending, cf.period_type, cf.operating_cash_flow,
                   cf.investing_cash_flow, cf.financing_cash_flow, cf.free_cash_flow,
                   cf.capital_expenditures, cf.dividends_paid, cf.net_change_in_cash,
                   cf.depreciation_amortization, cf.change_in_working_capital
            FROM cash_flow_statements cf
            JOIN companies c ON c.id = cf.company_id
            WHERE c.symbol = %s AND cf.period_type = 'annual'
            ORDER BY cf.period_ending DESC
            LIMIT 5
        """, (symbol,))
        cashflow_data = cursor.fetchall()
        
        conn.close()
        
        return jsonify({
            'income': [{
                'period_ending': row[0].strftime('%Y-%m-%d') if row[0] else None,
                'period_type': row[1],
                'total_revenue': float(row[2]) if row[2] else None,
                'cost_of_revenue': float(row[3]) if row[3] else None,
                'gross_profit': float(row[4]) if row[4] else None,
                'operating_income': float(row[5]) if row[5] else None,
                'net_income': float(row[6]) if row[6] else None,
                'earnings_per_share': float(row[7]) if row[7] else None,
                'research_development': float(row[8]) if row[8] else None,
                'selling_general_admin': float(row[9]) if row[9] else None,
                'interest_expense': float(row[10]) if row[10] else None,
                'income_before_tax': float(row[11]) if row[11] else None,
                'income_tax_expense': float(row[12]) if row[12] else None,
                'other_income_expense': float(row[13]) if row[13] else None
            } for row in income_data],
            'balance': [{
                'period_ending': row[0].strftime('%Y-%m-%d') if row[0] else None,
                'period_type': row[1],
                'total_assets': float(row[2]) if row[2] else None,
                'current_assets': float(row[3]) if row[3] else None,
                'total_liabilities': float(row[4]) if row[4] else None,
                'current_liabilities': float(row[5]) if row[5] else None,
                'stockholders_equity': float(row[6]) if row[6] else None,
                'total_debt': float(row[7]) if row[7] else None,
                'cash_and_equivalents': float(row[8]) if row[8] else None,
                'accounts_receivable': float(row[9]) if row[9] else None,
                'inventory': float(row[10]) if row[10] else None,
                'property_plant_equipment': float(row[11]) if row[11] else None,
                'accounts_payable': float(row[12]) if row[12] else None,
                'long_term_debt': float(row[13]) if row[13] else None,
                'retained_earnings': float(row[14]) if row[14] else None
            } for row in balance_data],
            'cashflow': [{
                'period_ending': row[0].strftime('%Y-%m-%d') if row[0] else None,
                'period_type': row[1],
                'operating_cash_flow': float(row[2]) if row[2] else None,
                'investing_cash_flow': float(row[3]) if row[3] else None,
                'financing_cash_flow': float(row[4]) if row[4] else None,
                'free_cash_flow': float(row[5]) if row[5] else None,
                'capital_expenditures': float(row[6]) if row[6] else None,
                'dividends_paid': float(row[7]) if row[7] else None,
                'net_change_in_cash': float(row[8]) if row[8] else None,
                'depreciation_amortization': float(row[9]) if row[9] else None,
                'change_in_working_capital': float(row[10]) if row[10] else None
            } for row in cashflow_data]
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stock/<symbol>/ratios')
def get_stock_ratios(symbol):
    """Get financial ratios calculated from statements"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                i.period_ending,
                CASE WHEN i.total_revenue > 0 THEN i.net_income::float / i.total_revenue * 100 ELSE NULL END as profit_margin,
                CASE WHEN b.total_assets > 0 THEN i.net_income::float / b.total_assets * 100 ELSE NULL END as roa,
                CASE WHEN b.stockholders_equity > 0 THEN i.net_income::float / b.stockholders_equity * 100 ELSE NULL END as roe,
                CASE WHEN b.current_liabilities > 0 THEN b.current_assets::float / b.current_liabilities ELSE NULL END as current_ratio,
                CASE WHEN b.stockholders_equity > 0 THEN b.total_debt::float / b.stockholders_equity ELSE NULL END as debt_to_equity,
                CASE WHEN i.total_revenue > 0 THEN i.operating_income::float / i.total_revenue * 100 ELSE NULL END as operating_margin
            FROM income_statements i
            JOIN balance_sheets b ON i.company_id = b.company_id AND i.period_ending = b.period_ending
            JOIN companies c ON c.id = i.company_id
            WHERE c.symbol = %s AND i.period_type = 'annual'
            ORDER BY i.period_ending DESC
            LIMIT 5
        """, (symbol,))
        
        ratios = cursor.fetchall()
        conn.close()
        
        return jsonify([{
            'period_ending': row[0].strftime('%Y-%m-%d') if row[0] else None,
            'profit_margin': row[1],
            'return_on_assets': row[2],
            'return_on_equity': row[3],
            'current_ratio': row[4],
            'debt_to_equity': row[5],
            'operating_margin': row[6]
        } for row in ratios])
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stock/<symbol>/analysis')
def get_stock_analysis(symbol):
    """Get comprehensive analysis including trends and comparisons"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Revenue growth analysis
        cursor.execute("""
            WITH revenue_growth AS (
                SELECT 
                    period_ending,
                    total_revenue,
                    LAG(total_revenue) OVER (ORDER BY period_ending) as prev_revenue,
                    CASE WHEN LAG(total_revenue) OVER (ORDER BY period_ending) > 0 
                         THEN (total_revenue - LAG(total_revenue) OVER (ORDER BY period_ending))::float / LAG(total_revenue) OVER (ORDER BY period_ending) * 100 
                         ELSE NULL END as growth_rate
                FROM income_statements i
                JOIN companies c ON c.id = i.company_id
                WHERE c.symbol = %s AND i.period_type = 'annual' AND i.total_revenue IS NOT NULL
                ORDER BY period_ending DESC
                LIMIT 5
            )
            SELECT * FROM revenue_growth WHERE growth_rate IS NOT NULL
        """, (symbol,))
        
        growth_data = cursor.fetchall()
        
        # Price performance vs sector
        cursor.execute("""
            SELECT 
                c.sector,
                COUNT(*) as sector_companies,
                AVG(cm.trailing_pe) as avg_sector_pe,
                AVG(cm.price_to_book) as avg_sector_pb
            FROM companies c
            JOIN company_metrics cm ON c.id = cm.company_id
            WHERE c.sector = (SELECT sector FROM companies WHERE symbol = %s)
            GROUP BY c.sector
        """, (symbol,))
        
        sector_data = cursor.fetchone()
        
        conn.close()
        
        return jsonify({
            'revenue_growth': [{
                'period': row[0].strftime('%Y-%m-%d') if row[0] else None,
                'revenue': row[1],
                'growth_rate': row[3]
            } for row in growth_data],
            'sector_comparison': {
                'sector': sector_data[0] if sector_data else None,
                'sector_companies': sector_data[1] if sector_data else None,
                'avg_sector_pe': sector_data[2] if sector_data else None,
                'avg_sector_pb': sector_data[3] if sector_data else None
            } if sector_data else None
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

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