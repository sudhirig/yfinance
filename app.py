from flask import Flask, render_template, jsonify, request
import psycopg2
from database_config import get_db_connection
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor

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
            return jsonify({
                'symbol': symbol,
                'long_name': symbol.replace('.NS', ''),
                'sector': None,
                'industry': None,
                'business_summary': None,
                'website': None,
                'full_time_employees': None,
                'market_cap': None,
                'trailing_pe': None,
                'price_to_book': None,
                'dividend_yield': None,
                'beta': None
            })

        # Map database columns to response
        return jsonify({
            'symbol': result[1] if result[1] else symbol,
            'long_name': result[2] if result[2] else symbol.replace('.NS', ''),
            'sector': result[3] if len(result) > 3 else None,
            'industry': result[4] if len(result) > 4 else None,
            'business_summary': result[5] if len(result) > 5 else None,
            'website': result[6] if len(result) > 6 else None,
            'full_time_employees': result[7] if len(result) > 7 else None,
            'market_cap': result[14] if len(result) > 14 and result[14] else None,
            'trailing_pe': result[15] if len(result) > 15 and result[15] else None,
            'price_to_book': result[16] if len(result) > 16 and result[16] else None,
            'dividend_yield': result[17] if len(result) > 17 and result[17] else None,
            'beta': result[18] if len(result) > 18 and result[18] else None
        })

    except Exception as e:
        print(f"Error in stock info for {symbol}: {e}")
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
            # Return empty metrics if no data found
            return jsonify({
                'market_cap': None,
                'trailing_pe': None,
                'forward_pe': None,
                'price_to_book': None,
                'dividend_yield': None,
                'dividend_rate': None,
                'beta': None,
                'fifty_two_week_high': None,
                'fifty_two_week_low': None,
                'price_to_sales': None,
                'enterprise_value': None,
                'profit_margin': None,
                'operating_margin': None,
                'return_on_assets': None,
                'return_on_equity': None,
                'revenue_per_share': None,
                'debt_to_equity': None,
                'current_ratio': None,
                'book_value': None,
                'operating_cash_flow': None,
                'levered_free_cash_flow': None
            })

        return jsonify({
            'market_cap': result[2] if len(result) > 2 else None,
            'trailing_pe': result[3] if len(result) > 3 else None,
            'forward_pe': result[4] if len(result) > 4 else None,
            'price_to_book': result[5] if len(result) > 5 else None,
            'dividend_yield': result[6] if len(result) > 6 else None,
            'dividend_rate': result[7] if len(result) > 7 else None,
            'beta': result[8] if len(result) > 8 else None,
            'fifty_two_week_high': result[9] if len(result) > 9 else None,
            'fifty_two_week_low': result[10] if len(result) > 10 else None,
            'price_to_sales': result[11] if len(result) > 11 else None,
            'enterprise_value': result[12] if len(result) > 12 else None,
            'profit_margin': result[13] if len(result) > 13 else None,
            'operating_margin': result[14] if len(result) > 14 else None,
            'return_on_assets': result[15] if len(result) > 15 else None,
            'return_on_equity': result[16] if len(result) > 16 else None,
            'revenue_per_share': result[17] if len(result) > 17 else None,
            'debt_to_equity': result[18] if len(result) > 18 else None,
            'current_ratio': result[19] if len(result) > 19 else None,
            'book_value': result[20] if len(result) > 20 else None,
            'operating_cash_flow': result[21] if len(result) > 21 else None,
            'levered_free_cash_flow': result[22] if len(result) > 22 else None
        })

    except Exception as e:
        print(f"Error in metrics for {symbol}: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/stock/<symbol>/financials')
def get_stock_financials(symbol):
    """Get financial statements for a stock"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get income statements with better error handling
        try:
            cursor.execute("""
                SELECT i.period_ending, i.period_type, i.total_revenue, i.cost_of_revenue, 
                       i.gross_profit, i.operating_income, i.net_income, i.diluted_eps,
                       i.operating_expense, i.interest_expense, i.tax_provision,
                       i.income_before_tax, i.normalized_income, i.total_expenses
                FROM income_statements i
                JOIN companies c ON c.id = i.company_id
                WHERE c.symbol = %s
                ORDER BY i.period_ending DESC
                LIMIT 5
            """, (symbol,))
        except Exception as e:
            print(f"Income statement query error for {symbol}: {e}")
            cursor.execute("SELECT 1 WHERE FALSE")  # Empty result
        income_data = cursor.fetchall()

        # Get balance sheets with error handling
        try:
            cursor.execute("""
                SELECT b.period_ending, b.period_type, b.total_assets, b.current_assets,
                       b.total_liabilities, b.current_liabilities, b.stockholders_equity,
                       b.total_debt, b.cash_and_cash_equivalents, b.accounts_receivable,
                       b.inventory, b.net_ppe, b.accounts_payable,
                       b.total_debt, b.retained_earnings
                FROM balance_sheets b
                JOIN companies c ON c.id = b.company_id
                WHERE c.symbol = %s
                ORDER BY b.period_ending DESC
                LIMIT 5
            """, (symbol,))
            balance_data = cursor.fetchall()
        except Exception as e:
            balance_data = []
            print(f"Balance sheet query error for {symbol}: {e}")

        # Get cash flow statements with error handling
        try:
            cursor.execute("""
                SELECT cf.period_ending, cf.period_type, cf.operating_cash_flow,
                       cf.investing_cash_flow, cf.financing_cash_flow, cf.free_cash_flow,
                       cf.capital_expenditure, cf.cash_dividends_paid, cf.changes_in_cash,
                       cf.depreciation_and_amortization, cf.change_in_working_capital
                FROM cash_flow_statements cf
                JOIN companies c ON c.id = cf.company_id
                WHERE c.symbol = %s
                ORDER BY cf.period_ending DESC
                LIMIT 5
            """, (symbol,))
            cashflow_data = cursor.fetchall()
        except Exception as e:
            cashflow_data = []
            print(f"Cash flow query error for {symbol}: {e}")

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
                'operating_expense': float(row[8]) if row[8] else None,
                'interest_expense': float(row[9]) if row[9] else None,
                'tax_provision': float(row[10]) if row[10] else None,
                'income_before_tax': float(row[11]) if row[11] else None,
                'normalized_income': float(row[12]) if row[12] else None,
                'total_expenses': float(row[13]) if row[13] else None
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
                'cash_and_cash_equivalents': float(row[8]) if row[8] else None,
                'accounts_receivable': float(row[9]) if row[9] else None,
                'inventory': float(row[10]) if row[10] else None,
                'net_ppe': float(row[11]) if row[11] else None,
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
                'capital_expenditure': float(row[6]) if row[6] else None,
                'cash_dividends_paid': float(row[7]) if row[7] else None,
                'changes_in_cash': float(row[8]) if row[8] else None,
                'depreciation_and_amortization': float(row[9]) if row[9] else None,
                'change_in_working_capital': float(row[10]) if row[10] else None
            } for row in cashflow_data]
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stock/<symbol>/ratios')
def get_stock_ratios(symbol):
    """Get financial ratios for a specific stock"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Get company ID
        cursor.execute("SELECT id FROM companies WHERE symbol = %s", (symbol,))
        company = cursor.fetchone()

        if not company:
            return jsonify({'error': 'Company not found'}), 404

        company_id = company['id']

        # Calculate ratios from latest data
        cursor.execute("""
            WITH latest_data AS (
                SELECT 
                    i.total_revenue, i.net_income, i.gross_profit, i.operating_income,
                    b.total_assets, b.stockholders_equity, b.total_debt, b.current_assets, b.current_liabilities,
                    cf.operating_cash_flow, cf.free_cash_flow,
                    cm.market_cap, cm.trailing_pe, cm.price_to_book, cm.dividend_yield
                FROM companies c
                LEFT JOIN income_statements i ON c.id = i.company_id 
                    AND i.period_ending = (SELECT MAX(period_ending) FROM income_statements WHERE company_id = c.id AND period_type = 'annual')
                    AND i.period_type = 'annual'
                LEFT JOIN balance_sheets b ON c.id = b.company_id 
                    AND b.period_ending = (SELECT MAX(period_ending) FROM balance_sheets WHERE company_id = c.id AND period_type = 'annual')
                    AND b.period_type = 'annual'
                LEFT JOIN cash_flow_statements cf ON c.id = cf.company_id 
                    AND cf.period_ending = (SELECT MAX(period_ending) FROM cash_flow_statements WHERE company_id = c.id AND period_type = 'annual')
                    AND cf.period_type = 'annual'
                LEFT JOIN company_metrics cm ON c.id = cm.company_id
                WHERE c.id = %s
            )
            SELECT 
                -- Profitability Ratios
                CASE WHEN total_revenue > 0 THEN ROUND((gross_profit::numeric / total_revenue * 100), 2) ELSE NULL END as gross_margin,
                CASE WHEN total_revenue > 0 THEN ROUND((operating_income::numeric / total_revenue * 100), 2) ELSE NULL END as operating_margin,
                CASE WHEN total_revenue > 0 THEN ROUND((net_income::numeric / total_revenue * 100), 2) ELSE NULL END as net_margin,
                CASE WHEN total_assets > 0 THEN ROUND((net_income::numeric / total_assets * 100), 2) ELSE NULL END as roa,
                CASE WHEN stockholders_equity > 0 THEN ROUND((net_income::numeric / stockholders_equity * 100), 2) ELSE NULL END as roe,

                -- Liquidity Ratios
                CASE WHEN current_liabilities > 0 THEN ROUND((current_assets::numeric / current_liabilities), 2) ELSE NULL END as current_ratio,

                -- Leverage Ratios
                CASE WHEN stockholders_equity > 0 THEN ROUND((total_debt::numeric / stockholders_equity), 2) ELSE NULL END as debt_to_equity,
                CASE WHEN total_assets > 0 THEN ROUND((total_debt::numeric / total_assets * 100), 2) ELSE NULL END as debt_ratio,

                -- Valuation Ratios
                trailing_pe,
                price_to_book,
                dividend_yield,

                -- Efficiency Ratios
                CASE WHEN total_assets > 0 THEN ROUND((total_revenue::numeric / total_assets), 2) ELSE NULL END as asset_turnover
            FROM latest_data
        """, (company_id,))

        ratios = cursor.fetchone()
        cursor.close()

        if ratios:
            return jsonify(dict(ratios))
        else:
            return jsonify({'error': 'No financial data available'}), 404

    except Exception as e:
        print(f"Error fetching ratios for {symbol}: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/stock/<symbol>/historical-metrics')
def get_historical_metrics(symbol):
    """Get historical metrics for a specific stock"""
    try:
        conn = get_db_connection()
        period_type = request.args.get('period', 'quarterly')  # annual, quarterly
        limit = request.args.get('limit', 20, type=int)

        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Get company ID
        cursor.execute("SELECT id FROM companies WHERE symbol = %s", (symbol,))
        company = cursor.fetchone()

        if not company:
            return jsonify({'error': 'Company not found'}), 404

        company_id = company['id']

        # Get historical metrics
        cursor.execute("""
            SELECT 
                metric_date,
                period_type,
                market_cap,
                trailing_pe,
                price_to_book,
                gross_margin,
                operating_margin,
                profit_margin,
                return_on_equity,
                return_on_assets,
                debt_to_equity,
                current_ratio,
                revenue_growth_yoy,
                earnings_growth_yoy,
                dividend_yield,
                fcf_per_share
            FROM historical_company_metrics
            WHERE company_id = %s AND period_type = %s
            ORDER BY metric_date DESC
            LIMIT %s
        """, (company_id, period_type, limit))

        metrics = cursor.fetchall()
        cursor.close()

        # Convert to list of dictionaries with formatted dates
        result = []
        for metric in metrics:
            metric_dict = dict(metric)
            metric_dict['metric_date'] = metric_dict['metric_date'].isoformat()
            result.append(metric_dict)

        return jsonify(result)

    except Exception as e:
        print(f"Error fetching historical metrics for {symbol}: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/stock/<symbol>/metrics-trend')
def get_metrics_trend(symbol):
    """Get historical trend for a specific metric"""
    try:
        metric = request.args.get('metric', 'trailing_pe')
        years = int(request.args.get('years', 3))

        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
            SELECT metric_date, %s as metric_value
            FROM historical_company_metrics hcm
            JOIN companies c ON hcm.company_id = c.id
            WHERE c.symbol = %s 
                AND metric_date >= %s
                AND %s IS NOT NULL
            ORDER BY metric_date ASC
        """ % (metric, '%s', '%s', metric)

        start_date = datetime.now() - timedelta(days=years*365)
        cursor.execute(query, (symbol, start_date))

        results = cursor.fetchall()
        cursor.close()

        if not results:
            return jsonify({'error': f'No {metric} data found for {symbol}'}), 404

        return jsonify({
            'symbol': symbol,
            'metric': metric,
            'years': years,
            'data': [{'date': row[0].isoformat(), 'value': float(row[1])} for row in results]
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stock/<symbol>/fresh-historical-metrics')
def get_fresh_historical_metrics(symbol):
    """Fetch fresh historical metrics directly from yfinance"""
    try:
        years = int(request.args.get('years', 3))

        from yfinance_historical_metrics_fetcher import YFinanceHistoricalMetricsFetcher

        fetcher = YFinanceHistoricalMetricsFetcher()

        # Get calculated historical metrics
        metrics_df = fetcher.calculate_historical_metrics(symbol, years_back=years)

        if metrics_df is None or metrics_df.empty:
            return jsonify({'error': f'No data available for {symbol}'}), 404

        # Convert to JSON-friendly format
        data = []
        for _, row in metrics_df.iterrows():
            record = {
                'date': row['date'].isoformat(),
                'period_type': row['period_type'],
                'close_price': row.get('close_price'),
                'market_cap': row.get('market_cap'),
                'trailing_pe': row.get('trailing_pe'),
                'return_on_equity': row.get('return_on_equity'),
                'price_to_book': row.get('price_to_book'),
                'price_to_sales': row.get('price_to_sales'),
                'total_revenue': row.get('total_revenue'),
                'book_value_per_share': row.get('book_value_per_share')
            }
            # Remove None values
            record = {k: v for k, v in record.items() if v is not None}
            data.append(record)

        return jsonify({
            'symbol': symbol,
            'years': years,
            'source': 'yfinance_direct',
            'records_count': len(data),
            'data': data
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stock/<symbol>/price-history-metrics')
def get_price_history_metrics(symbol):
    """Get price-based historical metrics directly from yfinance"""
    try:
        period = request.args.get('period', '2y')  # 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max

        from yfinance_historical_metrics_fetcher import YFinanceHistoricalMetricsFetcher

        fetcher = YFinanceHistoricalMetricsFetcher()
        price_metrics = fetcher.get_price_based_metrics(symbol, period=period)

        if price_metrics is None or price_metrics.empty:
            return jsonify({'error': f'No price data available for {symbol}'}), 404

        # Convert to JSON format
        data = []
        for date, row in price_metrics.iterrows():
            record = {
                'date': date.isoformat(),
                'open': row.get('Open'),
                'high': row.get('High'),
                'low': row.get('Low'),
                'close': row.get('Close'),
                'volume': row.get('Volume'),
                'market_cap': row.get('market_cap'),
                'dividend_yield': row.get('dividend_yield', 0)
            }
            # Remove None/NaN values
            record = {k: v for k, v in record.items() if pd.notna(v) and v is not None}
            data.append(record)

        return jsonify({
            'symbol': symbol,
            'period': period,
            'source': 'yfinance_direct',
            'records_count': len(data),
            'date_range': {
                'start': price_metrics.index[0].isoformat(),
                'end': price_metrics.index[-1].isoformat()
            },
            'data': data
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/metrics/comparison')
def get_metrics_comparison():
    """Compare metrics across multiple companies"""
    try:
        conn = get_db_connection()
        symbols = request.args.get('symbols', '').split(',')
        metric = request.args.get('metric', 'trailing_pe')
        period_type = request.args.get('period', 'quarterly')

        if not symbols or len(symbols) > 10:
            return jsonify({'error': 'Please provide 1-10 company symbols'}), 400

        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Get latest metrics for comparison
        placeholders = ','.join(['%s'] * len(symbols))
        cursor.execute(f"""
            SELECT 
                c.symbol,
                c.long_name,
                hcm.metric_date,
                hcm.{metric} as value
            FROM companies c
            JOIN historical_company_metrics hcm ON c.id = hcm.company_id
            WHERE c.symbol IN ({placeholders})
            AND hcm.period_type = %s
            AND hcm.{metric} IS NOT NULL
            AND hcm.metric_date = (
                SELECT MAX(metric_date) 
                FROM historical_company_metrics hcm2 
                WHERE hcm2.company_id = hcm.company_id 
                AND hcm2.period_type = %s
                AND hcm2.{metric} IS NOT NULL
            )
            ORDER BY hcm.{metric} DESC
        """, symbols + [period_type, period_type])

        comparison_data = cursor.fetchall()
        cursor.close()

        result = {
            'metric': metric,
            'period_type': period_type,
            'companies': [
                {
                    'symbol': row['symbol'],
                    'name': row['long_name'],
                    'value': float(row['value']),
                    'date': row['metric_date'].isoformat()
                }
                for row in comparison_data
            ]
        }

        return jsonify(result)

    except Exception as e:
        print(f"Error in metrics comparison: {e}")
        return jsonify({'error': 'Internal server error'}), 500

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
    conn = get_db_connection()
    app.run(host='0.0.0.0', port=5000, debug=True)