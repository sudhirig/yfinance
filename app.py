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
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT symbol, long_name, sector, industry
        FROM companies 
        ORDER BY symbol 
        LIMIT 50
    """)
    stocks = cursor.fetchall()
    conn.close()

    return jsonify([{
        'symbol': stock[0],
        'name': stock[1],
        'sector': stock[2],
        'industry': stock[3]
    } for stock in stocks])

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)