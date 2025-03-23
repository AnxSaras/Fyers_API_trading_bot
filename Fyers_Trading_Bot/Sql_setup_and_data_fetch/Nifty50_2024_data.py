import datetime
import time
import mysql.connector as mdb
from fyers_apiv3 import fyersModel

# ✅ Fyers API Credentials
app_id = "ISVT1FBO9P-100"
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJhcGkuZnllcnMuaW4iLCJpYXQiOjE3NDIzNTgyNTksImV4cCI6MTc0MjQzMDYxOSwibmJmIjoxNzQyMzU4MjU5LCJhdWQiOlsieDowIiwieDoxIiwieDoyIiwiZDoxIiwiZDoyIiwieDoxIiwieDowIl0sInN1YiI6ImFjY2Vzc190b2tlbiIsImF0X2hhc2giOiJnQUFBQUFCbjJrYnp1WWtxaExaMndfbm9MdTFFTWVJdVhYOXZ0V1FqUjVlUjhEdk5IcS1od0tETkx6VW9LMzRQNE1KZWhGY0h2QkFkTUxTN05ycE52N1p2ZGRyanhrNXZaQy0yN3UzNGFadjJqQ2FEbjkyVWxVZz0iLCJkaXNwbGF5X25hbWUiOiJBTktVUiBTQVJBU1dBVCIsIm9tcyI6IksxIiwiaHNtX2tleSI6ImZlODhjOTZiZTg2NTUwOTU4MDBlMTIwYjQxNzUwNmJiOThiN2I0MmY0OTQ2MGE2Y2YyMDNkMGE2IiwiaXNEZHBpRW5hYmxlZCI6Ik4iLCJpc010ZkVuYWJsZWQiOiJOIiwiZnlfaWQiOiJZQTQ2MjE4IiwiYXBwVHlwZSI6MTAwLCJwb2FfZmxhZyI6Ik4ifQ.QQWzSioZ-qGFJ2N8W1QPNdMgtmCpWjTsPsSCpYycp0c"

# ✅ Initialize Fyers API
fyers = fyersModel.FyersModel(client_id=app_id, token=access_token, is_async=False)

# ✅ MySQL Database Connection
db_config = {
    "host": "localhost",
    "user": "Algo_Trading",
    "password": "Apple@1331",
    "database": "Historical_data_2024"
}
con = mdb.connect(**db_config)
cur = con.cursor()

# ✅ Fetch Tickers from Database
def get_nifty50_tickers():
    """Fetch Nifty 50 tickers and symbol IDs from the database."""
    cur.execute("SELECT id, ticker, name FROM symbol")
    return cur.fetchall()

# ✅ Fetch Historical Data from Fyers API
def fetch_historical_data(symbol, start_date, end_date):
    """Fetches historical stock data from Fyers API in chunks."""
    all_prices = []
    batch_size = 100  # Fetch 100 days per request
    
    while start_date < end_date:
        batch_end = min(start_date + datetime.timedelta(days=batch_size), end_date)
        payload = {
            "symbol": f"NSE:{symbol}-EQ",
            "resolution": "D",
            "date_format": "1",
            "range_from": start_date.strftime('%Y-%m-%d'),
            "range_to": batch_end.strftime('%Y-%m-%d'),
            "cont_flag": "0"
        }
        
        for attempt in range(3):  # Retry mechanism
            response = fyers.history(data=payload)
            if "candles" in response:
                break
            print(f"⚠️ Retry {attempt+1}/3 for {symbol}...")
            time.sleep(2)
        
        if "candles" in response:
            all_prices.extend([
                (datetime.datetime.fromtimestamp(d[0]), d[1], d[2], d[3], d[4], d[5])
                for d in response["candles"]
            ])
        else:
            print(f"❌ Failed to fetch data for {symbol}")
        
        start_date = batch_end + datetime.timedelta(days=1)
    
    return all_prices

# ✅ Insert Data into MySQL
def insert_into_db(data_vendor_id, symbol_id, stock_name, price_data):
    """Inserts historical stock data into MySQL."""
    now = datetime.datetime.now(datetime.timezone.utc)
    insert_values = [
        (data_vendor_id, symbol_id, stock_name, d[0], now, now, d[1], d[2], d[3], d[4], d[5])
        for d in price_data
    ]
    
    query = """
    INSERT INTO daily_price (
        data_vendor_id, symbol_id, stock_name, price_date, created_date,
        last_updated_date, open_price, high_price, low_price,
        close_price, volume
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        cur.executemany(query, insert_values)
        con.commit()
    except mdb.Error as e:
        print(f"❌ MySQL Error: {e}")

# ✅ Main Execution
if __name__ == "__main__":
    tickers = get_nifty50_tickers()
    total_tickers = len(tickers)
    start_date = datetime.date(2024, 1, 1)
    end_date = datetime.date(2024, 12, 31)
    
    for i, (symbol_id, ticker, stock_name) in enumerate(tickers):
        print(f"📊 Fetching data for {stock_name} ({ticker}) ({i+1}/{total_tickers})...")
        historical_data = fetch_historical_data(ticker, start_date, end_date)
        
        if historical_data:
            insert_into_db(1, symbol_id, stock_name, historical_data)
            print(f"✅ Inserted {len(historical_data)} rows for {stock_name} ({ticker})")
        else:
            print(f"⚠️ No data found for {stock_name} ({ticker})")
    
    print("🎉 Data fetching complete!")
