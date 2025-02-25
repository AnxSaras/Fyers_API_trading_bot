import datetime
import mysql.connector as mdb
from fyers_apiv3 import fyersModel
import time

# ✅ Manually Enter Fyers API Credentials
app_id = "ISVT1FBO9P-100"
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJhcGkuZnllcnMuaW4iLCJpYXQiOjE3NDAzMDY2OTksImV4cCI6MTc0MDM1NzAzOSwibmJmIjoxNzQwMzA2Njk5LCJhdWQiOlsieDowIiwieDoxIiwieDoyIiwiZDoxIiwiZDoyIiwieDoxIiwieDowIl0sInN1YiI6ImFjY2Vzc190b2tlbiIsImF0X2hhc2giOiJnQUFBQUFCbnV2a0xhcEpzYUdJWjBoSkFEVlZDREdaRndvWXNJbi1KZDdzal9JQzZZU3o2NTBUcWx1d1JBNzJtTURjRHNkNTN5SXNBUGdDcUFyY2FpSllieENvY0JkbUZUWjBwWmJ1clVjTFRuQ19renhyOS1CRT0iLCJkaXNwbGF5X25hbWUiOiJBTktVUiBTQVJBU1dBVCIsIm9tcyI6IksxIiwiaHNtX2tleSI6ImZlODhjOTZiZTg2NTUwOTU4MDBlMTIwYjQxNzUwNmJiOThiN2I0MmY0OTQ2MGE2Y2YyMDNkMGE2IiwiaXNEZHBpRW5hYmxlZCI6Ik4iLCJpc010ZkVuYWJsZWQiOiJOIiwiZnlfaWQiOiJZQTQ2MjE4IiwiYXBwVHlwZSI6MTAwLCJwb2FfZmxhZyI6Ik4ifQ.MmZ1kgAsmZgjH2GbtOTTVJR7v50ZM_W6oZAGfoAqWPw"

# ✅ Initialize Fyers API
fyers = fyersModel.FyersModel(client_id=app_id, token=access_token, is_async=False)

# ✅ MySQL Database Connection
db_host = "localhost"
db_user = "sec_user"
db_pass = "Apple@1234"
db_name = "securities_master"

con = mdb.connect(host=db_host, user=db_user, password=db_pass, database=db_name)
cur = con.cursor()


# ✅ Fetch Tickers from Database
def obtain_list_of_db_tickers():
    """
    Fetches a list of ticker symbols & their names from the database.
    """
    cur.execute("SELECT id, ticker, name FROM symbol")  # ✅ Fetch stock name
    data = cur.fetchall()
    return [(d[0], d[1], d[2]) for d in data]


# ✅ Fetch Historical Data with Pagination (to avoid 1000-row limit)
def get_daily_historic_data_fyers(symbol):
    """
    Fetches historical data from Fyers API in smaller date chunks to avoid row limits.
    """
    today = datetime.datetime.today()
    start_date = today - datetime.timedelta(days=365)  # Fetch last 365 days

    all_prices = []
    batch_size = 100  # Fetch 100 days per request (to avoid Fyers 1000-row limit)

    while start_date < today:
        end_date = min(start_date + datetime.timedelta(days=batch_size), today)

        payload = {
            "symbol": f"NSE:{symbol}-EQ",
            "resolution": "D",
            "date_format": "1",
            "range_from": start_date.strftime('%Y-%m-%d'),
            "range_to": end_date.strftime('%Y-%m-%d'),
            "cont_flag": "0"
        }

        attempt = 0
        while attempt < 3:  # Retry up to 3 times in case of API failure
            response = fyers.history(data=payload)
            if "candles" in response:
                break
            print(f"⚠️ API Error for {symbol}, Retrying ({attempt + 1})...")
            time.sleep(2)  # Wait before retrying
            attempt += 1

        if "candles" in response:
            prices = [
                (datetime.datetime.fromtimestamp(d[0]), d[1], d[2], d[3], d[4], d[5])
                for d in response["candles"]
            ]
            all_prices.extend(prices)
        else:
            print(f"❌ Failed to fetch data for {symbol}: {response}")

        start_date = end_date + datetime.timedelta(days=1)  # Move to next batch

    return all_prices


# ✅ Insert Data into MySQL
def insert_daily_data_into_db(data_vendor_id, symbol_id, stock_name, daily_data):
    """
    Inserts daily stock price data into MySQL with the correct symbol ID.
    """
    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5, minutes=30)))  # ✅ IST Time

    daily_data = [
        (data_vendor_id, symbol_id, stock_name, d[0], now, now, d[1], d[2], d[3], d[4], d[5])
        for d in daily_data
    ]

    column_str = """data_vendor_id, symbol_id, stock_name, price_date, created_date,
                    last_updated_date, open_price, high_price, low_price,
                    close_price, volume"""
    insert_str = ("%s, " * 11)[:-2]
    final_str = f"INSERT INTO daily_price ({column_str}) VALUES ({insert_str})"

    try:
        cur.executemany(final_str, daily_data)
        con.commit()
    except mdb.Error as e:
        print(f"❌ MySQL Error: {e}")


# ✅ Main Execution
if __name__ == "__main__":
    tickers = obtain_list_of_db_tickers()
    total_tickers = len(tickers)

    for i, (symbol_id, ticker, stock_name) in enumerate(tickers):
        print(f"📊 Fetching data for {stock_name} ({ticker}) ({i + 1}/{total_tickers})...")
        historical_data = get_daily_historic_data_fyers(ticker)

        if historical_data:
            insert_daily_data_into_db(1, symbol_id, stock_name, historical_data)
            print(f"✅ Inserted {len(historical_data)} rows for {stock_name} ({ticker}) into database.")

    print("🎉 Successfully added Fyers historical pricing data to DB.")

# 0 4 * * * /usr/bin/python3 /Users/ankursaraswat/path/to/Nifty50_yr_data.py
# 0 4 * * * /Users/ankursaraswat/PycharmProjects/PythonProject/IbPy/IbPy/Nifty50_yr_data.py


