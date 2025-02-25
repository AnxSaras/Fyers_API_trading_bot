import datetime
import pandas as pd
from fyers_apiv3 import fyersModel
from sqlalchemy import create_engine
import urllib.parse

password = urllib.parse.quote_plus("Apple@1234")  # Encodes special characters
SQL_SERVER_CONNECTION_STRING = f'mysql+pymysql://root:{password}@localhost/securities_master'

# 🔹 MySQL Connection String
#SQL_SERVER_CONNECTION_STRING = 'mysql+pymysql://sec_user:Apple@1234@localhost/securities_master'

# 🔹 Read API Credentials
app_id = open("fyers_appid.txt", 'r').read().strip()
access_token = open("fyers_token.txt", 'r').read().strip()

# 🔹 Initialize Fyers API
fyers = fyersModel.FyersModel(client_id=app_id, token=access_token)


def fetch_nifty50_from_fyers():
    """
    Fetch NIFTY 50 stock symbols from Fyers API.
    Returns a list of tuples ready for MySQL insertion.
    """
    now = datetime.datetime.now(datetime.UTC)

    # 🔹 Define NIFTY 50 symbols in NSE format (without ".NS")
    nifty_50_tickers = [
        "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR", "ITC",
        "SBIN", "BHARTIARTL", "KOTAKBANK", "LT", "HCLTECH", "ASIANPAINT", "AXISBANK",
        "MARUTI", "SUNPHARMA", "TITAN", "ULTRACEMCO", "BAJFINANCE", "WIPRO",
        "ONGC", "M&M", "POWERGRID", "NTPC", "NESTLEIND", "JSWSTEEL", "TECHM",
        "TATAMOTORS", "INDUSINDBK", "HDFCLIFE", "DRREDDY", "BAJAJFINSV", "HINDALCO",
        "GRASIM", "CIPLA", "ADANIPORTS", "SBILIFE", "TATASTEEL", "DIVISLAB",
        "BRITANNIA", "COALINDIA", "BPCL", "EICHERMOT", "UPL", "HEROMOTOCO",
        "APOLLOHOSP", "BAJAJ-AUTO", "SHREECEM"
    ]

    symbols = []

    for ticker in nifty_50_tickers:
        fyers_symbol = f"NSE:{ticker}-EQ"
        data = {"symbols": fyers_symbol}

        try:
            response = fyers.quotes(data)
            if 'd' in response and response['d']:
                stock_info = response['d'][0]

                name = stock_info.get("name", ticker)
                sector = stock_info.get("sector", "Unknown")
                industry = stock_info.get("industry", "Unknown")
                isin = stock_info.get("isin", "Unknown")

                symbols.append((1, ticker, name, sector, industry, isin, now, now))
            else:
                print(f"⚠️ No data found for {ticker}")

        except Exception as e:
            print(f"❌ Error fetching data for {ticker}: {e}")

    return symbols


def insert_nifty50_symbols(symbols):
    """Insert the NIFTY 50 symbols into the MySQL database."""
    engine = create_engine(SQL_SERVER_CONNECTION_STRING)

    column_names = ["exchange_id", "ticker", "name", "sector", "industry", "isin", "created_date", "last_updated_date"]
    df = pd.DataFrame(symbols, columns=column_names)

    try:
        df.to_sql(name='symbol', con=engine, if_exists='append', index=False)
        print(f"✅ Successfully inserted {len(symbols)} NIFTY 50 symbols into MySQL.")
    except Exception as e:
        print(f"❌ Error inserting data into MySQL: {e}")


if __name__ == "__main__":
    symbols = fetch_nifty50_from_fyers()
    if symbols:
        insert_nifty50_symbols(symbols)
    else:
        print("⚠️ No symbols were found.")
