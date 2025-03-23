import time
import requests
import pandas as pd
import mysql.connector as mdb
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from fyers_api import fyersModel
from datetime import datetime
from openpyxl import Workbook
import os
import logging
from collections import deque

import requests

# ✅ Configure Telegram Bot
TELEGRAM_BOT_TOKEN = "AAEh17Ee9pgjcCv0Skuyq3kxx5kjiEfi_JI"
TELEGRAM_CHAT_ID = "5016132683"

def send_telegram_alert(message):
    """Send alert to Telegram"""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            logging.info(f"✅ Telegram alert sent: {message}")
        else:
            logging.error(f"❌ Telegram alert failed: {response.text}")
    except Exception as e:
        logging.error(f"❌ Telegram API Error: {e}")



# ✅ Set up logging for debugging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

#def logEntryFunc(self, timestamp, category, data):
    #print(f"[{timestamp}] {category}: {data}")  # Print to console instead of writing to file

# FYERS API Credentials
client_id = "ISVT1FBO9P-100"
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJhcGkuZnllcnMuaW4iLCJpYXQiOjE3NDI3MTE3MDgsImV4cCI6MTc0Mjc3NjIwOCwibmJmIjoxNzQyNzExNzA4LCJhdWQiOlsieDowIiwieDoxIiwieDoyIiwiZDoxIiwiZDoyIiwieDoxIiwieDowIl0sInN1YiI6ImFjY2Vzc190b2tlbiIsImF0X2hhc2giOiJnQUFBQUFCbjM2dWNUT2JfWWlOdUFWdTRuRnB4Mjh3Uzkza09VbVZDOGp2Z2NEVmx4clFsSGFLUUl6MGJ1ZnU1T2Z4WnBvcGpub254aUFZMmFxS2Q5NElBLVZKWWpndEJmVlZiQnozT2pOMEN3X3lCNkM1SGMzTT0iLCJkaXNwbGF5X25hbWUiOiJBTktVUiBTQVJBU1dBVCIsIm9tcyI6IksxIiwiaHNtX2tleSI6ImZlODhjOTZiZTg2NTUwOTU4MDBlMTIwYjQxNzUwNmJiOThiN2I0MmY0OTQ2MGE2Y2YyMDNkMGE2IiwiaXNEZHBpRW5hYmxlZCI6Ik4iLCJpc010ZkVuYWJsZWQiOiJOIiwiZnlfaWQiOiJZQTQ2MjE4IiwiYXBwVHlwZSI6MTAwLCJwb2FfZmxhZyI6Ik4ifQ.VL3LnO5DsaA3V_Lc6XCC0OezlLjGqqRmbr7mRGzz1cg"

# Database Configuration
db_config = {
    "host": "localhost",
    "user": "Algo_Trading",
    "password": "Apple@1331",
    "database": "Live_Trading"
}

# ✅ Connect to MySQL
def connect_db():
    try:
        con = mdb.connect(**db_config)
        logging.info("✅ Database connected!")
        return con
    except mdb.Error as e:
        logging.error(f"❌ DB Connection Error: {e}")
        return None

# ✅ Initialize Fyers API
fyers = fyersModel.FyersModel(client_id=client_id, token=access_token, is_async=False, log_path="")
logging.info("✅ Fyers API Initialized!")

# ✅ Fetch Available Funds
def get_available_funds():
    try:
        funds = fyers.funds()
        if funds and 'fund_limit' in funds:
            return float(funds["fund_limit"][0]["equityAmount"])
    except Exception as e:
        logging.error(f"❌ Error fetching funds: {e}")
    return None

# ✅ Fetch Live Market Data
def get_market_data(symbols):
    """Fetch market data for multiple symbols in a single API call."""
    try:
        symbols_str = ",".join([f"NSE:{symbol}-EQ" for symbol in symbols])
        response = fyers.quotes({"symbols": symbols_str})

        if response.get("s") == "ok" and "d" in response:
            return {
                item["n"].split(":")[-1]: {
                    "open": item["v"].get("open_price", 0),
                    "high": item["v"].get("high_price", 0),
                    "low": item["v"].get("low_price", 0),
                    "close": item["v"].get("lp", 0),
                    "volume": item["v"].get("volume", 0),
                }
                for item in response["d"]
            }
    except Exception as e:
        logging.error(f"❌ Error fetching market data: {e}")
    return {}

# ✅ Historical Prices Dictionary
historical_prices = {}

def update_closing_price(symbol, new_close, max_size=14):
    """Update closing prices using deque for efficiency."""
    if symbol not in historical_prices:
        historical_prices[symbol] = deque(maxlen=max_size)
    historical_prices[symbol].append(new_close)


# ✅ Fetch Historical Data from Fyers API
def fetch_historical_data(symbol, days=100):
    try:
        payload = {
            "symbol": f"NSE:{symbol}-EQ",
            "resolution": "D",  # Daily Data
            "date_format": "1",
            "range_from": (datetime.now() - pd.Timedelta(days=days)).strftime("%Y-%m-%d"),
            "range_to": datetime.now().strftime("%Y-%m-%d"),
            "cont_flag": "1"
        }
        
        response = fyers.history(payload)
        if response.get("s") == "ok":
            df = pd.DataFrame(response["candles"], columns=["timestamp", "open", "high", "low", "close", "volume"])
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit='s')
            df.set_index("timestamp", inplace=True)
            return df
        else:
            logging.error(f"❌ Error fetching historical data for {symbol}: {response}")
            return None
    except Exception as e:
        logging.error(f"❌ Exception fetching historical data for {symbol}: {e}")
        return None

# ✅ Update Latest Closing Price in Historical Data
def update_closing_price(symbol, new_close, max_size=14):
    historical_prices.setdefault(symbol, []).append(new_close)
    if len(historical_prices[symbol]) > max_size:
        historical_prices[symbol].pop(0)


# ✅ Calculate RSI with Period 14 and Smoothing 3
def calculate_rsi(df, period=14, smoothing=3):
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    avg_gain = gain.ewm(alpha=1/smoothing, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/smoothing, min_periods=period).mean()
    
    rs = avg_gain / (avg_loss + 1e-10)
    df["RSI"] = 100 - (100 / (1 + rs))
    
    return df


# ✅ Generate Buy/Sell Signals
def generate_signals(df):
    df["RSI_Previous"] = df["RSI"].shift(1)
    df["Buy_Signal"] = (df["RSI"] > 30) & (df["RSI_Previous"] <= 30)
    df["Sell_Signal"] = (df["RSI"] < 70) & (df["RSI_Previous"] >= 70)
    return df


# ✅ Place Bracket Order
def place_bracket_order(symbol, buy_price, sl=1.5, target=5):
    try:
        # ✅ Fetch available funds
        available_funds = get_available_funds()
        if available_funds is None or available_funds <= 0:
            logging.warning(f"⚠️ Insufficient funds for {symbol}. Order cannot be placed.")
            return None

        # ✅ Dynamically calculate qty
        qty = int(available_funds / buy_price)
        if qty <= 0:
            logging.warning(f"⚠️ Calculated quantity is zero or negative for {symbol}. Check available funds.")
            return None

        # ✅ Define order parameters
        order = {
            "symbol": f"NSE:{symbol}-EQ",
            "qty": qty,
            "type": 2,  # Limit order
            "side": 1,  # Buy
            "productType": "BO",  # Bracket Order
            "limitPrice": round(buy_price, 2),
            "stopPrice": round(buy_price - (buy_price * sl / 100), 2),  # Stop Loss
            "takeProfit": round(buy_price + (buy_price * target / 100), 2),  # Target
            "validity": "DAY",
        }

        # ✅ Place order via Fyers API
        response = fyers.place_order(order)

        # ✅ Log the response
        logging.info(f"📌 Order placed for {symbol}: {response}")

        return response
    except Exception as e:
        logging.error(f"❌ Order Placement Error for {symbol}: {e}")
        return None

# Save Trade Data to MySQL & Excel
def log_trade_to_db(symbol, entry_price, exit_price, profit, balance):
    try:
        con = connect_db()
        if con:
            cursor = con.cursor()
            cursor.execute("""
                INSERT INTO trade_log (symbol, entry_price, exit_price, profit, balance, timestamp)
                VALUES (%s, %s, %s, %s, %s, NOW())
            """, (symbol, entry_price, exit_price, profit, balance))
            con.commit()
            cursor.close()
            con.close()
            print(f"✅ Trade logged: {symbol} | Entry: {entry_price} | Exit: {exit_price} | Profit: {profit}")
    except mdb.Error as e:
        print(f"❌ DB Error: {e}")


# Save Trade Data to Excel
def save_to_excel(trades_log, trade_signals, summary):
    if not trades_log:
        print("⚠️ No trades to save.")
        return
    output_dir = "/Users/ankursaraswat/Fyers_API_trading_bot/STRATEGY_3_MW_RSI/"
    filename = os.path.join(output_dir, f"Live_MW_RSI_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
    with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
        df_trades = pd.DataFrame(trades_log, columns=[
            "Ticker", "Entry Date", "Initial Funds", "Buy Qty", "Entry Price", 
            "Exit Date", "Sell Qty", "Exit Price", "Gross P&L", "Gross P&L %",
            "Fees", "Net P&L", "Net P&L %"
        ])
        df_trades.to_excel(writer, sheet_name="Trades Executed", index=False)

        df_signals = pd.DataFrame(trade_signals, columns=["Ticker", "Date", "Signal Type", "Price"])
        df_signals.to_excel(writer, sheet_name="Trade Signals", index=False)

        total_net_pnl = df_trades["Net P&L"].sum()  # ✅ Calculate total net P&L
        summary[-1][3] = round(total_net_pnl, 2)  # ✅ Corrected assignment

        df_summary = pd.DataFrame(summary, columns=["Total Signals", "Total Buy Trades", "Total Sell Trades", "Net P&L"])
        df_summary.to_excel(writer, sheet_name="Summary", index=False)  # ✅ Write summary sheet

        # Apply Formatting
        workbook = writer.book
        worksheet = writer.sheets["Trades Executed"]
        format_green = workbook.add_format({"bg_color": "#C6EFCE", "font_color": "#006100", "bold": True})
        format_red = workbook.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006", "bold": True})
        header_format = workbook.add_format({"bold": True, "bg_color": "#D3D3D3", "border": 1})

        # ✅ Apply Conditional Formatting for Net P&L (L column)
        worksheet.conditional_format("L2:L1000", {"type": "cell", "criteria": ">", "value": 0, "format": format_green})
        worksheet.conditional_format("L2:L1000", {"type": "cell", "criteria": "<", "value": 0, "format": format_red})

        # ✅ Format Headers
        for col_num, value in enumerate(df_trades.columns.values):
            worksheet.write(0, col_num, value, header_format)

        # ✅ Add Grand Total Row
        grand_total_row = len(df_trades) + 2  # ✅ Fixed row position
        worksheet.write_row(
            grand_total_row, 0, ["Grand Total", "", "", "", "", "", "", "", 
                                 df_trades["Gross P&L"].sum(), "", df_trades["Fees"].sum(), df_trades["Net P&L"].sum(), ""], 
            workbook.add_format({"bold": True, "bg_color": "#FFFF99", "border": 1})
        )

    # ✅ Also save as CSV
    csv_filename = filename.replace(".xlsx", ".csv")
    df_trades.to_csv(csv_filename, index=False)

    print(f"✅ Excel file saved: {filename}")
    print(f"✅ CSV file saved: {csv_filename}")


# ✅ Auto Trading Logic
def auto_trade(symbols):
    max_retries = 5  # Stop after 5 failed attempts
    retry_count = 0

    while retry_count < max_retries:
        try:
            for symbol in symbols:
                logging.info(f"🔍 Checking {symbol} at {datetime.now()}...")
                
                df = fetch_historical_data(symbol)
                if df is None or df.empty:
                    logging.warning(f"⚠️ No valid market data for {symbol}")
                    continue
                
                df = calculate_rsi(df)
                df = generate_signals(df)

                # Extract signals
                buy_signal = df.iloc[-1].get("Buy_Signal", None)
                sell_signal = df.iloc[-1].get("Sell_Signal", None)
                close_price = df.iloc[-1].get("close", None)

                # Send Telegram Alerts
                if buy_signal and close_price:
                    alert_message = f"🚀 BUY Signal for {symbol} at ₹{close_price}"
                    logging.info(alert_message)
                    send_telegram_alert(alert_message)

                elif sell_signal and close_price:
                    alert_message = f"⚠️ SELL Signal for {symbol} at ₹{close_price}"
                    logging.info(alert_message)
                    send_telegram_alert(alert_message)

            time.sleep(5)  # Sleep to avoid API rate limits

        except Exception as e:
            logging.error(f"❌ Error in auto_trade: {str(e)}")
            retry_count += 1

            if retry_count >= max_retries:
                logging.critical("❌ Maximum retries reached. Stopping auto_trade.")
                break  # Stop execution if too many failures

# Run Trading Bot
if __name__ == "__main__":
   stock_list = [
        "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
        "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BEL", "BPCL",
        "BHARTIARTL", "BRITANNIA", "CIPLA", "COALINDIA", "DRREDDY",
        "EICHERMOT", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE",
        "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK", "INDUSINDBK",
        "INFY", "ITC", "JSWSTEEL", "KOTAKBANK", "LT",
        "M&M", "MARUTI", "NESTLEIND", "NTPC", "ONGC",
        "POWERGRID", "RELIANCE", "SBILIFE", "SHRIRAMFIN", "SBIN",
        "SUNPHARMA", "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL",
        "TECHM", "TITAN", "TRENT", "ULTRACEMCO", "WIPRO"
]
auto_trade(stock_list)
