import ccxt
import pandas as pd
import os
import time
from datetime import datetime, timedelta

# Set constants
TOKENS = ["ZRX/USDT", "ZRO/USDT", "ZIL/USDT", "UMA/USDT", "WAXP/USDT"]
DAYS_BACK = 365
OUTPUT_DIR = "/Users/harshit/Downloads/Research-Commons-Quant/okx-midcap-token-index/dataframes"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Setup OKX
okx = ccxt.okx()
okx.load_markets()

def fetch_ohlcv(symbol, days_back=365):
    since = okx.milliseconds() - days_back * 24 * 60 * 60 * 1000
    try:
        data = okx.fetch_ohlcv(symbol, timeframe='1d', since=since, limit=days_back)
        df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        return df
    except Exception as e:
        print(f"❌ Error fetching {symbol}: {e}")
        return None

# Fetch and save for each token
for symbol in TOKENS:
    print(f"📊 Fetching: {symbol}")
    df = fetch_ohlcv(symbol, DAYS_BACK)
    if df is not None and not df.empty:
        base = symbol.split("/")[0]
        file_path = os.path.join(OUTPUT_DIR, f"{base}.csv")
        df.to_csv(file_path, index=False)
        print(f"✅ Saved {base} to {file_path}")
    time.sleep(1.2)  # Avoid rate limit

print("\n🎯 Done fetching all token data.")
