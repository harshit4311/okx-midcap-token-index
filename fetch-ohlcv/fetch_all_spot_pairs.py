import ccxt
import os
import time
import pandas as pd
from datetime import datetime, timedelta

# Setup
okx = ccxt.okx()
okx.load_markets()
symbols = [s for s in okx.symbols if s.endswith("/USDT")]

# Constants
DAYS_BACK = 365
TIMEFRAME = '1d'
MS_IN_DAY = 24 * 60 * 60 * 1000
OUTPUT_DIR = "/Users/harshit/Downloads/Research-Commons-Quant/okx-midcap-token-index/dataframes"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Helper to fetch all 1y data in chunks
def fetch_full_ohlcv(symbol, since_ts):
    all_data = []
    while True:
        try:
            data = okx.fetch_ohlcv(symbol, timeframe=TIMEFRAME, since=since_ts, limit=300)
            if not data:
                break
            all_data.extend(data)
            last_ts = data[-1][0]
            # Move ahead by 1 candle
            since_ts = last_ts + MS_IN_DAY
            if len(data) < 300:
                break
            time.sleep(1.1)
        except Exception as e:
            print(f"❌ Error fetching {symbol}: {e}")
            break
    return all_data

# Main loop
now = okx.milliseconds()
since = now - DAYS_BACK * MS_IN_DAY

print(f"📥 Fetching full OHLCV for {len(symbols)} tokens...")
for symbol in symbols:
    print(f"→ {symbol}")
    ohlcv = fetch_full_ohlcv(symbol, since_ts=since)

    if ohlcv:
        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df = df.drop_duplicates(subset="timestamp").reset_index(drop=True)

        if len(df) >= 360:  # Accept if mostly complete
            token_name = symbol.replace("/", "_").replace(":", "_")
            file_path = os.path.join(OUTPUT_DIR, f"{token_name}.csv")
            df.to_csv(file_path, index=False)
            print(f"✅ Saved {token_name}.csv ({len(df)} rows)")
        else:
            print(f"⚠️ Skipped {symbol}: Only {len(df)} days of data")

    time.sleep(1.2)

print("✅ Done fetching.")
