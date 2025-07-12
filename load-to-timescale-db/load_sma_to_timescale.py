import pandas as pd
from sqlalchemy import create_engine

# 📦 Connect to your PostgreSQL DB
engine = create_engine("postgresql://postgres:postgres@localhost:5432/crypto_index")

# 🎯 SMA windows to compute
sma_windows = [25, 100]

# 🎯 Get all tokens
tokens_df = pd.read_sql("SELECT DISTINCT token FROM ohlcv_data;", engine)
tokens = tokens_df['token'].tolist()

# 🛠 Collect all new SMA rows
new_sma_rows = []

for token in tokens:
    # Load price data
    df = pd.read_sql(f"""
        SELECT timestamp, close
        FROM ohlcv_data
        WHERE token = '{token}'
        ORDER BY timestamp
    """, engine)

    for window in sma_windows:
        df[f'sma_{window}'] = df['close'].rolling(window=window).mean()

        sma_df = df[['timestamp', f'sma_{window}']].dropna().copy()
        sma_df['token'] = token
        sma_df['sma_window'] = window
        sma_df.rename(columns={f'sma_{window}': 'sma_value'}, inplace=True)

        new_sma_rows.append(sma_df[['token', 'timestamp', 'sma_window', 'sma_value']])

# 👇 Combine and insert
final_sma_df = pd.concat(new_sma_rows)

# 📤 Insert into sma_data
final_sma_df.to_sql("sma_data", engine, if_exists='append', index=False)

print("✅ SMA25 and SMA100 values added to sma_data.")
