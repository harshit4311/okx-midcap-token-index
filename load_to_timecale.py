import os
import pandas as pd
import psycopg2
from tqdm import tqdm

DATA_DIR = "/Users/harshit/Downloads/Research-Commons-Quant/okx-midcap-token-index/ohlcv_csvs"
TOKENS = ["AIDOGE_USDT", "VELO_USDT", "MERL_USDT", "BIGTIME_USDT", "SUSHI_USDT"]

# DB connection settings
conn = psycopg2.connect(
    dbname="crypto_index",
    user="postgres",
    password="postgres",  # change if needed
    host="localhost",
    port=5432
)
cur = conn.cursor()

insert_query = """
    INSERT INTO ohlcv_data (token, timestamp, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (token, timestamp) DO NOTHING;
"""

for token in TOKENS:
    file_path = os.path.join(DATA_DIR, f"{token}.csv")
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        continue

    df = pd.read_csv(file_path)
    if "timestamp" not in df.columns or "close" not in df.columns:
        print(f"Invalid format in {token}")
        continue

    df['timestamp'] = pd.to_datetime(df['timestamp'])

    for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Inserting {token}"):
        cur.execute(insert_query, (
            token,
            row['timestamp'],
            row.get('open', None),
            row.get('high', None),
            row.get('low', None),
            row.get('close', None),
            row.get('volume', None)
        ))
    conn.commit()

cur.close()
conn.close()
print("✅ Done inserting all tokens.")
