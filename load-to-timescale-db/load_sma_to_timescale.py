import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

DATA_DIR = "./ohlcv_csvs"
SMA_WINDOWS = [20, 50]

# Connect to TimescaleDB
conn = psycopg2.connect(
    dbname="crypto_index",
    user="postgres",
    password="postgres",  # update if needed
    host="localhost",
    port=5432
)
cur = conn.cursor()

for file in os.listdir(DATA_DIR):
    if not file.endswith(".csv"):
        continue

    token = file.replace(".csv", "")
    df = pd.read_csv(os.path.join(DATA_DIR, file))
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.sort_values("timestamp", inplace=True)

    for window in SMA_WINDOWS:
        df[f"sma_{window}"] = df["close"].rolling(window=window).mean()

        sma_rows = df[["timestamp", f"sma_{window}"]].dropna()
        data = [
            (token, row["timestamp"], window, row[f"sma_{window}"])
            for _, row in sma_rows.iterrows()
        ]

        insert_query = """
            INSERT INTO sma_data (token, timestamp, sma_window, sma_value)
            VALUES %s
            ON CONFLICT (token, timestamp, sma_window) DO NOTHING;
        """
        execute_values(cur, insert_query, data)
        print(f"Inserted SMA{window} for {token}")

conn.commit()
cur.close()
conn.close()
print("✅ All SMA data inserted.")
