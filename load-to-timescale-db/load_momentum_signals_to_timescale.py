import pandas as pd
import psycopg2
from datetime import timedelta

# DB connection
conn = psycopg2.connect(
    dbname="crypto_index",
    user="postgres",
    password="postgres",
    host="localhost",
    port=5432
)

# Load all tokens
tokens = pd.read_sql("SELECT DISTINCT token FROM ohlcv_data", conn)['token'].tolist()

momentum_window = 14
signal_records = []

for token in tokens:
    df = pd.read_sql(f"""
        SELECT timestamp, close
        FROM ohlcv_data
        WHERE token = '{token}'
        ORDER BY timestamp
    """, conn)

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['momentum'] = df['close'].pct_change(momentum_window)

    # Create buy/sell signal
    df['signal'] = None
    df.loc[(df['momentum'] > 0) & (df['momentum'].shift(1) <= 0), 'signal'] = 'momentum_buy'
    df.loc[(df['momentum'] < 0) & (df['momentum'].shift(1) >= 0), 'signal'] = 'momentum_sell'

    signals = df.dropna(subset=['signal'])

    for _, row in signals.iterrows():
        signal_records.append((token, row['timestamp'], row['signal'], f'momentum_{momentum_window}d'))

# Insert into DB
import psycopg2.extras
with conn:
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO momentum_signals (token, timestamp, signal, strategy_name)
            VALUES %s
            ON CONFLICT (token, timestamp, strategy_name) DO NOTHING
        """, signal_records)
