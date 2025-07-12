import pandas as pd
import psycopg2

# DB connection
conn = psycopg2.connect(
    dbname="crypto_index",
    user="postgres",
    password="postgres",
    host="localhost",
    port=5432
)

tokens = pd.read_sql("SELECT DISTINCT token FROM ohlcv_data", conn)['token'].tolist()

for token in tokens:
    df = pd.read_sql(f"""
        SELECT timestamp, close
        FROM ohlcv_data
        WHERE token = '{token}'
        ORDER BY timestamp
    """, conn)

    df['sma10'] = df['close'].rolling(window=10).mean()

    insert_data = [
        (token, row['timestamp'], 10, row['sma10'])
        for _, row in df.iterrows() if pd.notnull(row['sma10'])
    ]

    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO sma_data (token, timestamp, sma_window, sma_value)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, insert_data)
        conn.commit()
