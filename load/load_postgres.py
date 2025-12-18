# This file will load NVDA historical data into Postgres
import psycopg2
from psycopg2.extras import execute_values


def load_stock_prices(df):
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="stock_etl",
        user="postgres",
        password="lily1234"
    )

    insert_sql = """
        INSERT INTO raw.stock_prices (
            symbol, date, open, high, low, close, volume
        )
        VALUES %s
        ON CONFLICT (symbol, date) DO NOTHING;
    """

    records = [
        (
            row.symbol,
            row.date,
            row.open,
            row.high,
            row.low,
            row.close,
            int(row.volume)
        )
        for row in df.itertuples(index=False)
    ]

    with conn.cursor() as cur:
        execute_values(cur, insert_sql, records)
        conn.commit()

    conn.close()