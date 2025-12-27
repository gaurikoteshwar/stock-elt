from sqlalchemy import create_engine, text
import pandas as pd

# Postgres credentials
USERNAME = "postgres"
PASSWORD = "lily1234"
HOST = "localhost"
PORT = "5432"
DBNAME = "stock_etl"

# Create SQLAlchemy engine
engine = create_engine(f"postgresql+psycopg2://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}")

# Ensure staging schema exists
with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))

# Read raw stock data
df_raw = pd.read_sql("SELECT * FROM raw.stock_prices", engine)
print(f"Total raw rows: {len(df_raw)}")

# Get latest date in staging
latest_date_query = "SELECT MAX(date) FROM staging.stg_stock_prices"
latest_date = pd.read_sql(latest_date_query, engine).iloc[0,0]

# Filter for only new rows
if latest_date is not None:
    df_raw = df_raw[df_raw['date'] > latest_date]
    print(f"New rows to append: {len(df_raw)}")
else:
    print("No existing staging data found; loading full raw dataset.")

# Continue only if there are new rows
if not df_raw.empty:
    # Rename and select needed columns
    df_stg = df_raw.rename(columns={
        "open": "open_price",
        "high": "high_price",
        "low": "low_price",
        "close": "close_price"
    })
    df_stg = df_stg[["symbol", "date", "open_price", "high_price", "low_price", "close_price", "volume"]]

    # Append new rows to staging table
    df_stg.to_sql(
        "stg_stock_prices",
        engine,
        schema="staging",
        if_exists="append",  # append instead of replace
        index=False
    )
    print("Staging table updated successfully!")
else:
    print("No new data to append. Staging table is up to date.")

# Optional: Check staging table
df_check = pd.read_sql("SELECT * FROM staging.stg_stock_prices ORDER BY date DESC LIMIT 5", engine)
print(df_check)