from sqlalchemy import create_engine, text
import pandas as pd

USERNAME = "postgres"
PASSWORD = "lily1234"
HOST = "localhost"
PORT = "5432"
DBNAME = "stock_etl"

engine = create_engine(f"postgresql+psycopg2://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}")


# Ensure staging schema exists
with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))

df_raw = pd.read_sql("SELECT * FROM raw.stock_prices", engine)
print(df_raw.head())

df_stg = df_raw.rename(columns={
    "open": "open_price",
    "high": "high_price",
    "low": "low_price",
    "close": "close_price"
})

df_stg = df_stg[["symbol", "date", "open_price", "high_price", "low_price", "close_price", "volume"]]
print(df_stg.head())

df_stg.to_sql(
    "stg_stock_prices",
    engine,
    schema="staging",
    if_exists="replace",
    index=False
)

print("Staging table loaded successfully!")
df_check = pd.read_sql("SELECT * FROM staging.stg_stock_prices LIMIT 5", engine)
print(df_check)