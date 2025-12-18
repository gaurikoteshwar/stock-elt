from extract.fetch_historical_prices import fetch_historical_prices
from load.load_postgres import load_stock_prices
from logger import logger
import psycopg2
from datetime import datetime, timedelta
import pandas as pd

symbol = "NVDA"

# -----------------------------
# Step 1: Get last date in DB
# -----------------------------
def get_last_date(symbol):
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="stock_etl",
        user="postgres",
        password="lily1234"
    )
    cur = conn.cursor()
    cur.execute("SELECT MAX(date) FROM raw.stock_prices WHERE symbol=%s;", (symbol,))
    last_date = cur.fetchone()[0]
    conn.close()
    return last_date

# -----------------------------
# Step 2: Determine start/end
# -----------------------------
last_date = get_last_date(symbol)
start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")  # next day
end_date = datetime.today().strftime("%Y-%m-%d")  # today

logger.info(f"Updating {symbol} from {start_date} to {end_date}")

# -----------------------------
# Step 3: Fetch data
# -----------------------------
try:
    df = fetch_historical_prices(symbol, start_date, end_date)
except Exception as e:
    logger.error(f"Failed to fetch data: {e}")
    df = pd.DataFrame()  # prevent crashing

# -----------------------------
# Step 4: Load + validation
# -----------------------------
if not df.empty:
    # Load to Postgres
    try:
        load_stock_prices(df)
        logger.info(f"Loaded {len(df)} new rows")
    except Exception as e:
        logger.error(f"Failed to load data: {e}")

    # Check for missing dates
    expected_dates = pd.date_range(start=last_date + timedelta(days=1), end=datetime.today())
    missing_dates = set(expected_dates.date) - set(df['date'])
    if missing_dates:
        logger.warning(f"Missing price data for these dates: {sorted(missing_dates)}")
else:
    logger.info("No new price data available yet")