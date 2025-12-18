from extract.fetch_historical_prices import fetch_historical_prices
from load.load_postgres import load_stock_prices

# List of (start, end) date tuples
date_ranges = [
    ("2020-12-18", "2021-12-17"),
    ("2021-12-18", "2022-12-17"),
    ("2022-12-18", "2023-12-17"),
    ("2023-12-18", "2024-12-17"),
    ("2024-12-18", "2025-12-18"),
]

symbol = "NVDA"

for start, end in date_ranges:
    print(f"Fetching data: {start} → {end}")
    df = fetch_historical_prices(symbol, start, end)
    load_stock_prices(df)
    print(f"Loaded {len(df)} rows from {start} to {end}")