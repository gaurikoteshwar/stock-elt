from extract.fetch_historical_prices import fetch_historical_prices

df = fetch_historical_prices(
    symbol="NVDA",
    start_date="2020-01-01",
    end_date="2020-01-10"
)

print(df.head())
print(df.columns)