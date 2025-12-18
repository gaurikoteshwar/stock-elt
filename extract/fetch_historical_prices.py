import yfinance as yf
import pandas as pd


def fetch_historical_prices(symbol, start_date, end_date):
    df = yf.download(
        symbol,
        start=start_date,
        end=end_date,
        auto_adjust=False
    )

    # Drop adjusted close
    if ("Adj Close", symbol) in df.columns:
        df = df.drop(columns=[("Adj Close", symbol)])

    # Flatten MultiIndex columns
    df.columns = [col[0].lower() for col in df.columns]

    # Move date index to column
    df = df.reset_index()

    # Rename Date -> date (important!)
    df = df.rename(columns={"Date": "date"})

    # Add symbol column
    df["symbol"] = symbol

    # Reorder columns to match raw table
    df = df[["symbol", "date", "open", "high", "low", "close", "volume"]]

    return df