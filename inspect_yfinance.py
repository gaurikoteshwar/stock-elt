import yfinance as yf

ticker = "NVDA"
data = yf.download(ticker, period="5d")

print(data.head())
print(data.columns)
