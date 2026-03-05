import yfinance as yf
import pandas as pd

# Exemple avec Apple
# ticker = "AAPL"

# Télécharger les données
# data = yf.download(ticker, start="2024-01-01", end="2024-12-31")

# print(data.head())
# print(data.columns)

'''
ticker_obj = yf.Ticker("AAPL")
info = ticker_obj.info
print("Sector:", info.get("sector"))
print("Industry:", info.get("industry"))
print("Market Cap:", info.get("marketCap"))
'''

tickers = ["AAPL", "MSFT", "GOOGL"]
data = yf.download(tickers, start='2022-01-01')
data.to_csv("test.csv")

yf.Ticker("AAPL").info
yf.Ticker("MSFT").info
yf.Ticker("GOOGL").info

'''
ticker_obj = yf.Ticker("AAPL")
calendar = ticker_obj.calendar
print(calendar)
'''
