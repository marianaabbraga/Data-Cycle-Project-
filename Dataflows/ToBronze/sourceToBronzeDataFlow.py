import os
import pandas as pd
import yfinance as yf
from datetime import datetime

# ==============================
# CONFIGURATION
# ==============================

PORTFOLIO = ['AAPL', 'MSFT', 'NVDA']
START_DATE = "2016-01-01"
END_DATE = None  # None = today
BASE_FOLDER = "Tests"

# ==============================
# FUNCTIONS
# ==============================

def extract_prices(ticker, ticker_symbol, start_date, end_date, folder, timestamp):
    
    df_prices = ticker.history(start=start_date, end=end_date)

    if df_prices.empty:
        print(f"No price data found for {ticker_symbol}")
        return

    df_prices.reset_index(inplace=True)

    df_prices["ticker"] = ticker_symbol
    df_prices["extraction_timestamp"] = datetime.now()
    df_prices["source"] = "yfinance"

    prices_filename = f"{ticker_symbol}_prices_{timestamp}.parquet"
    df_prices.to_parquet(os.path.join(folder, prices_filename), index=False)


def extract_metadata(ticker, ticker_symbol, folder, timestamp):

    info = ticker.info

    metadata = {
        "ticker": ticker_symbol,
        "company_name": info.get("longName"),
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "currency": info.get("currency"),
        "exchange": info.get("exchange"),
        "asset_type": info.get("quoteType"),
        "market_cap": info.get("marketCap"),
        "country": info.get("country"),
        "extraction_timestamp": datetime.now().isoformat(),
        "source": "yfinance"
    }

    metadata_df = pd.DataFrame([metadata])

    metadata_filename = f"{ticker_symbol}_metadata_{timestamp}.parquet"
    metadata_df.to_parquet(os.path.join(folder, metadata_filename), index=False)


# ==============================
# SETUP
# ==============================

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

os.makedirs(BASE_FOLDER, exist_ok=True)

# ==============================
# EXTRACTION LOOP
# ==============================

for ticker_symbol in PORTFOLIO:

    print(f"Downloading data for {ticker_symbol}...")

    ticker = yf.Ticker(ticker_symbol)

    ticker_folder = os.path.join(BASE_FOLDER, ticker_symbol)
    os.makedirs(ticker_folder, exist_ok=True)

    extract_prices(
        ticker,
        ticker_symbol,
        START_DATE,
        END_DATE,
        ticker_folder,
        timestamp
    )

    extract_metadata(
        ticker,
        ticker_symbol,
        ticker_folder,
        timestamp
    )

    print(f"{ticker_symbol} saved successfully.")

print("Extraction completed.")