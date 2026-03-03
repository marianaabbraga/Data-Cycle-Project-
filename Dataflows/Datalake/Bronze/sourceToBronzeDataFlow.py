import os
import pandas as pd
import yfinance as yf
from datetime import datetime

# ==============================
# CONFIGURATION
# ==============================

PORTFOLIO = ['AAPL', 'MSFT', 'NVDA']
START_DATE = "2022-01-01"
END_DATE = None  # None = today

BASE_FOLDER = "test" 

# ==============================
# SETUP
# ==============================

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

if not os.path.exists(BASE_FOLDER):
    os.makedirs(BASE_FOLDER)

# ==============================
# EXTRACTION
# ==============================

for ticker_symbol in PORTFOLIO:
    print(f"Downloading data for {ticker_symbol}...")
    
    ticker = yf.Ticker(ticker_symbol)
    
    # ---- PRICE HISTORY ----
    df_prices = ticker.history(start=START_DATE, end=END_DATE)
    
    if df_prices.empty:
        print(f"No data found for {ticker_symbol}")
        continue

    df_prices.reset_index(inplace=True)
    
    # Add extraction metadata
    df_prices["ticker"] = ticker_symbol
    df_prices["extraction_timestamp"] = datetime.now()
    df_prices["source"] = "yfinance"

    # Create ticker folder
    ticker_folder = os.path.join(BASE_FOLDER, ticker_symbol)
    os.makedirs(ticker_folder, exist_ok=True)

    # Save as Parquet
    prices_filename = f"{ticker_symbol}_prices_{timestamp}.parquet"
    df_prices.to_parquet(os.path.join(ticker_folder, prices_filename), index=False)

    # ---- METADATA ----
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
    metadata_df.to_parquet(os.path.join(ticker_folder, metadata_filename), index=False)

    print(f"{ticker_symbol} saved successfully.")

print("Extraction completed.")