import os
import pandas as pd
import numpy as np
from glob import glob
from datetime import datetime
from sklearn.cluster import KMeans

# ==============================================================================
# CONFIG
# ==============================================================================

BRONZE_DIR = r"..\DataLake\Bronze"
SILVER_DIR = "output_silver"

os.makedirs(SILVER_DIR, exist_ok=True)

# ==============================================================================
# LOAD
# ==============================================================================

def load_all_prices():
    files = glob(f"{BRONZE_DIR}/ticker/*/*_prices_*.parquet")
    dfs = [pd.read_parquet(f) for f in files]
    return pd.concat(dfs, ignore_index=True)

def load_all_info():
    files = glob(f"{BRONZE_DIR}/ticker/*/*_info_*.parquet")
    dfs = [pd.read_parquet(f) for f in files]
    return pd.concat(dfs, ignore_index=True)

# ==============================================================================
# TYPE FIX
# ==============================================================================

def fix_types(df):
    """
    Functions:
        1. Convert prices and volumes to numbers
        2. Standardize time format
    """
    
    df = df.copy()

    numeric_cols = ["Open", "High", "Low", "Close", "Volume"]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            # errors="coerce": If it can't be translated, mark it as blank to avoid crashing the entire system.

    # Unify time zones to avoid confusion
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True)
        df["Date"] = df["Date"].dt.tz_convert(None)

    return df

# ==============================================================================
# CLEANING
# ==============================================================================

def clean_prices(df):
    df = df.copy()

    df = fix_types(df)
    # Discard blank data
    df = df.dropna(subset=["Close", "Date"])
    df = df.drop_duplicates(subset=["Date", "ticker"])
    df = df.sort_values(["ticker", "Date"])

    # OHLC Validation: Checks if the price logic is reasonable
    # Open = Opening price, High = Highest price, Low = Lowest price, Close = Closing price
    # Logic: The highest price must be greater than or equal to the opening price, closing price, and lowest price.
    df = df[
        (df["High"] >= df["Low"]) &
        (df["High"] >= df["Open"]) &
        (df["High"] >= df["Close"]) &
        (df["Low"] <= df["Open"]) &
        (df["Low"] <= df["Close"])
    ]

    # IQR Outlier Detection: Finding "Outrageous" Prices
    # Principle: A normal price should be within three times the width of the middle 50% range.
    # Prices outside this range are likely due to input errors.
    q1 = df["Close"].quantile(0.25) # 25th percentile (25% of prices are below this)
    q3 = df["Close"].quantile(0.75) # 75th percentile (75% of prices are below this)
    iqr = q3 - q1 # The middle 50% price range

    # Only retain data within the normal range (use 3 times instead of 1.5 times, because stock price fluctuations are inherently large)
    df = df[(df["Close"] >= q1 - 3*iqr) & (df["Close"] <= q3 + 3*iqr)]

    return df

# ==============================================================================
# INDICATORS
# ==============================================================================

def compute_indicators(df):
    """
    Calculate technical analysis indicators to help predict price trends.
    """
    df = df.copy()

    # SMA (Simple Moving Average): The average of the closing prices over the last N days.
    # sma_20 = 20-day moving average (short-term trend), sma_50 = 50-day moving average (medium-term trend).
    # Application: A short-term moving average crossing above a long-term moving average = a buy signal (golden cross).
    df["sma_20"] = df.groupby("ticker")["Close"].transform(lambda x: x.rolling(20).mean())
    df["sma_50"] = df.groupby("ticker")["Close"].transform(lambda x: x.rolling(50).mean())


    # RSI (Relative Strength Index): Measures which side is more aggressive – buyers or sellers.
    # Calculation: Compare the average increase vs. average decrease over the past 14 days.
    # RSI > 70: Overbought (Too many buyers, likely to fall) ⚠️
    # RSI < 30: Oversold (Too many sellers, likely to rise) ✅
    delta = df.groupby("ticker")["Close"].diff()

    gain = delta.clip(lower=0) # Only keep the days with price increases
    loss = -delta.clip(upper=0) # Only keep the days that are down

    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()

    rs = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1 + rs))
    
    # MACD (Trend Reversal Detector): Short-term trend - Long-term trend
    # EMA (Exponential Moving Average): Focuses more on recent price than SMA
    # MACD > 0: Short-term stronger than long-term, bullish market (suitable for holding)
    # MACD < 0: Short-term weaker than long-term, bearish market (consider selling)
    ema12 = df.groupby("ticker")["Close"].transform(lambda x: x.ewm(span=12).mean())
    ema26 = df.groupby("ticker")["Close"].transform(lambda x: x.ewm(span=26).mean())

    df["macd"] = ema12 - ema26

    # Bollinger Bands: Normal price fluctuation range
    # Principle: 95% of data falls within the mean ± 2 standard deviations (statistical normal distribution)
    # Application: Price touching the upper band may indicate overbought (sell), touching the lower band may indicate oversold (buy)
    std = df.groupby("ticker")["Close"].transform(lambda x: x.rolling(20).std())

    df["bb_upper"] = df["sma_20"] + 2 * std
    df["bb_lower"] = df["sma_20"] - 2 * std

    # ATR (Average True Range): Measures the volatility of price movements
    # TR = Today's High - Low
    # ATR = Average TR over the past 14 days
    # Uses: Setting stop-loss levels (entry price - 2 × ATR)
    df["tr"] = df["High"] - df["Low"]
    df["atr"] = df.groupby("ticker")["tr"].transform(lambda x: x.rolling(14).mean())

    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df[numeric_cols] = df[numeric_cols].round(2)

    return df

# ==============================================================================
# TABLES
# ==============================================================================

def build_stocks_master(df_info):
    df = df_info[["ticker", "longName", "sector", "industry"]].drop_duplicates()
    df.columns = ["ticker", "company_name", "sector", "industry"]
    return df

def build_price_history(df):
    df = df.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume"
    })

    df["adj_close"] = df["close"]
    df["year"] = df["date"].dt.year 

    # 2位小数保留（交易价格字段）
    num_cols = ["open", "high", "low", "close", "adj_close"]
    df[num_cols] = df[num_cols].round(2)

    return df[["ticker","date","year","open","high","low","close","volume","adj_close"]]

def build_technical(df):
    """
    Technical Analysis Log - Records all calculated indicators
    For easy access to "What is Apple's RSI today?
    """
    
    df["year"] = df["Date"].dt.year

    return df[[
        "ticker","Date","year","sma_20","sma_50","atr","rsi","macd","bb_upper","bb_lower"
    ]].rename(columns={"Date":"date"})

# ==============================================================================
# HIVE PARTITION SAVE
# ==============================================================================

def save_hive_partitioned(df, base_path):
    for (ticker, year), sub_df in df.groupby(["ticker", "year"]):
        path = os.path.join(
            base_path,
            f"ticker={ticker}",
            f"year={year}"
        )
        os.makedirs(path, exist_ok=True)

        file_path = os.path.join(path, "data.parquet")
        sub_df.to_parquet(file_path, index=False)

        print(f"✅ {file_path} ({len(sub_df)} rows)")

# ==============================================================================
# ANALYTICS
# ==============================================================================

def analytics(df):
    """
    Simple analysis examples:
    1. Descriptive statistics: Closing price distribution for each stock
    2. Volatility: Standard deviation of returns (measures risk)
    3. K-Means clustering: Automatically divides stocks into 3 categories
    """
    print("\n📊 Descriptive Stats")
    print(df.groupby("ticker")["close"].describe())

    print("\n📉 Volatility")
    df["returns"] = df.groupby("ticker")["close"].pct_change()
    print(df.groupby("ticker")["returns"].std())

    print("\n📊 Clustering")
    features = df.groupby("ticker")[["close","volume"]].mean()

    kmeans = KMeans(n_clusters=3, random_state=42).fit(features)
    features["cluster"] = kmeans.labels_

    print(features)

# ==============================================================================
# MAIN
# ==============================================================================

print("="*60)
print("Silver Layer Processing (Hive Partitioned)")
print("="*60)

df_prices = load_all_prices()
df_info   = load_all_info()

df_prices = clean_prices(df_prices)
df_ind    = compute_indicators(df_prices)

stocks_master = build_stocks_master(df_info)
price_history = build_price_history(df_prices)
technical     = build_technical(df_ind)

# =========================
# SAVE
# =========================

print("\nSaving Hive-partitioned tables...")

# stocks_master
os.makedirs(f"{SILVER_DIR}/stocks_master", exist_ok=True)
stocks_master.to_parquet(f"{SILVER_DIR}/stocks_master/data.parquet", index=False)

# price_history（ticker + year）
save_hive_partitioned(
    price_history,
    os.path.join(SILVER_DIR, "price_history")
)

# technical_indicators（ticker + year）
save_hive_partitioned(
    technical,
    os.path.join(SILVER_DIR, "technical_indicators")
)

print("\n✅ Silver Layer DONE")

analytics(price_history)