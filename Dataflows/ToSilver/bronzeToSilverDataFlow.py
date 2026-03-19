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
SILVER_DIR = r"..\DataLake\Silver"
# BRONZE_DIR = "Bronze"
# SILVER_DIR = "Silver"

# [INCREMENTAL] Path to the watermark file that records the last successful run.
# Silver reads this to know which Bronze files are new; Bronze writes it after
# each extraction so Silver always has a reliable cutoff timestamp.
# WATERMARK_FILE = r"..\DataLake\_last_silver_run.txt"
WATERMARK_FILE = "_last_silver_run.txt"

# [INCREMENTAL] Lookback window fed to compute_indicators alongside new rows.
# Must be >= the longest rolling window used below (sma_50 = 50 days).
# 60 gives a small safety buffer on top of that.
LOOKBACK_ROWS = 60

os.makedirs(SILVER_DIR, exist_ok=True)

# ==============================================================================
# WATERMARK
# ==============================================================================

# [INCREMENTAL] 
# load_watermark() → returns 0.0 on the very first run so every existing
#                    Bronze file is treated as "new" (full back-fill once,
#                    incremental from the second run onwards).
# save_watermark() → called ONLY after all Silver writes succeed, so a
#                    mid-run crash leaves the watermark unchanged and the
#                    next run will safely retry the same batch.

def load_watermark() -> float:
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE) as f:
            ts_str = f.read().strip()
        return datetime.strptime(ts_str, "%Y-%m-%d_%H-%M-%S").timestamp()
    print(" No watermark found — treating all files as new (first run).")
    return 0.0


def save_watermark() -> None:
    ts_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    with open(WATERMARK_FILE, "w") as f:
        f.write(ts_str)
    print(f" Watermark updated → {ts_str}")

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

# [INCREMENTAL] Incremental versions of the two loaders above.
# Instead of globbing every file, they filter by mtime > last_ts so only
# Bronze files written since the last Silver run are loaded.

def load_new_files(pattern: str, last_ts: float) -> pd.DataFrame:
    all_files = glob(pattern)
    # Keep only files whose modification time is newer than the watermark
    new_files = [f for f in all_files if os.path.getmtime(f) > last_ts]
    if not new_files:
        return pd.DataFrame()
    print(f"  {len(new_files)} new file(s) matched: {pattern.split('/')[-1]}")
    dfs = [pd.read_parquet(f) for f in new_files]
    return pd.concat(dfs, ignore_index=True)


def load_silver_lookback(table: str, ticker: str, n_rows: int) -> pd.DataFrame:
    # [INCREMENTAL] Pull the most recent n_rows from an existing Silver partition.
    # This gives compute_indicators enough historical context so that rolling
    # windows (e.g. sma_50) produce valid values for the very first new row,
    # even when only a handful of new Bronze rows arrived today.
    base = os.path.join(SILVER_DIR, table, f"ticker={ticker}")
    if not os.path.exists(base):
        return pd.DataFrame()
    files = glob(os.path.join(base, "*/data.parquet"))
    if not files:
        return pd.DataFrame()
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    date_col = "date" if "date" in df.columns else "Date"
    df = df.sort_values(date_col).tail(n_rows)
    return df

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

    avg_gain = gain.groupby(df["ticker"]).transform(lambda x: x.rolling(14).mean())
    avg_loss = loss.groupby(df["ticker"]).transform(lambda x: x.rolling(14).mean())

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

# [INCREMENTAL] Upsert helpers — used instead of save_hive_partitioned in the
# incremental main flow below.
#
# upsert_partition(): merges new rows into one existing parquet file on disk.
#   - File doesn't exist yet → write directly (first run, same as before).
#   - File exists → concat old + new, deduplicate on keys, overwrite.
#   keep="last": if yfinance later corrects a past price the re-extracted
#   value wins. Change to keep="first" to preserve the original value instead.
#
# save_hive_partitioned_incremental(): same ticker/year loop as
#   save_hive_partitioned, but calls upsert_partition per file so only the
#   partitions that appear in today's batch are touched.

def upsert_partition(new_df: pd.DataFrame, file_path: str, keys: list) -> None:
    if os.path.exists(file_path):
        old_df   = pd.read_parquet(file_path)
        combined = pd.concat([old_df, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=keys, keep="last")
    else:
        combined = new_df.copy()
    combined.to_parquet(file_path, index=False)
    print(f"  {file_path}  (+{len(new_df)} rows → total {len(combined)})")


def save_hive_partitioned_incremental(df: pd.DataFrame, base_path: str, keys: list) -> None:
    for (ticker, year), sub_df in df.groupby(["ticker", "year"]):
        path = os.path.join(
            base_path,
            f"ticker={ticker}",
            f"year={year}"
        )
        os.makedirs(path, exist_ok=True)
        file_path = os.path.join(path, "data.parquet")
        upsert_partition(sub_df, file_path, keys)

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

# [INCREMENTAL] Load watermark from last successful run.
# On first run this returns 0.0, which causes load_new_files to match every
# existing Bronze file — effectively a full back-fill.
last_ts = load_watermark()

# [INCREMENTAL] Replace load_all_prices / load_all_info with filtered loaders
# that only read Bronze files written since last_ts.
df_prices = load_new_files(f"{BRONZE_DIR}/ticker/*/*_prices_*.parquet", last_ts)
df_info   = load_new_files(f"{BRONZE_DIR}/ticker/*/*_info_*.parquet",   last_ts)

if df_prices.empty:
    print("\n⏭️  No new price data. Silver is already up to date.")
else:
    df_prices = clean_prices(df_prices)

    # [INCREMENTAL] Per-ticker lookback + indicator loop.
    # Problem: compute_indicators uses rolling windows (sma_50 needs 50 rows).
    # If today's Bronze batch only has 1 new row, the rolling window has no
    # history and produces NaN for every indicator.
    # Solution: for each ticker, prepend the last LOOKBACK_ROWS rows from the
    # existing Silver price_history partition before calling compute_indicators,
    # then strip those lookback rows out again before writing to Silver.
    ind_parts = []

    for ticker, grp_new in df_prices.groupby("ticker"):
        lookback_raw = load_silver_lookback("price_history", ticker, LOOKBACK_ROWS)

        if not lookback_raw.empty:
            # Re-align Silver column names back to the raw format that
            # compute_indicators expects (Date / Open / High / Low / Close …)
            lookback_raw = lookback_raw.rename(columns={
                "date": "Date", "open": "Open", "high": "High",
                "low":  "Low",  "close": "Close", "volume": "Volume"
            })
            shared_cols = grp_new.columns.intersection(lookback_raw.columns)
            combined = pd.concat(
                [lookback_raw[shared_cols], grp_new],
                ignore_index=True
            ).sort_values("Date")
        else:
            combined = grp_new.sort_values("Date")

        with_ind = compute_indicators(combined)

        # Drop the lookback rows — only write the genuinely new dates to Silver
        new_dates = grp_new["Date"].values
        only_new  = with_ind[with_ind["Date"].isin(new_dates)]
        ind_parts.append(only_new)

    df_ind = pd.concat(ind_parts, ignore_index=True)

    stocks_master = build_stocks_master(df_info) if not df_info.empty else None
    price_history = build_price_history(df_prices)
    technical     = build_technical(df_ind)

    # =========================
    # SAVE
    # =========================

    print("\nSaving Hive-partitioned tables...")

    # stocks_master
    # [INCREMENTAL] Upsert on ticker key instead of a blind overwrite, so
    # existing tickers not present in today's info batch are preserved.
    if stocks_master is not None:
        os.makedirs(f"{SILVER_DIR}/stocks_master", exist_ok=True)
        upsert_partition(
            stocks_master,
            f"{SILVER_DIR}/stocks_master/data.parquet",
            keys=["ticker"]
        )

    # price_history（ticker + year）
    # [INCREMENTAL] Replaced save_hive_partitioned → save_hive_partitioned_incremental
    # so each partition file is upserted (not overwritten) on key (ticker, date).
    save_hive_partitioned_incremental(
        price_history,
        os.path.join(SILVER_DIR, "price_history"),
        keys=["ticker", "date"]
    )

    # technical_indicators（ticker + year）
    save_hive_partitioned_incremental(
        technical,
        os.path.join(SILVER_DIR, "technical_indicators"),
        keys=["ticker", "date"]
    )

    # [INCREMENTAL] Watermark is saved AFTER all writes succeed.
    # If anything above raises an exception the watermark stays at its old
    # value, so the next run retries the same batch — no data is silently lost.
    save_watermark()

    print("\n✅ Silver Layer DONE")

    analytics(price_history)