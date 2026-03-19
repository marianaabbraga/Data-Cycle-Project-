import pandas as pd

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
