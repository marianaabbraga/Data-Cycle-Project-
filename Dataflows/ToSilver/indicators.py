import numpy as np

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

    # rs = avg_gain / avg_loss
    # df["rsi"] = 100 - (100 / (1 + rs))

    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["rsi"] = 100 - (100 / (1 + rs))
    df["rsi"] = df["rsi"].fillna(50) 

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
