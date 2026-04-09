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
