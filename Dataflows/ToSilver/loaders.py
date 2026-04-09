import os
import pandas as pd
from glob import glob
from config import BRONZE_DIR, SILVER_DIR

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
