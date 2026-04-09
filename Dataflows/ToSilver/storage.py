import os
import pandas as pd

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
