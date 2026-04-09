import os
import pandas as pd

from config import BRONZE_DIR, SILVER_DIR, LOOKBACK_ROWS
from watermark import load_watermark, save_watermark
from loaders import load_new_files, load_silver_lookback
from cleaning import clean_prices
from indicators import compute_indicators
from tables import build_stocks_master, build_price_history, build_technical
from storage import upsert_partition, save_hive_partitioned_incremental
from analytics import analytics

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
