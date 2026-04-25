"""
Silver → Gold consolidation script
Reads price_history parquet files (structured as silver/price_history/<ticker>/<year>/*.parquet)
and outputs one consolidated parquet file per ticker into the gold output directory.
"""

import os
import pandas as pd
from pathlib import Path


# ── Configuration ─────────────────────────────────────────────────────────────

SILVER_PRICE_HISTORY_DIR = Path("data/silver/lake/price_history")
GOLD_OUTPUT_DIR          = Path("data/knime/input")

# ──────────────────────────────────────────────────────────────────────────────


def consolidate_ticker(ticker_path: Path, output_dir: Path, stocks_master: pd.DataFrame) -> None:
    """
    Reads all yearly parquet files for a single ticker, concatenates them,
    sorts by date, and writes a single parquet file to output_dir.
    """
    ticker = ticker_path.name
    yearly_files = sorted(ticker_path.rglob("*.parquet"))   # recursively finds all parquet files

    if not yearly_files:
        print(f"  [SKIP] {ticker} — no parquet files found")
        return

    # Read and concatenate all years
    frames = []
    for f in yearly_files:
        try:
            df = pd.read_parquet(f)
            frames.append(df)
        except Exception as e:
            print(f"  [WARN] Could not read {f}: {e}")

    if not frames:
        print(f"  [SKIP] {ticker} — all files failed to read")
        return

    combined = pd.concat(frames, ignore_index=True)

    # Ensure there is a date column and sort chronologically
    date_col = _detect_date_column(combined)
    if date_col:
        combined[date_col] = pd.to_datetime(combined[date_col])
        combined = combined.sort_values(date_col).reset_index(drop=True)
    else:
        print(f"  [WARN] {ticker} — no date column detected, skipping sort")

    # Add ticker column if not already present
    if "ticker" not in combined.columns:
        combined.insert(0, "ticker", ticker)

    # Remove duplicate rows (same date appearing in two yearly files)
    if date_col:
        before = len(combined)
        combined = combined.drop_duplicates(subset=[date_col]).reset_index(drop=True)
        dupes = before - len(combined)
        if dupes:
            print(f"  [INFO] {ticker} — removed {dupes} duplicate row(s)")

    # Add sector and industry from stocks_master
    combined = combined.merge(stocks_master, on="ticker", how="left")

    # Write output
    output_path = output_dir / f"{ticker}.parquet"
    combined.to_parquet(output_path, index=False)
    print(f"  [OK]   {ticker} — {len(combined)} rows → {output_path}")


def _detect_date_column(df: pd.DataFrame) -> str | None:
    """Returns the name of the first column that looks like a date."""
    candidates = ["date", "Date", "datetime", "Datetime", "timestamp", "Timestamp"]
    for col in candidates:
        if col in df.columns:
            return col
    # Fallback: first column whose name contains 'date'
    for col in df.columns:
        if "date" in col.lower():
            return col
    return None


STOCKS_MASTER_PATH = Path("../DataLake/Silver/stocks_master/data.parquet")


def main():
    if not SILVER_PRICE_HISTORY_DIR.exists():
        raise FileNotFoundError(f"Silver directory not found: {SILVER_PRICE_HISTORY_DIR}")

    GOLD_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load sector/industry lookup table
    stocks_master = pd.read_parquet(STOCKS_MASTER_PATH)[["ticker", "sector", "industry"]]

    ticker_dirs = [p for p in sorted(SILVER_PRICE_HISTORY_DIR.iterdir()) if p.is_dir()]

    if not ticker_dirs:
        print("No ticker directories found. Check your SILVER_PRICE_HISTORY_DIR path.")
        return

    print(f"Found {len(ticker_dirs)} ticker(s) in {SILVER_PRICE_HISTORY_DIR}")
    print(f"Output directory: {GOLD_OUTPUT_DIR}\n")

    for ticker_path in ticker_dirs:
        consolidate_ticker(ticker_path, GOLD_OUTPUT_DIR, stocks_master)

    print(f"\nDone. {len(ticker_dirs)} ticker(s) processed.")


if __name__ == "__main__":
    main()