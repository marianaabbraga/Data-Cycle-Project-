"""
Bronze Layer — Raw Data Ingestion
"""

import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import sys


sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

# ==============================================================================
# CONFIGURATION
# ==============================================================================

PORTFOLIO = ["AAPL", "MSFT", "NVDA"]
ETF_PORTFOLIO = ["SPY", "QQQ"]
MARKETS = ["us_market", "eu_market"]
START_DATE = "2016-01-01"
OUTPUT_DIR = os.getenv("BRONZE_OUTPUT_DIR", r"..\DataLake\Bronze")

SUBFOLDERS = ["ticker", "market", "sector_industry", "equity", "funds"]

for folder in SUBFOLDERS:
    os.makedirs(os.path.join(OUTPUT_DIR, folder), exist_ok=True)

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

print("=" * 60)
print("Bronze Layer — Raw Data Ingestion")
print(f"Run timestamp : {timestamp}")
print("=" * 60)

# [INCREMENTAL] Price watermark — tracks the last trading date successfully
# written to Bronze so each run only pulls genuinely new rows from yfinance.
#
# File layout (plain text, one line):  YYYY-MM-DD
#
# How it interacts with START_DATE:
#   • First run ever  → watermark file doesn't exist → fall back to START_DATE
#                       (full history back-fill, same as before)
#   • Subsequent runs → read watermark date, pass (watermark + 1 day) as
#                       `start` to ticker.history() so yfinance only returns
#                       rows after the last known date
#   • Same-day re-run → yfinance returns 0 new rows → skip file creation
#                       entirely, no duplicate files produced
#
# The watermark is written once after the prices loop completes (not per-ticker
# inside the loop) so that last_price_date used for filtering stays fixed at
# the value loaded at the start of the run for every ticker.

PRICE_WATERMARK_FILE = os.path.join(OUTPUT_DIR, "_last_price_date.txt")

def load_price_watermark() -> str:
    # [INCREMENTAL] Return the last successfully extracted trading date
    # (YYYY-MM-DD string) or fall back to START_DATE on the very first run.
    if os.path.exists(PRICE_WATERMARK_FILE):
        with open(PRICE_WATERMARK_FILE) as f:
            return f.read().strip()
    print(f" No price watermark found — starting from START_DATE ({START_DATE}).")
    return START_DATE

def save_price_watermark(date_str: str) -> None:
    # [INCREMENTAL] Persist the most recent trading date seen in this run.
    # Called once per ticker after a successful non-empty pull.
    with open(PRICE_WATERMARK_FILE, "w") as f:
        f.write(date_str)

# [INCREMENTAL] Content-hash deduplication for snapshot data.
#
# calendar / info / sector / industry / equity / funds don't have a row-level
# date we can filter on — they are snapshots that change rarely (quarterly
# at most).  Writing a new timestamped file every run fills Bronze with
# identical copies that differ only in their filename and extraction_timestamp.
#
# Strategy: before writing, compute an MD5 of the DataFrame content (excluding
# the extraction_timestamp column which changes every run).  Compare it to the
# hash stored from the previous run.  If identical → skip.  If different →
# write the file and update the stored hash.
#
# Hash files live in OUTPUT_DIR/_hashes/ and are named after the logical key
# of the data they guard, e.g. "ticker_AAPL_calendar.md5".

import hashlib

HASH_DIR = os.path.join(OUTPUT_DIR, "_hashes")
os.makedirs(HASH_DIR, exist_ok=True)

def _df_hash(df: pd.DataFrame) -> str:
    # [INCREMENTAL] Stable MD5 over DataFrame content.
    # extraction_timestamp is excluded so it doesn't make every run look new.
    cols = [c for c in df.columns if c != "extraction_timestamp"]
    return hashlib.md5(
        df[cols].astype(str).to_csv(index=False).encode()
    ).hexdigest()

def _hash_path(key: str) -> str:
    # [INCREMENTAL] Path to the .md5 file for a given logical key.
    return os.path.join(HASH_DIR, f"{key}.md5")

def changed(df: pd.DataFrame, key: str) -> bool:
    # [INCREMENTAL] Return True if df content differs from the last saved hash.
    # Always returns True on the first run (no hash file yet).
    path = _hash_path(key)
    new_hash = _df_hash(df)
    if os.path.exists(path):
        with open(path) as f:
            if f.read().strip() == new_hash:
                return False
    return True

def save_hash(df: pd.DataFrame, key: str) -> None:
    # [INCREMENTAL] Persist the content hash after a successful file write.
    with open(_hash_path(key), "w") as f:
        f.write(_df_hash(df))

# ==============================================================================
# HELPER
# ==============================================================================

def save_raw(df, subfolder, filename, key=None):
    try:
        df = df.copy()
    except:
        df = pd.DataFrame(df)
        
    try:
        df.to_parquet("/tmp/test.parquet")
    except:
        df = df.astype(str)

    if key:
        path = os.path.join(OUTPUT_DIR, subfolder, key)
        os.makedirs(path, exist_ok=True)
        full_path = os.path.join(path, filename)
    else:
        full_path = os.path.join(OUTPUT_DIR, subfolder, filename)

    df.to_parquet(full_path, index=False)
    print(f"    ✅ {full_path} ({len(df)} rows × {len(df.columns)} cols)")
    return full_path


# ==============================================================================
# US-4 : TICKER & CALENDAR DATA
# ==============================================================================

print("\n" + "=" * 60)
print("US-4 : Ticker Data")
print("=" * 60)

# [INCREMENTAL] Determine the start date for this run's price pull.
# last_price_date is the most recent trading date already in Bronze (or
# START_DATE on first run).
#
# We pass last_price_date itself (not +1 day) as `start` to yfinance so the
# response always overlaps by one day with what we already have.  After
# fetching, we drop any row whose date <= last_price_date before saving —
# this keeps the file creation logic simple while handling the case where
# today's session hasn't closed yet (yfinance returns nothing newer than
# last_price_date → after dedup the batch is empty → no file is written).
last_price_date = load_price_watermark()
incremental_start = last_price_date

print(f"  Last price date in Bronze : {last_price_date}")
print(f"  Pulling prices from       : {incremental_start}")

# [INCREMENTAL] latest_seen collects the furthest date reached across all
# tickers during this run.  It is intentionally kept separate from
# last_price_date so that the filter condition inside the loop always compares
# against the watermark value that was loaded at the start of the run — not a
# value that was mutated by a previous ticker's successful pull.
# save_price_watermark() is called once after the loop ends.
latest_seen = last_price_date

for symbol in PORTFOLIO:
    print(f"\n[{symbol}]")
    ticker = yf.Ticker(symbol)

    # Prices
    try:
        # [INCREMENTAL] Use incremental_start instead of the fixed START_DATE.
        # yfinance is asked from last_price_date onwards (inclusive) so the
        # response always contains at least the last known day.
        # We then strip any row whose date <= last_price_date to keep only the
        # genuinely new trading sessions.
        # If today's session hasn't closed yet, nothing newer than
        # last_price_date comes back → new_prices is empty → no file written.
        df_prices = ticker.history(start=incremental_start)
        if not df_prices.empty:
            df_prices.reset_index(inplace=True)
            # [INCREMENTAL] Drop the overlap day(s) — keep only dates strictly
            # after the watermark.
            df_prices = df_prices[
                pd.to_datetime(df_prices["Date"]).dt.date >
                datetime.strptime(last_price_date, "%Y-%m-%d").date()
            ]
        if not df_prices.empty:
            df_prices["ticker"] = symbol
            df_prices["extraction_timestamp"] = datetime.now().isoformat()
            save_raw(df_prices, "ticker", f"{symbol}_prices_{timestamp}.parquet", symbol)

            # [INCREMENTAL] Advance latest_seen to the latest date in this
            # batch so the next run starts from the day after.
            # We take the max across all tickers so the watermark reflects
            # the furthest date any ticker reached.
            latest_date = pd.to_datetime(df_prices["Date"]).max().strftime("%Y-%m-%d")
            if latest_date > latest_seen:
                latest_seen = latest_date
        else:
            # [INCREMENTAL] Empty result means no new trading days since the
            # last run — skip silently instead of writing a zero-row file.
            print(f" No new price data for {symbol} since {last_price_date}.")
    except Exception as e:
        print(f"⚠️ prices failed for {symbol}: {e}")

    # Calendar
    try:
        calendar = ticker.calendar
        if calendar:
            df_cal = pd.DataFrame([calendar]) if isinstance(calendar, dict) else calendar.reset_index()
            df_cal["ticker"] = symbol
            df_cal["extraction_timestamp"] = datetime.now().isoformat()
            # [INCREMENTAL] Only write if content changed since last run.
            if changed(df_cal, f"ticker_{symbol}_calendar"):
                save_raw(df_cal, "ticker", f"{symbol}_calendar_{timestamp}.parquet", symbol)
                save_hash(df_cal, f"ticker_{symbol}_calendar")
            else:
                print(f" calendar unchanged for {symbol}, skipping.")
    except Exception as e:
        print(f"⚠️ calendar failed for {symbol}: {e}")

    # Info
    try:
        info = ticker.info
        df_info = pd.DataFrame([info])
        df_info["ticker"] = symbol
        df_info["extraction_timestamp"] = datetime.now().isoformat()
        # [INCREMENTAL] Only write if content changed since last run.
        if changed(df_info, f"ticker_{symbol}_info"):
            save_raw(df_info, "ticker", f"{symbol}_info_{timestamp}.parquet", symbol)
            save_hash(df_info, f"ticker_{symbol}_info")
        else:
            print(f" info unchanged for {symbol}, skipping.")
    except Exception as e:
        print(f"⚠️ info failed for {symbol}: {e}")

# [INCREMENTAL] Write the watermark once after all tickers have been processed.
# Doing this outside the loop ensures last_price_date stays fixed at the
# value loaded at run start, so every ticker's filter sees the same cutoff.
if latest_seen > last_price_date:
    save_price_watermark(latest_seen)

print("\nUS-4 complete.")


# ==============================================================================
# US-5 : MARKET DATA
# ==============================================================================

print("\n" + "=" * 60)
print("US-5 : Market Data")
print("=" * 60)

for market_id in MARKETS:
    print(f"\n[{market_id}]")

    try:
        mkt = yf.Market(market_id)
    except Exception as e:
        print(f"⚠️ Market init failed: {e}")
        continue

    # STATUS
    try:
        status = mkt.status
        if status:
            if isinstance(status, dict):
                df_status = pd.DataFrame([status])
            else:
                df_status = pd.DataFrame([{"status": str(status)}])

            df_status["market"] = market_id
            df_status["extraction_timestamp"] = datetime.now().isoformat()

            # [INCREMENTAL] Only write if content changed since last run.
            if changed(df_status, f"market_{market_id}_status"):
                save_raw(df_status, "market",
                         f"{market_id}_status_{timestamp}.parquet", market_id)
                save_hash(df_status, f"market_{market_id}_status")
            else:
                print(f" status unchanged for {market_id}, skipping.")

    except Exception as e:
        print(f"⚠️ status failed for {market_id}: {e}")

    # SUMMARY
    try:
        summary = mkt.summary

        rows = []
        if isinstance(summary, dict):
            for category, content in summary.items():
                if isinstance(content, dict):
                    rows.append({"market": market_id, "category": category, **content})
                elif isinstance(content, list):
                    for item in content:
                        if isinstance(item, dict):
                            rows.append({"market": market_id, "category": category, **item})

        if rows:
            df_summary = pd.DataFrame(rows).astype(str)
            df_summary["extraction_timestamp"] = datetime.now().isoformat()

            # [INCREMENTAL] Only write if content changed since last run.
            if changed(df_summary, f"market_{market_id}_summary"):
                save_raw(df_summary, "market",
                         f"{market_id}_summary_{timestamp}.parquet", market_id)
                save_hash(df_summary, f"market_{market_id}_summary")
            else:
                print(f" summary unchanged for {market_id}, skipping.")

    except Exception as e:
        print(f"⚠️ summary failed (yfinance bug): {e}")

print("\nUS-5 complete.")


# ==============================================================================
# US-6 : SECTOR & INDUSTRY DATA
# ==============================================================================

print("\n" + "=" * 60)
print("US-6 : Sector & Industry Data")
print("=" * 60)

seen_sectors = set()
seen_industries = set()

for symbol in PORTFOLIO:
    try:
        info = yf.Ticker(symbol).info
        sector_key = info.get("sectorKey")
        industry_key = info.get("industryKey")
    except Exception as e:
        print(f"⚠️ info read failed: {e}")
        continue

    # Sector
    if sector_key and sector_key not in seen_sectors:
        seen_sectors.add(sector_key)
        try:
            sec = yf.Sector(sector_key)

            overview = {"sector_key": sector_key,
                        "name": getattr(sec, "name", None),
                        "extraction_timestamp": datetime.now().isoformat()}

            if isinstance(sec.overview, dict):
                overview.update(sec.overview)

            df_sec = pd.DataFrame([overview])
            # [INCREMENTAL] Only write if content changed since last run.
            if changed(df_sec, f"sector_{sector_key}"):
                save_raw(df_sec, "sector_industry",
                         f"sector_{sector_key}_{timestamp}.parquet", symbol)
                save_hash(df_sec, f"sector_{sector_key}")
            else:
                print(f" sector {sector_key} unchanged, skipping.")

        except Exception as e:
            print(f"⚠️ sector failed: {e}")

    # Industry
    if industry_key and industry_key not in seen_industries:
        seen_industries.add(industry_key)
        try:
            ind = yf.Industry(industry_key)

            overview = {"industry_key": industry_key,
                        "name": getattr(ind, "name", None),
                        "extraction_timestamp": datetime.now().isoformat()}

            if isinstance(ind.overview, dict):
                overview.update(ind.overview)

            df_ind = pd.DataFrame([overview])
            # [INCREMENTAL] Only write if content changed since last run.
            if changed(df_ind, f"industry_{industry_key}"):
                save_raw(df_ind, "sector_industry",
                         f"industry_{industry_key}_{timestamp}.parquet", symbol)
                save_hash(df_ind, f"industry_{industry_key}")
            else:
                print(f" industry {industry_key} unchanged, skipping.")

        except Exception as e:
            print(f"⚠️ industry failed: {e}")

print("\nUS-6 complete.")


# ==============================================================================
# US-7 : EQUITY & FUNDS DATA
# ==============================================================================

print("\n" + "=" * 60)
print("US-7 : Equity & Funds Data")
print("=" * 60)

# EQUITY
for symbol in PORTFOLIO:
    print(f"\n[Equity: {symbol}]")
    ticker = yf.Ticker(symbol)

    for attr in ["income_stmt", "balance_sheet", "cashflow"]:
        try:
            df = getattr(ticker, attr)
            if isinstance(df, pd.DataFrame) and not df.empty:
                df = df.T.reset_index()
                df["ticker"] = symbol
                df["extraction_timestamp"] = datetime.now().isoformat()
                # [INCREMENTAL] Only write if content changed since last run.
                if changed(df, f"equity_{symbol}_{attr}"):
                    save_raw(df, "equity", f"{symbol}_{attr}_{timestamp}.parquet", symbol)
                    save_hash(df, f"equity_{symbol}_{attr}")
                else:
                    print(f" {attr} unchanged for {symbol}, skipping.")
        except Exception as e:
            print(f"⚠️ {attr} failed: {e}")


# FUNDS
for symbol in ETF_PORTFOLIO:
    print(f"\n[Fund: {symbol}]")
    ticker = yf.Ticker(symbol)

    try:
        fd = ticker.funds_data

        summary = {
            "ticker": symbol,
            "description": getattr(fd, "description", None),
            "extraction_timestamp": datetime.now().isoformat()
        }

        if isinstance(fd.fund_overview, dict):
            summary.update(fd.fund_overview)

        if isinstance(fd.fund_operations, dict):
            summary.update(fd.fund_operations)

        if isinstance(fd.asset_classes, dict):
            summary.update(fd.asset_classes)

        df_fund = pd.DataFrame([summary])
        # [INCREMENTAL] Only write if content changed since last run.
        if changed(df_fund, f"funds_{symbol}_overview"):
            save_raw(df_fund, "funds",
                     f"{symbol}_overview_{timestamp}.parquet", symbol)
            save_hash(df_fund, f"funds_{symbol}_overview")
        else:
            print(f" funds overview unchanged for {symbol}, skipping.")

    except Exception as e:
        print(f"⚠️ funds_data failed: {e}")

print("\nUS-7 complete.")


# ==============================================================================
# SUMMARY
# ==============================================================================

print("\n" + "=" * 60)
print("✅ Bronze extraction complete.")
print("=" * 60)

# ── Manifest generation (SHA-256 hashes of all output files) ─────────────────
import json as _json

def _sha256(filepath):
    h = hashlib.sha256()
    with open(filepath, "rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def _generate_manifest(output_dir, stage):
    from glob import glob as _glob
    files = sorted(_glob(os.path.join(output_dir, "**", "*.parquet"), recursive=True))
    entries = []
    for fp in files:
        rel = os.path.relpath(fp, output_dir)
        entries.append({"path": rel, "sha256": _sha256(fp)})
    composite = "\n".join(f"{e['path']}:{e['sha256']}" for e in entries)
    manifest_hash = hashlib.sha256(composite.encode()).hexdigest()
    manifest = {
        "stage": stage,
        "generated_at": datetime.now().isoformat(),
        "output_dir": os.path.abspath(output_dir),
        "manifest_hash": manifest_hash,
        "files": entries,
    }
    manifest_path = os.path.join(output_dir, f"{stage}_manifest.json")
    with open(manifest_path, "w") as fh:
        _json.dump(manifest, fh, indent=2)
    print(f"\n📋 {stage} manifest: {len(entries)} files, hash={manifest_hash[:16]}…")
    return manifest_path

_generate_manifest(OUTPUT_DIR, "bronze")