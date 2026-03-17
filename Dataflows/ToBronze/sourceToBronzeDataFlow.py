"""
Bronze Layer — Raw Data Ingestion
"""

import os
import pandas as pd
import yfinance as yf
from datetime import datetime

# ==============================================================================
# CONFIGURATION
# ==============================================================================

PORTFOLIO = ["AAPL", "MSFT", "NVDA"]
ETF_PORTFOLIO = ["SPY", "QQQ"]
MARKETS = ["us_market", "eu_market"]
START_DATE = "2016-01-01"
OUTPUT_DIR = "output_raw"

SUBFOLDERS = ["ticker", "market", "sector_industry", "equity", "funds"]

for folder in SUBFOLDERS:
    os.makedirs(os.path.join(OUTPUT_DIR, folder), exist_ok=True)

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

print("=" * 60)
print("Bronze Layer — Raw Data Ingestion")
print(f"Run timestamp : {timestamp}")
print("=" * 60)

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

for symbol in PORTFOLIO:
    print(f"\n[{symbol}]")
    ticker = yf.Ticker(symbol)

    # Prices
    try:
        df_prices = ticker.history(start=START_DATE)
        if not df_prices.empty:
            df_prices.reset_index(inplace=True)
            df_prices["ticker"] = symbol
            df_prices["extraction_timestamp"] = datetime.now().isoformat()
            save_raw(df_prices, "ticker", f"{symbol}_prices_{timestamp}.parquet", symbol)
    except Exception as e:
        print(f"⚠️ prices failed for {symbol}: {e}")

    # Calendar
    try:
        calendar = ticker.calendar
        if calendar:
            df_cal = pd.DataFrame([calendar]) if isinstance(calendar, dict) else calendar.reset_index()
            df_cal["ticker"] = symbol
            df_cal["extraction_timestamp"] = datetime.now().isoformat()
            save_raw(df_cal, "ticker", f"{symbol}_calendar_{timestamp}.parquet", symbol)
    except Exception as e:
        print(f"⚠️ calendar failed for {symbol}: {e}")

    # Info
    try:
        info = ticker.info
        df_info = pd.DataFrame([info])
        df_info["ticker"] = symbol
        df_info["extraction_timestamp"] = datetime.now().isoformat()
        save_raw(df_info, "ticker", f"{symbol}_info_{timestamp}.parquet", symbol)
    except Exception as e:
        print(f"⚠️ info failed for {symbol}: {e}")

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

            save_raw(df_status, "market",
                     f"{market_id}_status_{timestamp}.parquet", market_id)

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

            save_raw(df_summary, "market",
                     f"{market_id}_summary_{timestamp}.parquet", market_id)

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

            save_raw(pd.DataFrame([overview]),
                     "sector_industry",
                     f"sector_{sector_key}_{timestamp}.parquet",
                     symbol)

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

            save_raw(pd.DataFrame([overview]),
                     "sector_industry",
                     f"industry_{industry_key}_{timestamp}.parquet",
                     symbol)

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
                save_raw(df, "equity", f"{symbol}_{attr}_{timestamp}.parquet", symbol)
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

        save_raw(pd.DataFrame([summary]),
                 "funds",
                 f"{symbol}_overview_{timestamp}.parquet",
                 symbol)

    except Exception as e:
        print(f"⚠️ funds_data failed: {e}")

print("\nUS-7 complete.")


# ==============================================================================
# SUMMARY
# ==============================================================================

print("\n" + "=" * 60)
print("✅ Bronze extraction complete.")
print("=" * 60)