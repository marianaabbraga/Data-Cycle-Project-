import os
import pandas as pd
from glob import glob
import numpy as np

from config import SILVER_DIR, DB_SERVER, DB_DATABASE, DB_USER, DB_PASSWORD, DB_DRIVER
from watermark import load_watermark_gold, save_watermark_gold
from factTables import connect, insert_from_dataframe, merge_into_table, delete_and_insert
from goldSchema import create_all_tables

# ==============================================================================
# SILVER READERS
# ==============================================================================

def read_silver_table(table: str, last_ts: float = 0.0) -> pd.DataFrame:
    """
    Read Silver parquet files for the given table.

    When last_ts > 0 (incremental run) only files whose mtime is newer than
    last_ts are loaded — i.e. the partitions Silver wrote in the latest batch.
    When last_ts == 0.0 (first run / full back-fill) every file is read.

    stocks_master is always passed last_ts=0.0 by the caller because it is
    a single flat parquet file (not Hive-partitioned); its mtime changes on
    every Silver run, so a timestamp guard would over-filter it.
    """
    pattern   = os.path.join(SILVER_DIR, table, "**", "*.parquet")
    all_files = glob(pattern, recursive=True)

    new_files = [f for f in all_files if os.path.getmtime(f) > last_ts] \
                if last_ts > 0.0 else all_files

    if not new_files:
        print(f"  ⏭️  No new parquet files for Silver table '{table}'")
        return pd.DataFrame()

    df = pd.concat([pd.read_parquet(f) for f in new_files], ignore_index=True)
    print(f"  Loaded Silver '{table}': {len(df):,} rows "
          f"from {len(new_files)}/{len(all_files)} file(s)")
    return df


# ==============================================================================
# DIMENSION BUILDERS
# ==============================================================================

def build_dim_date(price_df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive DimDate from every distinct date in price_history.
    DateId format: YYYYMMDD (integer) — e.g. 20240315 for 2024-03-15.
    This makes date lookups fast and human-readable without a JOIN.
    """
    dates = price_df["date"].drop_duplicates().dropna()
    dim = pd.DataFrame({"date": pd.to_datetime(dates)})
    dim["DateId"]  = dim["date"].dt.strftime("%Y%m%d").astype(int)
    dim["Year"]    = dim["date"].dt.year.astype(int)
    dim["Quarter"] = dim["date"].dt.quarter.astype(int)
    dim["Month"]   = dim["date"].dt.month.astype(int)
    dim["Week"]    = dim["date"].dt.isocalendar().week.astype(int)
    dim["Day"]     = dim["date"].dt.day.astype(int)
    return dim[["DateId", "Year", "Quarter", "Month", "Week", "Day"]].drop_duplicates("DateId")


def build_dim_sector(master_df: pd.DataFrame) -> pd.DataFrame:
    """Unique sectors with a surrogate key (row position + 1)."""
    sectors = master_df["sector"].dropna().drop_duplicates().reset_index(drop=True)
    return pd.DataFrame({
        "SectorId": range(1, len(sectors) + 1),
        "Sector":   sectors,
    })


def build_dim_industry(master_df: pd.DataFrame) -> pd.DataFrame:
    """Unique industries with a surrogate key."""
    industries = master_df["industry"].dropna().drop_duplicates().reset_index(drop=True)
    return pd.DataFrame({
        "IndustryId": range(1, len(industries) + 1),
        "Industry":   industries,
    })


def build_dim_ticker(master_df: pd.DataFrame) -> pd.DataFrame:
    """
    One row per ticker symbol.
    TickerId uses row position + 1 as a surrogate key (Python-side only).
    The authoritative TickerId used in fact tables is re-read from SQL Server
    after the MERGE so that IDENTITY values assigned by the DB are used.
    """
    df = (master_df[["ticker", "company_name", "sector", "industry"]]
          .drop_duplicates("ticker")
          .reset_index(drop=True))
    df.insert(0, "TickerId", range(1, len(df) + 1))
    df = df.rename(columns={
        "ticker":       "Symbol",
        "company_name": "Name",
        "sector":       "Sector",
        "industry":     "Industry",
    })
    return df[["TickerId", "Symbol", "Name", "Sector", "Industry"]]


# ==============================================================================
# LOOKUP HELPERS
# ==============================================================================

def make_ticker_lookup(dim_ticker: pd.DataFrame) -> dict:
    return dict(zip(dim_ticker["Symbol"], dim_ticker["TickerId"]))

def make_sector_lookup(dim_sector: pd.DataFrame) -> dict:
    return dict(zip(dim_sector["Sector"], dim_sector["SectorId"]))

def make_industry_lookup(dim_industry: pd.DataFrame) -> dict:
    return dict(zip(dim_industry["Industry"], dim_industry["IndustryId"]))


# ==============================================================================
# FACT BUILDERS
# ==============================================================================

def build_fact_stock_prices(
    price_df: pd.DataFrame,
    dim_ticker: pd.DataFrame,
    dim_sector: pd.DataFrame,
    dim_industry: pd.DataFrame,
) -> pd.DataFrame:

    ticker_map   = make_ticker_lookup(dim_ticker)
    sector_map   = make_sector_lookup(dim_sector)
    industry_map = make_industry_lookup(dim_industry)

    df = price_df.copy()
    df["DateId"]   = pd.to_datetime(df["date"]).dt.strftime("%Y%m%d").astype(int)
    df["TickerId"] = df["ticker"].map(ticker_map)

    ticker_sector   = dim_ticker.set_index("Symbol")["Sector"]
    ticker_industry = dim_ticker.set_index("Symbol")["Industry"]
    df["SectorId"]   = df["ticker"].map(ticker_sector).map(sector_map)
    df["IndustryId"] = df["ticker"].map(ticker_industry).map(industry_map)

    df = df.rename(columns={
        "open":      "OpenPrice",
        "high":      "HighPrice",
        "low":       "LowPrice",
        "close":     "ClosePrice",
        "adj_close": "AdjustedClosePrice",
        "volume":    "Volume",
    })

    keep = ["DateId", "TickerId", "SectorId", "IndustryId",
            "OpenPrice", "HighPrice", "LowPrice", "ClosePrice",
            "AdjustedClosePrice", "Volume"]

    df = df[keep].dropna(subset=["DateId", "TickerId", "SectorId", "IndustryId"])
    for col in ["DateId", "TickerId", "SectorId", "IndustryId"]:
        df[col] = df[col].astype(int)
    return df.drop_duplicates(subset=["DateId", "TickerId"])


def build_fact_technical(
    tech_df: pd.DataFrame,
    dim_ticker: pd.DataFrame,
    dim_sector: pd.DataFrame,
    dim_industry: pd.DataFrame,
) -> pd.DataFrame:

    ticker_map   = make_ticker_lookup(dim_ticker)
    sector_map   = make_sector_lookup(dim_sector)
    industry_map = make_industry_lookup(dim_industry)

    df = tech_df.copy()
    df["DateId"]   = pd.to_datetime(df["date"]).dt.strftime("%Y%m%d").astype(int)
    df["TickerId"] = df["ticker"].map(ticker_map)

    ticker_sector   = dim_ticker.set_index("Symbol")["Sector"]
    ticker_industry = dim_ticker.set_index("Symbol")["Industry"]
    df["SectorId"]   = df["ticker"].map(ticker_sector).map(sector_map)
    df["IndustryId"] = df["ticker"].map(ticker_industry).map(industry_map)

    df = df.rename(columns={
        "sma_20":   "sma20",
        "sma_50":   "sma50",
        "bb_lower": "bollinger_bands_down",
        "bb_upper": "bollinger_bands_up",
    })

    keep = ["DateId", "TickerId", "SectorId", "IndustryId",
            "sma20", "sma50", "atr", "rsi", "macd",
            "bollinger_bands_down", "bollinger_bands_up"]

    df = df[keep].dropna(subset=["DateId", "TickerId", "SectorId", "IndustryId"])
    for col in ["DateId", "TickerId", "SectorId", "IndustryId"]:
        df[col] = df[col].astype(int)
    return df.drop_duplicates(subset=["DateId", "TickerId"])


# ==============================================================================
# MAIN
# ==============================================================================

print("=" * 60)
print("Gold Layer Processing (Silver → SQL Server Star Schema)")
print("=" * 60)

# ── 1. Connect ────────────────────────────────────────────────────────────────
if DB_USER:
    conn = connect(DB_SERVER, DB_DATABASE, driver=DB_DRIVER,
                   trusted=False, username=DB_USER, password=DB_PASSWORD)
else:
    conn = connect(DB_SERVER, DB_DATABASE, driver=DB_DRIVER)

# ── 2. Ensure all Gold tables exist (idempotent) ──────────────────────────────
create_all_tables(conn)

# ── 3. Load Gold watermark ────────────────────────────────────────────────────
# last_ts == 0.0  → first run, read every Silver file (full back-fill)
# last_ts  > 0.0  → incremental run, only Silver files written since last_ts
last_ts = load_watermark_gold()
print(f"\n[INCREMENTAL] Gold watermark: {last_ts}")

# ── 4. Read Silver (incremental) ──────────────────────────────────────────────
print("\nReading Silver layer (incremental)...")
silver_prices = read_silver_table("price_history",        last_ts)
silver_tech   = read_silver_table("technical_indicators", last_ts)

# stocks_master is always read in full so dimension lookups are never stale.
# It is small so reading it every run is not a performance concern.
print("\nReading Silver stocks_master (full — needed for dimension lookups)...")
silver_master_full = read_silver_table("stocks_master", last_ts=0.0)

if silver_prices.empty and silver_tech.empty:
    print("\n⏭️  Nothing new in Silver. Gold is already up to date.")
    conn.close()
    exit()

# ── 5. Build dimensions (always from full master so lookups are complete) ──────
print("\nBuilding dimensions...")
dim_date     = build_dim_date(silver_prices) if not silver_prices.empty else pd.DataFrame()
dim_sector   = build_dim_sector(silver_master_full)
dim_industry = build_dim_industry(silver_master_full)
dim_ticker   = build_dim_ticker(silver_master_full)

print(f"  DimDate:     {len(dim_date):,} rows")
print(f"  DimSector:   {len(dim_sector):,} rows")
print(f"  DimIndustry: {len(dim_industry):,} rows")
print(f"  DimTicker:   {len(dim_ticker):,} rows")

# ── 6. Upsert dimension tables ────────────────────────────────────────────────
# MERGE strategy — safe for slowly-changing dimensions:
#   WHEN MATCHED     → update descriptive attributes (e.g. sector rename)
#   WHEN NOT MATCHED → insert new row
#
# DimTicker: TickerId is an IDENTITY column managed by SQL Server.
# We pass only Symbol/Name/Sector/Industry to the MERGE so SQL Server
# never sees an attempt to SET an identity column (which raises an error).
print("\nUpserting dimension tables...")

if not dim_date.empty:
    merge_into_table(
        conn, "dbo.DimDate", dim_date,
        key_cols=["DateId"],
        update_cols=["Year", "Quarter", "Month", "Week", "Day"],
    )

merge_into_table(
    conn, "dbo.DimSector", dim_sector,
    key_cols=["SectorId"],
    update_cols=["Sector"],
)

merge_into_table(
    conn, "dbo.DimIndustry", dim_industry,
    key_cols=["IndustryId"],
    update_cols=["Industry"],
)

dim_ticker_no_id = dim_ticker[["Symbol", "Name", "Sector", "Industry"]]
merge_into_table(
    conn, "dbo.DimTicker", dim_ticker_no_id,
    key_cols=["Symbol"],
    update_cols=["Name", "Sector", "Industry"],
)

# ── 7. Re-read DimTicker from DB for authoritative IDENTITY TickerIds ─────────
print("\n  Re-reading DimTicker from SQL Server for authoritative TickerIds...")
dim_ticker_db = pd.read_sql(
    "SELECT TickerId, Symbol, Sector, Industry FROM dbo.DimTicker", conn
)

# ── 8. Build fact tables ──────────────────────────────────────────────────────
print("\nBuilding fact tables...")

fact_prices = pd.DataFrame()
if not silver_prices.empty:
    fact_prices = build_fact_stock_prices(
        silver_prices, dim_ticker_db, dim_sector, dim_industry
    )
    print(f"  FactStockPrices:         {len(fact_prices):,} rows")

fact_tech = pd.DataFrame()
if not silver_tech.empty:
    fact_tech = build_fact_technical(
        silver_tech, dim_ticker_db, dim_sector, dim_industry
    )
    print(f"  FactTechnicalIndicators: {len(fact_tech):,} rows")

# ── 9. Load fact tables (DELETE touched dates → bulk INSERT) ──────────────────
# DELETE + INSERT per DateId slice:
#   - Faster than row-by-row MERGE for large fact tables.
#   - Idempotent: re-running the same Silver batch cleanly replaces old rows
#     (handles price corrections from yfinance without leaving stale data).
print("\nLoading fact tables into SQL Server...")

if not fact_prices.empty:
    delete_and_insert(conn, "dbo.FactStockPrices", fact_prices)

if not fact_tech.empty:
    delete_and_insert(conn, "dbo.FactTechnicalIndicators", fact_tech)

# FactPredictedStockPrices intentionally skipped — stays empty until the
# ML prediction module is implemented.

# ── 10. Save Gold watermark (only after all writes succeed) ───────────────────
save_watermark_gold()

conn.close()
print("\n✅ Gold Layer DONE (incremental)")