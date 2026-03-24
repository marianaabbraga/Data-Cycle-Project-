import os
import sys
import pandas as pd
from glob import glob

sys.path.append(os.path.abspath('../ToSilver/config'))
from config import SILVER_DIR
from factTables import connect, insert_from_dataframe
from goldSchema import create_all_tables

# ==============================================================================
# CONFIG
# ==============================================================================

DB_SERVER   = "10.130.25.154"
DB_DATABASE = "stockmarketdb2"

# ==============================================================================
# SILVER READERS
# ==============================================================================

def read_silver_table(table: str) -> pd.DataFrame:
    """Glob all parquet partitions under SILVER_DIR/<table> and concat them."""
    pattern = os.path.join(SILVER_DIR, table, "**", "*.parquet")
    files   = glob(pattern, recursive=True)
    if not files:
        print(f"  ⚠️  No parquet files found for Silver table '{table}'")
        return pd.DataFrame()
    dfs = [pd.read_parquet(f) for f in files]
    df  = pd.concat(dfs, ignore_index=True)
    print(f"  Loaded Silver '{table}': {len(df):,} rows from {len(files)} file(s)")
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
    dim["DateId"]   = dim["date"].dt.strftime("%Y%m%d").astype(int)
    dim["Year"]     = dim["date"].dt.year.astype("Int16")
    dim["Quarter"]  = dim["date"].dt.quarter.astype("Int8")
    dim["Month"]    = dim["date"].dt.month.astype("Int8")
    dim["Week"]     = dim["date"].dt.isocalendar().week.astype("Int8")
    dim["Day"]      = dim["date"].dt.day.astype("Int8")
    return dim[["DateId","Year","Quarter","Month","Week","Day"]].drop_duplicates("DateId")


def build_dim_sector(master_df: pd.DataFrame) -> pd.DataFrame:
    """Unique sectors with a surrogate key (row position + 1)."""
    sectors = master_df["sector"].dropna().drop_duplicates().reset_index(drop=True)
    dim = pd.DataFrame({
        "SectorId": range(1, len(sectors) + 1),
        "Sector":   sectors,
    })
    return dim


def build_dim_industry(master_df: pd.DataFrame) -> pd.DataFrame:
    """Unique industries with a surrogate key."""
    industries = master_df["industry"].dropna().drop_duplicates().reset_index(drop=True)
    dim = pd.DataFrame({
        "IndustryId": range(1, len(industries) + 1),
        "Industry":   industries,
    })
    return dim


def build_dim_ticker(master_df: pd.DataFrame) -> pd.DataFrame:
    """
    One row per ticker symbol.
    TickerId uses row position + 1 as a surrogate key.
    Sector / Industry are denormalised here as well (schema diagram shows them
    as plain text columns on DimTicker).
    """
    df = master_df[["ticker","company_name","sector","industry"]].drop_duplicates("ticker").reset_index(drop=True)
    df.insert(0, "TickerId", range(1, len(df) + 1))
    df = df.rename(columns={
        "ticker":       "Symbol",
        "company_name": "Name",
        "sector":       "Sector",
        "industry":     "Industry",
    })
    return df[["TickerId","Symbol","Name","Sector","Industry"]]

# ==============================================================================
# LOOKUP HELPERS  (Silver key → Gold surrogate int)
# ==============================================================================

def make_date_lookup(dim_date: pd.DataFrame) -> dict:
    """ticker date (Timestamp) → DateId (int YYYYMMDD)"""
    return {
        pd.Timestamp(row.date) if hasattr(row, "date") else row.DateId: row.DateId
        for row in dim_date.itertuples(index=False)
    }

def make_ticker_lookup(dim_ticker: pd.DataFrame) -> dict:
    """ticker symbol string → TickerId"""
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

    # Attach dimension keys by joining on the natural key columns
    ticker_map   = make_ticker_lookup(dim_ticker)
    sector_map   = make_sector_lookup(dim_sector)
    industry_map = make_industry_lookup(dim_industry)

    df = price_df.copy()

    # DateId: YYYYMMDD integer from the date column
    df["DateId"] = pd.to_datetime(df["date"]).dt.strftime("%Y%m%d").astype(int)

    # Surrogate keys via map — rows whose ticker/sector/industry aren't in the
    # dimension tables get NaN and are dropped below.
    df["TickerId"] = df["ticker"].map(ticker_map)

    # DimTicker carries Sector & Industry, so we join through it
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

    keep = ["DateId","TickerId","SectorId","IndustryId",
            "OpenPrice","HighPrice","LowPrice","ClosePrice",
            "AdjustedClosePrice","Volume"]

    df = df[keep].dropna(subset=["DateId","TickerId","SectorId","IndustryId"])

    # Cast surrogate keys to int (they may be float after .map on missing rows)
    for col in ["DateId","TickerId","SectorId","IndustryId"]:
        df[col] = df[col].astype(int)

    return df.drop_duplicates(subset=["DateId","TickerId"])


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

    keep = ["DateId","TickerId","SectorId","IndustryId",
            "sma20","sma50","atr","rsi","macd",
            "bollinger_bands_down","bollinger_bands_up"]

    df = df[keep].dropna(subset=["DateId","TickerId","SectorId","IndustryId"])

    for col in ["DateId","TickerId","SectorId","IndustryId"]:
        df[col] = df[col].astype(int)

    return df.drop_duplicates(subset=["DateId","TickerId"])

# ==============================================================================
# MAIN
# ==============================================================================

print("="*60)
print("Gold Layer Processing (Silver → SQL Server Star Schema)")
print("="*60)

# ── 1. Connect ────────────────────────────────────────────────────────────────
conn = connect(DB_SERVER, DB_DATABASE)

# ── 2. Ensure all tables exist ────────────────────────────────────────────────
create_all_tables(conn)

# ── 3. Read Silver ────────────────────────────────────────────────────────────
print("\nReading Silver layer...")
silver_master  = read_silver_table("stocks_master")
silver_prices  = read_silver_table("price_history")
silver_tech    = read_silver_table("technical_indicators")

if silver_prices.empty:
    print("\n⏭️  No Silver data found. Exiting.")
    conn.close()
    exit()

# ── 4. Build dimensions ───────────────────────────────────────────────────────
print("\nBuilding dimensions...")
dim_date     = build_dim_date(silver_prices)
dim_sector   = build_dim_sector(silver_master)
dim_industry = build_dim_industry(silver_master)
dim_ticker   = build_dim_ticker(silver_master)

print(f"  DimDate:     {len(dim_date):,} rows")
print(f"  DimSector:   {len(dim_sector):,} rows")
print(f"  DimIndustry: {len(dim_industry):,} rows")
print(f"  DimTicker:   {len(dim_ticker):,} rows")

# ── 5. Build facts ────────────────────────────────────────────────────────────
print("\nBuilding fact tables...")
fact_prices  = build_fact_stock_prices(silver_prices, dim_ticker, dim_sector, dim_industry)
fact_tech    = build_fact_technical(silver_tech,   dim_ticker, dim_sector, dim_industry)

print(f"  FactStockPrices:           {len(fact_prices):,} rows")
print(f"  FactTechnicalIndicators:   {len(fact_tech):,} rows")

# ── 6. Load into SQL Server ───────────────────────────────────────────────────
print("\nLoading into SQL Server...")

insert_from_dataframe(conn, "dbo.DimDate",     dim_date)
insert_from_dataframe(conn, "dbo.DimSector",   dim_sector)
insert_from_dataframe(conn, "dbo.DimIndustry", dim_industry)
insert_from_dataframe(conn, "dbo.DimTicker",   dim_ticker)

insert_from_dataframe(conn, "dbo.FactStockPrices",          fact_prices)
insert_from_dataframe(conn, "dbo.FactTechnicalIndicators",  fact_tech)

# FactPredictedStockPrices intentionally skipped — table exists, stays empty.

conn.close()
print("\n✅ Gold Layer DONE")