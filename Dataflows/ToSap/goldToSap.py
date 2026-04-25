"""
export_to_csv.py
────────────────
Exports the gold layer of the SQL Server data warehouse to two CSV files
intended for import into SAP Analytics Cloud (SAC).

Generated files:
  - stock_prices_daily.csv
  - technical_indicators_daily.csv

Dependencies:
  pip install pyodbc pandas python-dotenv
"""

import os
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyodbc
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# Logging configuration
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Environment variables
# Create a .env file at the project root:
#bc2Dm3hjSbaLqe~O)sH
#   DB_SERVER=my_server\\SQLEXPRESS   (or IP)
#   DB_NAME=StockWarehouse
#   DB_USER=my_user                   (leave empty for Windows Auth)
#   DB_PASSWORD=my_password           (leave empty for Windows Auth)
#   OUTPUT_DIR=./output
# ─────────────────────────────────────────────
load_dotenv()

DB_SERVER   = os.getenv("DB_SERVER", ".\STOCKSQLSERVER")
DB_NAME     = os.getenv("DB_NAME",   "stockmarketdb2")
DB_USER     = os.getenv("DB_USER",   "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
OUTPUT_DIR  = Path(os.getenv("OUTPUT_DIR", r"data\sap"))

# C:\DataCycleProject\Data-Cycle-Project-\data\sap"
# ─────────────────────────────────────────────
# Denormalized SQL queries
# ─────────────────────────────────────────────
QUERY_PRICES = """
SELECT
    -- Time dimensions

    CAST(
        CAST(d.Year AS VARCHAR) + '-' +
        RIGHT('0' + CAST(d.Month AS VARCHAR), 2) + '-' +
        RIGHT('0' + CAST(d.Day AS VARCHAR), 2)
    AS DATE) AS DateKey,
    d.Year,
    d.Quarter,
    d.Month,
    d.Week,
    d.Day,

    -- Ticker / sector dimensions
    t.Symbol,
    t.Name,
    s.Sector,
    i.Industry,

    -- Price metrics
    f.OpenPrice,
    f.HighPrice,
    f.LowPrice,
    f.ClosePrice,
    f.AdjustedClosePrice,
    f.Volume



FROM FactStockPrices f
JOIN DimDate     d ON f.DateId     = d.DateId
JOIN DimTicker   t ON f.TickerId   = t.TickerId
JOIN DimSector   s ON f.SectorId   = s.SectorId
JOIN DimIndustry i ON f.IndustryId = i.IndustryId

ORDER BY d.Year, d.Month, d.Day, t.Symbol;
"""

QUERY_INDICATORS = """
SELECT
    -- Time dimensions

    CAST(
        CAST(d.Year AS VARCHAR) + '-' +
        RIGHT('0' + CAST(d.Month AS VARCHAR), 2) + '-' +
        RIGHT('0' + CAST(d.Day AS VARCHAR), 2)
    AS DATE) AS DateKey,
    d.Year,
    d.Quarter,
    d.Month,
    d.Week,
    d.Day,

    -- Ticker / sector dimensions
    t.Symbol,
    t.Name,
    s.Sector,
    i.Industry,

    -- Technical indicators
    f.sma20,
    f.sma50,
    f.atr,
    f.rsi,
    f.macd,
    f.bollinger_bands_down,
    f.bollinger_bands_up


FROM FactTechnicalIndicators f
JOIN DimDate     d ON f.DateId     = d.DateId
JOIN DimTicker   t ON f.TickerId   = t.TickerId
JOIN DimSector   s ON f.SectorId   = s.SectorId
JOIN DimIndustry i ON f.IndustryId = i.IndustryId

ORDER BY d.Year, d.Month, d.Day, t.Symbol;
"""


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def build_connection_string() -> str:
    """Builds the ODBC connection string based on available authentication."""
    driver = "ODBC Driver 17 for SQL Server"

    if DB_USER and DB_PASSWORD:
        # SQL Server authentication
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={DB_SERVER};"
            f"DATABASE={DB_NAME};"
            f"UID={DB_USER};"
            f"PWD={DB_PASSWORD};"
        )
    else:
        # Windows authentication (Trusted Connection)
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={DB_SERVER};"
            f"DATABASE={DB_NAME};"
            f"Trusted_Connection=yes;"
        )


def get_connection() -> pyodbc.Connection:
    """Opens and returns a SQL Server connection."""
    conn_str = build_connection_string()
    log.info("Connecting to %s / %s ...", DB_SERVER, DB_NAME)
    conn = pyodbc.connect(conn_str, timeout=30)
    log.info("Connection established.")
    return conn


def export_query_to_csv(
    conn: pyodbc.Connection,
    query: str,
    filename: str,
    output_dir: Path,
    chunk_size: int = 50_000,
) -> Path:
    """
    Executes `query`, reads results in chunks and writes the CSV.
    Returns the path of the created file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / filename

    log.info("Exporting → %s", filepath)
    first_chunk = True
    total_rows  = 0

    for chunk in pd.read_sql(query, conn, chunksize=chunk_size):
        chunk.to_csv(
            filepath,
            mode="w" if first_chunk else "a",
            header=first_chunk,
            index=False,
            encoding="utf-8-sig",   # UTF-8 BOM — Excel / SAC compatibility
            sep=",",
            float_format="%.4f",    # 4 decimal places for financial indicators
        )
        total_rows  += len(chunk)
        first_chunk  = False
        log.info("  … %d rows written", total_rows)

    log.info("✓ %s — %d rows total", filename, total_rows)
    return filepath


def add_export_metadata(output_dir: Path, files: list[dict]) -> None:
    """Generates a small export log file for traceability."""
    meta_path = output_dir / "export_metadata.txt"
    with open(meta_path, "w", encoding="utf-8") as f:
        f.write(f"SAP SAC Export — {datetime.now().isoformat()}\n")
        f.write(f"Source: {DB_SERVER} / {DB_NAME}\n\n")
        for item in files:
            f.write(f"{item['file']}  —  {item['rows']} rows\n")
    log.info("Metadata written → %s", meta_path)


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────
def main() -> None:
    start = datetime.now()
    log.info("=== Export started ===")

    conn = get_connection()
    exported = []

    try:
        # 1. Stock prices
        path_prices = export_query_to_csv(
            conn,
            query=QUERY_PRICES,
            filename="stock_prices_daily.csv",
            output_dir=OUTPUT_DIR,
        )
        exported.append({"file": path_prices.name, "rows": sum(1 for _ in open(path_prices)) - 1})

        # 2. Technical indicators
        path_indicators = export_query_to_csv(
            conn,
            query=QUERY_INDICATORS,
            filename="technical_indicators_daily.csv",
            output_dir=OUTPUT_DIR,
        )
        exported.append({"file": path_indicators.name, "rows": sum(1 for _ in open(path_indicators)) - 1})

    finally:
        conn.close()
        log.info("Connection closed.")

    add_export_metadata(OUTPUT_DIR, exported)

    elapsed = (datetime.now() - start).total_seconds()
    log.info("=== Export completed in %.1f seconds ===", elapsed)
    log.info("Files available in: %s", OUTPUT_DIR.resolve())


if __name__ == "__main__":
    main()