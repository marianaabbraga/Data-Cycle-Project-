import pandas as pd
import pyodbc
from datetime import datetime

CSV_FILE_PATH = r"C:\DataCycleProject\Data-Cycle-Project-\data\knime\output\output.csv"

SQL_SERVER   = r".\STOCKSQLSERVER"
DATABASE     = "stockmarketdb2"
# Windows authentication (Trusted_Connection) — replace with
# UID/PWD if you use a SQL Server login
CONNECTION_STRING = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
)
# ==============================================================


def get_date_id(cursor: pyodbc.Cursor, date_value: str) -> int:
    """Returns the DateId matching the given date (format YYYY-MM-DD).
    DimDate contains columns DateId, Year, Month, Day (integers).
    If the date is missing, it is inserted automatically."""
    dt = datetime.strptime(date_value, "%Y-%m-%d")
    cursor.execute(
        "SELECT DateId FROM DimDate WHERE Year = ? AND Month = ? AND Day = ?",
        dt.year, dt.month, dt.day
    )
    row = cursor.fetchone()
    if row:
        return row[0]

    # Date not found → insert automatically
    # DateId follows the YYYYMMDD format (e.g. 20260325)
    new_id = dt.year * 10000 + dt.month * 100 + dt.day
    cursor.execute(
        "INSERT INTO DimDate (DateId, Year, Month, Day) VALUES (?, ?, ?, ?)",
        new_id, dt.year, dt.month, dt.day
    )
    print(f"  [DimDate] Date '{date_value}' inserted with DateId={new_id}.")
    return new_id


def get_ticker_id(cursor: pyodbc.Cursor, ticker: str) -> int:
    """Returns the TickerId matching the given ticker symbol.
    DimTicker contains columns TickerId and Symbol."""
    cursor.execute(
        "SELECT TickerId FROM DimTicker WHERE Symbol = ?",
        ticker
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    raise ValueError(
        f"Ticker '{ticker}' not found in DimTicker. "
        "Please populate the ticker dimension first."
    )


def get_sector_id(cursor: pyodbc.Cursor, sector: str) -> int:
    """Returns the SectorId matching the given sector.
    DimSector contains columns SectorId and Sector."""
    cursor.execute(
        "SELECT SectorId FROM DimSector WHERE Sector = ?",
        sector
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    raise ValueError(
        f"Sector '{sector}' not found in DimSector. "
        "Please populate the sector dimension first."
    )


def get_industry_id(cursor: pyodbc.Cursor, industry: str) -> int:
    """Returns the IndustryId matching the given industry.
    DimIndustry contains columns IndustryId and Industry."""
    cursor.execute(
        "SELECT IndustryId FROM DimIndustry WHERE Industry = ?",
        industry
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    raise ValueError(
        f"Industry '{industry}' not found in DimIndustry. "
        "Please populate the industry dimension first."
    )


def main():
    print(f"[{datetime.now():%H:%M:%S}] Reading CSV file: {CSV_FILE_PATH}")
    df = pd.read_csv(CSV_FILE_PATH, parse_dates=["date", "exportDate"])

    # Normalize date columns to string format
    df["date"]       = df["date"].dt.strftime("%Y-%m-%d")
    df["exportDate"] = df["exportDate"].dt.strftime("%Y-%m-%d")

    print(f"  → {len(df)} rows loaded.")

    print(f"[{datetime.now():%H:%M:%S}] Connecting to SQL Server ({SQL_SERVER} / {DATABASE})...")
    conn   = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()

    # Cache to avoid querying dimensions repeatedly for the same value
    date_cache     = {}
    ticker_cache   = {}
    sector_cache   = {}
    industry_cache = {}

    insert_sql = """
        INSERT INTO FactPredictedStockPrices
            (DateId, TickerId, SectorId, IndustryId, PredictionDateId, PredictedPrice)
        VALUES (?, ?, ?, ?, ?, ?)
    """

    inserted = 0
    errors   = 0

    print(f"[{datetime.now():%H:%M:%S}] Inserting rows...")
    for idx, row in df.iterrows():
        try:
            # --- Resolve foreign keys (with cache) ---
            date_id = date_cache.setdefault(
                row["date"],
                get_date_id(cursor, row["date"])
            )
            ticker_id = ticker_cache.setdefault(
                row["ticker"],
                get_ticker_id(cursor, row["ticker"])
            )
            sector_id = sector_cache.setdefault(
                row["sector"],
                get_sector_id(cursor, row["sector"])
            )
            industry_id = industry_cache.setdefault(
                row["industry"],
                get_industry_id(cursor, row["industry"])
            )
            prediction_date_id = date_cache.setdefault(
                row["exportDate"],
                get_date_id(cursor, row["exportDate"])
            )

            cursor.execute(
                insert_sql,
                date_id,
                ticker_id,
                sector_id,
                industry_id,
                prediction_date_id,
                float(row["close"])
            )
            inserted += 1

        except ValueError as ve:
            print(f"  [WARNING] Row {idx + 2} skipped: {ve}")
            errors += 1
        except Exception as ex:
            print(f"  [ERROR] Row {idx + 2} — {ex}")
            errors += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"\n[{datetime.now():%H:%M:%S}] Done.")
    print(f"  Rows inserted: {inserted}")
    print(f"  Rows skipped:  {errors}")


if __name__ == "__main__":
    main()