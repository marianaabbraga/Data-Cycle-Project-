# ==============================================================================
# GOLD SCHEMA — Table definitions matching the star-schema diagram
# ==============================================================================
#
# Usage:
#   from goldSchema import create_all_tables
#   create_all_tables(conn)
#
# All CREATE calls are idempotent (IF NOT EXISTS), so this is safe to run on
# every pipeline execution.
# ==============================================================================

from factTables import create_table          # re-use the generic helper

# ── Dimension tables ───────────────────────────────────────────────────────────

DIM_DATE = {
    "DateId"   : "INT NOT NULL",
    "Year"     : "SMALLINT",
    "Quarter"  : "TINYINT",
    "Month"    : "TINYINT",
    "Week"     : "TINYINT",
    "Day"      : "TINYINT",
}

DIM_TICKER = {
    "TickerId" : "INT IDENTITY(1,1) NOT NULL",
    "Symbol"   : "NVARCHAR(10) NOT NULL",
    "Name"     : "NVARCHAR(255)",
    "Sector"   : "NVARCHAR(100)",
    "Industry" : "NVARCHAR(100)",
}

DIM_SECTOR = {
    "SectorId" : "INT IDENTITY(1,1) NOT NULL",
    "Sector"   : "NVARCHAR(100) NOT NULL",
}

DIM_INDUSTRY = {
    "IndustryId" : "INT IDENTITY(1,1) NOT NULL",
    "Industry"   : "NVARCHAR(100) NOT NULL",
}

# ── Fact tables ────────────────────────────────────────────────────────────────

FACT_STOCK_PRICES = {
    "DateId"           : "INT NOT NULL",
    "TickerId"         : "INT NOT NULL",
    "SectorId"         : "INT NOT NULL",
    "IndustryId"       : "INT NOT NULL",
    "OpenPrice"        : "FLOAT",
    "HighPrice"        : "FLOAT",
    "LowPrice"         : "FLOAT",
    "ClosePrice"       : "FLOAT",
    "AdjustedClosePrice": "FLOAT",
    "Volume"           : "BIGINT",
}

FACT_TECHNICAL_INDICATORS = {
    "DateId"            : "INT NOT NULL",
    "TickerId"          : "INT NOT NULL",
    "SectorId"          : "INT NOT NULL",
    "IndustryId"        : "INT NOT NULL",
    "sma20"             : "FLOAT",
    "sma50"             : "FLOAT",
    "atr"               : "FLOAT",
    "rsi"               : "FLOAT",
    "macd"              : "FLOAT",
    "bollinger_bands_down": "FLOAT",
    "bollinger_bands_up"  : "FLOAT",
}

# FactPredictedStockPrices intentionally left empty — will be filled later
FACT_PREDICTED_STOCK_PRICES = {
    "DateId"          : "INT NOT NULL",
    "TickerId"        : "INT NOT NULL",
    "SectorId"        : "INT NOT NULL",
    "IndustryId"      : "INT NOT NULL",
    "PredictionDateId": "INT",
    "PredictedPrice"  : "FLOAT",
}


# ==============================================================================
# ENTRY POINT
# ==============================================================================

def create_all_tables(conn):
    """Create every Gold table (idempotent). Call once per pipeline run."""

    create_table(conn, "dbo.DimDate",    DIM_DATE,    primary_key=["DateId"])
    create_table(conn, "dbo.DimTicker",  DIM_TICKER,  primary_key=["TickerId"])
    create_table(conn, "dbo.DimSector",  DIM_SECTOR,  primary_key=["SectorId"])
    create_table(conn, "dbo.DimIndustry",DIM_INDUSTRY,primary_key=["IndustryId"])

    create_table(conn, "dbo.FactStockPrices",
                 FACT_STOCK_PRICES,
                 primary_key=["DateId", "TickerId"])

    create_table(conn, "dbo.FactTechnicalIndicators",
                 FACT_TECHNICAL_INDICATORS,
                 primary_key=["DateId", "TickerId"])

    # Prediction table created but left empty for now
    create_table(conn, "dbo.FactPredictedStockPrices",
                 FACT_PREDICTED_STOCK_PRICES,
                 primary_key=["DateId", "TickerId"])

    print("\n✅ All Gold tables ready.")