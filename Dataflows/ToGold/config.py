import os

# ==============================================================================
# CONFIG  (ToGold)
# ==============================================================================

_default_silver = os.path.join(os.path.dirname(__file__), "..", "..", "DataLake", "Silver")
SILVER_DIR = os.getenv("SILVER_INPUT_DIR", _default_silver)

# SQL Server connection — env vars are set by docker-compose for the pipeline
# container; the hardcoded defaults match the local Windows dev setup.
DB_SERVER   = os.getenv("DB_SERVER", "localhost\\STOCKSQLSERVER")
DB_DATABASE = os.getenv("DB_NAME", "stockmarketdb2")
DB_USER     = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_DRIVER   = os.getenv("DB_DRIVER", "{ODBC Driver 18 for SQL Server}")

GOLD_WATERMARK_FILE = os.path.join(os.path.dirname(__file__), "_last_gold_run.txt")