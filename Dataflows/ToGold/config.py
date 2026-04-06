import os

# ==============================================================================
# CONFIG  (ToGold)
# ==============================================================================

SILVER_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "DataLake", "Silver")

# SQL Server connection settings
DB_SERVER   = "localhost\\STOCKSQLSERVER"
DB_DATABASE = "stockmarketdb2"

GOLD_WATERMARK_FILE = os.path.join(os.path.dirname(__file__), "_last_gold_run.txt")