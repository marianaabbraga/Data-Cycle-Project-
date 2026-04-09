import os

# ==============================================================================
# CONFIG
# ==============================================================================

BRONZE_DIR = os.getenv("BRONZE_INPUT_DIR", r"..\DataLake\Bronze")
SILVER_DIR = os.getenv("SILVER_OUTPUT_DIR", r"..\DataLake\Silver")

# [INCREMENTAL] Path to the watermark file that records the last successful run.
# Silver reads this to know which Bronze files are new; Bronze writes it after
# each extraction so Silver always has a reliable cutoff timestamp.
# WATERMARK_FILE = r"..\DataLake\_last_silver_run.txt"
WATERMARK_FILE = "_last_silver_run.txt"

# [INCREMENTAL] Lookback window fed to compute_indicators alongside new rows.
# Must be >= the longest rolling window used below (sma_50 = 50 days).
# 60 gives a small safety buffer on top of that.
LOOKBACK_ROWS = 60

os.makedirs(SILVER_DIR, exist_ok=True)
