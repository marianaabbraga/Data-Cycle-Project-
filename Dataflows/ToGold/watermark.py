import os
from datetime import datetime
from config import GOLD_WATERMARK_FILE

# ==============================================================================
# GOLD WATERMARK  (ToGold)
# ==============================================================================
#
# Tracks the last successful Silver → Gold run independently of ToSilver's
# _last_silver_run.txt, so the two pipelines can run at different cadences
# without interfering with each other.
#
# load_watermark_gold() → returns 0.0 on the very first run, which causes
#                         read_silver_table() to load every existing Silver
#                         file (full back-fill). From the second run onwards
#                         only Silver partitions written after the watermark
#                         timestamp are loaded.
#
# save_watermark_gold() → called ONLY after all SQL Server writes succeed.
#                         If the pipeline crashes mid-run the watermark stays
#                         at its previous value, so the next run safely retries
#                         the same Silver batch — no data is silently lost.

def load_watermark_gold() -> float:
    if os.path.exists(GOLD_WATERMARK_FILE):
        with open(GOLD_WATERMARK_FILE) as f:
            ts_str = f.read().strip()
        return datetime.strptime(ts_str, "%Y-%m-%d_%H-%M-%S").timestamp()
    print("  No Gold watermark found — treating all Silver files as new (first run).")
    return 0.0


def save_watermark_gold() -> None:
    ts_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    with open(GOLD_WATERMARK_FILE, "w") as f:
        f.write(ts_str)
    print(f"  Gold watermark updated → {ts_str}")