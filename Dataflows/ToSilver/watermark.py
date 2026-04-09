import os
from datetime import datetime
from config import WATERMARK_FILE

# ==============================================================================
# WATERMARK
# ==============================================================================

# [INCREMENTAL] 
# load_watermark() → returns 0.0 on the very first run so every existing
#                    Bronze file is treated as "new" (full back-fill once,
#                    incremental from the second run onwards).
# save_watermark() → called ONLY after all Silver writes succeed, so a
#                    mid-run crash leaves the watermark unchanged and the
#                    next run will safely retry the same batch.

def load_watermark() -> float:
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE) as f:
            ts_str = f.read().strip()
        return datetime.strptime(ts_str, "%Y-%m-%d_%H-%M-%S").timestamp()
    print(" No watermark found — treating all files as new (first run).")
    return 0.0


def save_watermark() -> None:
    ts_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    with open(WATERMARK_FILE, "w") as f:
        f.write(ts_str)
    print(f" Watermark updated → {ts_str}")
