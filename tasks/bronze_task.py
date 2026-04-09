"""
Bronze Task — SAP Extraction (mocked for POC)
==============================================

Runs an SAP extraction script via subprocess.  When no real script is
provided, generates deterministic mock data so the rest of the pipeline
can be tested end-to-end.
"""

import csv
import os
import subprocess
from datetime import datetime

from prefect import task
from prefect.logging import get_run_logger


@task(name="bronze-sap-extract", retries=1, retry_delay_seconds=30)
def run_bronze(data_dir: str = "./data", sap_script: str | None = None) -> dict:
    """
    Execute the Bronze stage.

    Parameters
    ----------
    data_dir   : root data directory (shared across all stages)
    sap_script : path to the real SAP extraction script; when *None*,
                 mock data is generated instead

    Returns
    -------
    dict with ``file`` (CSV path) and ``log`` (log path)
    """
    logger = get_run_logger()

    bronze_dir = os.path.join(data_dir, "bronze")
    os.makedirs(bronze_dir, exist_ok=True)

    output_file = os.path.join(bronze_dir, "sap_extract.csv")
    log_file    = os.path.join(bronze_dir, "bronze.log")
    log: list[str] = []

    log.append(f"[{datetime.now().isoformat()}] Bronze stage started")

    # ----- Real SAP script (subprocess) ---------------------------------
    if sap_script and os.path.exists(sap_script):
        logger.info(f"Running SAP script: {sap_script}")
        result = subprocess.run(
            ["python", sap_script],
            capture_output=True, text=True, timeout=300,
        )
        log.append(f"SAP stdout:\n{result.stdout}")
        if result.returncode != 0:
            log.append(f"SAP stderr:\n{result.stderr}")
            log.append(f"[ERROR] SAP script exited with code {result.returncode}")

    # ----- Mock data (POC) -----------------------------------------------
    else:
        logger.info("No SAP script provided — generating mock data")
        _generate_mock_data(output_file)
        log.append(f"Mock data written -> {output_file}")

    # ----- Row count validation ------------------------------------------
    if os.path.exists(output_file):
        with open(output_file, "r") as fh:
            row_count = sum(1 for _ in fh) - 1
        log.append(f"Output: {row_count} rows in {output_file}")
        logger.info(f"Bronze output: {row_count} rows")
    else:
        log.append("[CRITICAL] No output file generated!")

    log.append(f"[{datetime.now().isoformat()}] Bronze stage completed")

    with open(log_file, "w") as fh:
        fh.write("\n".join(log))

    return {"file": output_file, "log": log_file}


# ---------------------------------------------------------------------- #
#  Mock helpers                                                           #
# ---------------------------------------------------------------------- #

def _generate_mock_data(output_path: str):
    """Write a small SAP-style CSV so downstream stages have something to work with."""
    headers = [
        "material_id", "plant", "quantity", "unit",
        "posting_date", "document_number", "movement_type",
    ]
    rows = [
        ["MAT001", "1000", "150.00", "KG",  "2026-03-18", "4900001234", "101"],
        ["MAT002", "1000", "75.50",  "L",   "2026-03-18", "4900001235", "101"],
        ["MAT003", "2000", "200.00", "PC",  "2026-03-18", "4900001236", "201"],
        ["MAT001", "2000", "50.00",  "KG",  "2026-03-17", "4900001237", "101"],
        ["MAT004", "1000", "300.00", "KG",  "2026-03-17", "4900001238", "311"],
        ["MAT002", "3000", "120.00", "L",   "2026-03-16", "4900001239", "101"],
        ["MAT005", "1000", "85.25",  "PC",  "2026-03-16", "4900001240", "201"],
        ["MAT003", "3000", "60.00",  "PC",  "2026-03-15", "4900001241", "311"],
    ]
    with open(output_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)
