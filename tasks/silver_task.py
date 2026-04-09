"""
Silver Task — KNIME + Python Cleaning
======================================

Optionally runs a KNIME workflow via its batch launcher, then applies
Python-based cleaning (dedup, type coercion, whitespace stripping)
and writes the result as a Parquet file.
"""

import os
import subprocess
from datetime import datetime

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger


@task(name="silver-transform", retries=1, retry_delay_seconds=30)
def run_silver(
    data_dir: str = "./data",
    bronze_file: str | None = None,
    knime_batch: str | None = None,
) -> dict:
    """
    Execute the Silver stage.

    Parameters
    ----------
    data_dir    : root data directory
    bronze_file : explicit path to the Bronze CSV; defaults to
                  ``<data_dir>/bronze/sap_extract.csv``
    knime_batch : path to the KNIME batch launcher; when *None* the
                  KNIME step is skipped

    Returns
    -------
    dict with ``file`` (Parquet path) and ``log`` (log path)
    """
    logger = get_run_logger()

    silver_dir = os.path.join(data_dir, "silver")
    os.makedirs(silver_dir, exist_ok=True)

    if bronze_file is None:
        bronze_file = os.path.join(data_dir, "bronze", "sap_extract.csv")

    output_file = os.path.join(silver_dir, "cleaned_data.parquet")
    log_file    = os.path.join(silver_dir, "silver.log")
    log: list[str] = []

    log.append(f"[{datetime.now().isoformat()}] Silver stage started")
    log.append(f"Input: {bronze_file}")

    # ----- KNIME (optional, subprocess) ----------------------------------
    if knime_batch and os.path.exists(knime_batch):
        logger.info(f"Running KNIME: {knime_batch}")
        result = subprocess.run(
            [knime_batch, "-nosplash", "-application",
             "org.knime.product.KNIME_BATCH_APPLICATION"],
            capture_output=True, text=True, timeout=600,
        )
        log.append(f"KNIME stdout:\n{result.stdout}")
        if result.returncode != 0:
            log.append(f"KNIME stderr:\n{result.stderr}")
            log.append(f"[ERROR] KNIME exited with code {result.returncode}")
    else:
        log.append("KNIME step skipped (no batch path)")

    # ----- Python cleaning -----------------------------------------------
    logger.info("Cleaning bronze data")
    df = pd.read_csv(bronze_file)
    initial = len(df)
    log.append(f"Loaded {initial} rows")

    df = df.drop_duplicates()
    log.append(f"Duplicates removed: {initial - len(df)}")

    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].str.strip()

    date_cols = [c for c in df.columns if "date" in c.lower()]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    before = len(df)
    df = df.dropna(how="all")
    log.append(f"Empty rows dropped: {before - len(df)}")

    numeric_hints = ["quantity", "amount", "price", "value"]
    for col in df.columns:
        if any(h in col.lower() for h in numeric_hints):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["_silver_timestamp"] = datetime.now().isoformat()
    df["_source_file"] = os.path.basename(bronze_file)

    df.to_parquet(output_file, index=False, engine="pyarrow")
    log.append(f"Output: {len(df)} rows -> {output_file}")
    logger.info(f"Silver output: {len(df)} rows")

    log.append(f"[{datetime.now().isoformat()}] Silver stage completed")

    with open(log_file, "w") as fh:
        fh.write("\n".join(log))

    return {"file": output_file, "log": log_file}
