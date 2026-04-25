"""
Bronze Task — yfinance Raw Data Ingestion
==========================================

Runs ``Dataflows/ToBronze/sourceToBronzeDataFlow.py`` as a subprocess,
captures its stdout/stderr into a log file, and reads back the manifest
that the script generates (SHA-256 hashes of every output parquet).
"""

import json
import os
import subprocess
import sys
from datetime import datetime

from prefect import task
from prefect.logging import get_run_logger

SCRIPT_REL = os.path.join("Dataflows", "ToBronze", "sourceToBronzeDataFlow.py")


@task(name="bronze-extract", retries=1, retry_delay_seconds=30)
def run_bronze(data_dir: str = "./data") -> dict:
    """
    Execute the Bronze stage.

    Returns
    -------
    dict with ``manifest`` (path), ``log`` (path), ``manifest_hash``,
    and ``output_dir``.
    """
    logger = get_run_logger()

    bronze_dir = os.path.join(data_dir, "bronze")
    os.makedirs(bronze_dir, exist_ok=True)
    log_file = os.path.join(bronze_dir, "bronze.log")
    log_lines: list[str] = [f"[{datetime.now().isoformat()}] Bronze stage started"]

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    script = os.path.join(project_root, SCRIPT_REL)

    if not os.path.isfile(script):
        raise FileNotFoundError(f"Bronze script not found: {script}")

    output_dir = os.path.abspath(os.path.join(data_dir, "bronze", "lake"))
    env = {**os.environ, "BRONZE_OUTPUT_DIR": output_dir}

    logger.info("Running ToBronze script: %s", script)
    log_lines.append(f"Script: {script}")
    log_lines.append(f"Output dir: {output_dir}")
    result = subprocess.run(
        [sys.executable, script],
        cwd=project_root,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=600,
    )

    log_lines.append(f"--- stdout ---\n{result.stdout}")
    if result.stderr:
        log_lines.append(f"--- stderr ---\n{result.stderr}")
    log_lines.append(f"Exit code: {result.returncode}")

    if result.returncode != 0:
        log_lines.append(f"[ERROR] ToBronze exited with code {result.returncode}")
        logger.error("ToBronze failed (exit %d): %s", result.returncode, result.stderr[:500])

    manifest_path = os.path.join(output_dir, "bronze_manifest.json")
    manifest_hash = ""
    if os.path.isfile(manifest_path):
        with open(manifest_path) as f:
            manifest = json.load(f)
        manifest_hash = manifest.get("manifest_hash", "")
        n_files = len(manifest.get("files", []))
        log_lines.append(f"Manifest: {n_files} files, hash={manifest_hash[:16]}...")
        logger.info("Bronze complete: %d files, manifest_hash=%s", n_files, manifest_hash[:16])
    else:
        log_lines.append("[CRITICAL] No manifest generated!")
        logger.error("Bronze manifest not found at %s", manifest_path)

    log_lines.append(f"[{datetime.now().isoformat()}] Bronze stage completed")
    with open(log_file, "w") as fh:
        fh.write("\n".join(log_lines))

    if result.returncode != 0:
        raise RuntimeError(f"ToBronze script failed with exit code {result.returncode}")

    return {
        "manifest": manifest_path,
        "log": log_file,
        "manifest_hash": manifest_hash,
        "output_dir": output_dir,
    }
