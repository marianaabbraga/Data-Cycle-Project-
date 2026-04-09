"""
Silver Task — Transform & Clean (Hive-partitioned)
====================================================

Runs ``Dataflows/ToSilver/bronzeToSilverDataFlow.py`` as a subprocess.
Before launching, optionally verifies the bronze output hasn't been
tampered with by re-checking its manifest hash.
"""

import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime

from prefect import task
from prefect.logging import get_run_logger

SCRIPT_REL = os.path.join("Dataflows", "ToSilver", "bronzeToSilverDataFlow.py")


def _verify_input_manifest(manifest_path: str, expected_hash: str, logger):
    """Re-hash every file listed in *manifest_path* and compare to *expected_hash*."""
    with open(manifest_path) as f:
        manifest = json.load(f)

    output_dir = manifest["output_dir"]
    entries = manifest.get("files", [])
    recomputed: list[str] = []

    for entry in sorted(entries, key=lambda e: e["path"]):
        fpath = os.path.join(output_dir, entry["path"])
        if not os.path.isfile(fpath):
            raise FileNotFoundError(
                f"Input verification failed: file listed in manifest is missing: {fpath}"
            )
        h = hashlib.sha256()
        with open(fpath, "rb") as fh:
            for chunk in iter(lambda: fh.read(8192), b""):
                h.update(chunk)
        recomputed.append(f"{entry['path']}:{h.hexdigest()}")

    actual_hash = hashlib.sha256("\n".join(recomputed).encode()).hexdigest()
    if actual_hash != expected_hash:
        raise ValueError(
            f"Input manifest hash mismatch — expected {expected_hash}, "
            f"got {actual_hash}. Bronze output may have been modified."
        )
    logger.info("Input manifest verified: hash=%s (%d files)", expected_hash[:16], len(entries))


@task(name="silver-transform", retries=1, retry_delay_seconds=30)
def run_silver(
    data_dir: str = "./data",
    bronze_output_dir: str | None = None,
    expected_manifest_hash: str | None = None,
) -> dict:
    """
    Execute the Silver stage.

    Parameters
    ----------
    data_dir              : root data directory
    bronze_output_dir     : path to the Bronze lake directory (contains parquets)
    expected_manifest_hash: hash from the bronze overwatcher to verify input integrity

    Returns
    -------
    dict with ``manifest``, ``log``, ``manifest_hash``, ``output_dir``
    """
    logger = get_run_logger()

    silver_dir = os.path.join(data_dir, "silver")
    os.makedirs(silver_dir, exist_ok=True)
    log_file = os.path.join(silver_dir, "silver.log")
    log_lines: list[str] = [f"[{datetime.now().isoformat()}] Silver stage started"]

    if bronze_output_dir is None:
        bronze_output_dir = os.path.abspath(os.path.join(data_dir, "bronze", "lake"))

    # Verify bronze output integrity before processing
    if expected_manifest_hash:
        bronze_manifest = os.path.join(bronze_output_dir, "bronze_manifest.json")
        if os.path.isfile(bronze_manifest):
            _verify_input_manifest(bronze_manifest, expected_manifest_hash, logger)
            log_lines.append(f"Input integrity verified (hash={expected_manifest_hash[:16]}...)")
        else:
            logger.warning("Bronze manifest not found at %s — skipping verification", bronze_manifest)
            log_lines.append(f"[WARNING] Bronze manifest missing, cannot verify input")

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    script = os.path.join(project_root, SCRIPT_REL)

    if not os.path.isfile(script):
        raise FileNotFoundError(f"Silver script not found: {script}")

    output_dir = os.path.abspath(os.path.join(data_dir, "silver", "lake"))
    env = {
        **os.environ,
        "BRONZE_INPUT_DIR": bronze_output_dir,
        "SILVER_OUTPUT_DIR": output_dir,
    }

    logger.info("Running ToSilver script: %s", script)
    log_lines.append(f"Script: {script}")
    log_lines.append(f"Bronze input dir: {bronze_output_dir}")
    log_lines.append(f"Silver output dir: {output_dir}")

    result = subprocess.run(
        [sys.executable, script],
        cwd=project_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=900,
    )

    log_lines.append(f"--- stdout ---\n{result.stdout}")
    if result.stderr:
        log_lines.append(f"--- stderr ---\n{result.stderr}")
    log_lines.append(f"Exit code: {result.returncode}")

    if result.returncode != 0:
        log_lines.append(f"[ERROR] ToSilver exited with code {result.returncode}")
        logger.error("ToSilver failed (exit %d): %s", result.returncode, result.stderr[:500])

    manifest_path = os.path.join(output_dir, "silver_manifest.json")
    manifest_hash = ""
    if os.path.isfile(manifest_path):
        with open(manifest_path) as f:
            manifest = json.load(f)
        manifest_hash = manifest.get("manifest_hash", "")
        n_files = len(manifest.get("files", []))
        log_lines.append(f"Manifest: {n_files} files, hash={manifest_hash[:16]}...")
        logger.info("Silver complete: %d files, manifest_hash=%s", n_files, manifest_hash[:16])
    else:
        log_lines.append("[CRITICAL] No manifest generated!")
        logger.error("Silver manifest not found at %s", manifest_path)

    log_lines.append(f"[{datetime.now().isoformat()}] Silver stage completed")
    with open(log_file, "w") as fh:
        fh.write("\n".join(log_lines))

    if result.returncode != 0:
        raise RuntimeError(f"ToSilver script failed with exit code {result.returncode}")

    return {
        "manifest": manifest_path,
        "log": log_file,
        "manifest_hash": manifest_hash,
        "output_dir": output_dir,
    }
