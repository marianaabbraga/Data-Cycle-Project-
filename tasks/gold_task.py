"""
Gold Task — Final Layer Load
=============================

Runs ``Dataflows/ToGold/silverToGoldDataFlow.py`` as a subprocess.
If the script is empty / not implemented yet, the task logs a warning
and returns gracefully so the pipeline doesn't crash.

Before launching, optionally verifies the silver output hasn't been
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

SCRIPT_REL = os.path.join("Dataflows", "ToGold", "silverToGoldDataFlow.py")


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
            f"got {actual_hash}. Silver output may have been modified."
        )
    logger.info("Input manifest verified: hash=%s (%d files)", expected_hash[:16], len(entries))


@task(name="gold-load", retries=1, retry_delay_seconds=30)
def run_gold(
    data_dir: str = "./data",
    silver_output_dir: str | None = None,
    expected_manifest_hash: str | None = None,
) -> dict:
    """
    Execute the Gold stage.

    Parameters
    ----------
    data_dir              : root data directory
    silver_output_dir     : path to the Silver lake directory
    expected_manifest_hash: hash from the silver overwatcher to verify input integrity

    Returns
    -------
    dict with ``manifest``, ``log``, ``manifest_hash``, ``output_dir``
    """
    logger = get_run_logger()

    gold_dir = os.path.join(data_dir, "gold")
    os.makedirs(gold_dir, exist_ok=True)
    log_file = os.path.join(gold_dir, "gold.log")
    log_lines: list[str] = [f"[{datetime.now().isoformat()}] Gold stage started"]

    if silver_output_dir is None:
        silver_output_dir = os.path.abspath(os.path.join(data_dir, "silver", "lake"))

    # Verify silver output integrity before processing
    if expected_manifest_hash:
        silver_manifest = os.path.join(silver_output_dir, "silver_manifest.json")
        if os.path.isfile(silver_manifest):
            _verify_input_manifest(silver_manifest, expected_manifest_hash, logger)
            log_lines.append(f"Input integrity verified (hash={expected_manifest_hash[:16]}...)")
        else:
            logger.warning("Silver manifest not found at %s — skipping verification", silver_manifest)
            log_lines.append("[WARNING] Silver manifest missing, cannot verify input")

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    script = os.path.join(project_root, SCRIPT_REL)

    if not os.path.isfile(script):
        msg = f"Gold script not found: {script}"
        logger.warning(msg)
        log_lines.append(f"[WARNING] {msg} — skipping Gold stage")
        with open(log_file, "w") as fh:
            fh.write("\n".join(log_lines))
        return {"manifest": None, "log": log_file, "manifest_hash": "", "output_dir": ""}

    # Check if script is empty (not implemented yet)
    if os.path.getsize(script) == 0:
        logger.warning("ToGold script is empty — Gold stage not yet implemented, skipping")
        log_lines.append("[WARNING] ToGold script is empty — skipping")
        log_lines.append(f"[{datetime.now().isoformat()}] Gold stage skipped (not implemented)")
        with open(log_file, "w") as fh:
            fh.write("\n".join(log_lines))
        return {"manifest": None, "log": log_file, "manifest_hash": "", "output_dir": ""}

    output_dir = os.path.abspath(os.path.join(data_dir, "gold", "lake"))
    env = {
        **os.environ,
        "SILVER_INPUT_DIR": silver_output_dir,
        "GOLD_OUTPUT_DIR": output_dir,
    }

    logger.info("Running ToGold script: %s", script)
    log_lines.append(f"Script: {script}")
    log_lines.append(f"Silver input dir: {silver_output_dir}")
    log_lines.append(f"Gold output dir: {output_dir}")

    result = subprocess.run(
        [sys.executable, script],
        cwd=project_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
    )

    log_lines.append(f"--- stdout ---\n{result.stdout}")
    if result.stderr:
        log_lines.append(f"--- stderr ---\n{result.stderr}")
    log_lines.append(f"Exit code: {result.returncode}")

    if result.returncode != 0:
        log_lines.append(f"[ERROR] ToGold exited with code {result.returncode}")
        logger.error("ToGold failed (exit %d): %s", result.returncode, result.stderr[:500])

    manifest_path = os.path.join(output_dir, "gold_manifest.json")
    manifest_hash = ""
    if os.path.isfile(manifest_path):
        with open(manifest_path) as f:
            manifest = json.load(f)
        manifest_hash = manifest.get("manifest_hash", "")
        n_files = len(manifest.get("files", []))
        log_lines.append(f"Manifest: {n_files} files, hash={manifest_hash[:16]}...")
        logger.info("Gold complete: %d files, manifest_hash=%s", n_files, manifest_hash[:16])
    else:
        log_lines.append("[WARNING] No manifest generated by ToGold")

    log_lines.append(f"[{datetime.now().isoformat()}] Gold stage completed")
    with open(log_file, "w") as fh:
        fh.write("\n".join(log_lines))

    if result.returncode != 0:
        logger.warning(
            "ToGold exited with code %d — Gold stage failed but pipeline will continue. "
            "Check %s for details.", result.returncode, log_file,
        )

    return {
        "manifest": manifest_path if os.path.isfile(manifest_path) else None,
        "log": log_file,
        "manifest_hash": manifest_hash,
        "output_dir": output_dir,
    }
