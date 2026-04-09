"""
Prefect Orchestrator — Data Pipeline
=====================================

Chains three stages with an AI integrity gatekeeper between each:

    Bronze ──► Overwatcher ──► Silver ──► Overwatcher ──► Gold ──► Overwatcher

Each stage produces a manifest of output files with SHA-256 hashes.
The Overwatcher re-verifies every hash and forwards the manifest hash
to the next stage, which re-checks it before processing — creating an
unbroken chain of custody.

The process stays alive after startup and waits for triggers:
  * Automatic — daily schedule (default 06:00 UTC, configurable via CRON)
  * Manual    — Prefect UI at http://localhost:4200
  * CLI       — prefect deployment run 'data-pipeline/daily-pipeline'

Environment variables (all optional):

    DATA_DIR              — shared data folder          (default: ./data)
    OPENAI_API_KEY        — enables Marvin AI analysis
    MARVIN_AGENT_MODEL    — LLM model for Overwatcher   (default: openai:gpt-4o-mini)
    USE_AI_ANALYSIS       — set to "true" to enable AI  (default: false)
    PIPELINE_CRON         — cron schedule                (default: 0 6 * * *)
"""

import logging
import os
from datetime import datetime, timezone

from prefect import flow, get_run_logger

from tasks.bronze_task import run_bronze
from tasks.silver_task import run_silver
from tasks.gold_task import run_gold
from tasks.overwatcher import overwatcher


def _setup_pipeline_log(data_dir: str) -> logging.FileHandler | None:
    """Create a timestamped pipeline log file and return the handler."""
    logs_dir = os.path.join(data_dir, "pipeline_logs")
    os.makedirs(logs_dir, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M")
    log_path = os.path.join(logs_dir, f"pipeline_{ts}.log")
    handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
    ))
    return handler, log_path


class _PipelineFileLogger:
    """Thin wrapper: writes to both the Prefect run logger and a file."""

    def __init__(self, prefect_logger, file_handler: logging.FileHandler):
        self._prefect = prefect_logger
        self._flog = logging.getLogger("pipeline_run")
        self._flog.setLevel(logging.DEBUG)
        self._flog.addHandler(file_handler)
        self._handler = file_handler

    def info(self, msg, *args):
        self._prefect.info(msg, *args)
        self._flog.info(msg, *args)

    def warning(self, msg, *args):
        self._prefect.warning(msg, *args)
        self._flog.warning(msg, *args)

    def error(self, msg, *args):
        self._prefect.error(msg, *args)
        self._flog.error(msg, *args)

    def close(self):
        self._handler.flush()
        self._flog.removeHandler(self._handler)
        self._handler.close()


@flow(name="data-pipeline", log_prints=True)
def data_pipeline(
    data_dir: str   = os.getenv("DATA_DIR", "./data"),
    use_ai: bool    = os.getenv("USE_AI_ANALYSIS", "").lower() == "true",
    ai_model: str | None = os.getenv("MARVIN_AGENT_MODEL"),
):
    """
    Main orchestrator.

    Every parameter falls back to an environment variable, so the same
    code works unchanged on bare-metal and inside Docker.
    """
    prefect_logger = get_run_logger()
    file_handler, log_file = _setup_pipeline_log(data_dir)
    logger = _PipelineFileLogger(prefect_logger, file_handler)

    logger.info("Pipeline log → %s", log_file)

    # ════════════════════════  BRONZE  ════════════════════════════════════
    logger.info("=" * 60)
    logger.info("STAGE 1 / 3 — BRONZE  (Raw Data Ingestion)")
    logger.info("=" * 60)

    bronze = run_bronze(data_dir=data_dir)
    logger.info("Bronze finished — manifest_hash=%s", bronze.get("manifest_hash", "N/A"))

    bronze_check = overwatcher(
        stage="bronze",
        manifest_path=bronze["manifest"],
        log_path=bronze["log"],
        data_dir=data_dir,
        use_ai=use_ai,
        model=ai_model,
    )
    logger.info("Bronze overwatcher — status=%s, hash=%s",
                bronze_check.get("status"), bronze_check.get("manifest_hash"))

    # ════════════════════════  SILVER  ════════════════════════════════════
    logger.info("=" * 60)
    logger.info("STAGE 2 / 3 — SILVER  (Transform & Clean)")
    logger.info("=" * 60)

    silver = run_silver(
        data_dir=data_dir,
        bronze_output_dir=bronze["output_dir"],
        expected_manifest_hash=bronze_check["manifest_hash"],
    )
    logger.info("Silver finished — manifest_hash=%s", silver.get("manifest_hash", "N/A"))

    silver_check = overwatcher(
        stage="silver",
        manifest_path=silver["manifest"],
        log_path=silver["log"],
        data_dir=data_dir,
        use_ai=use_ai,
        model=ai_model,
    )
    logger.info("Silver overwatcher — status=%s, hash=%s",
                silver_check.get("status"), silver_check.get("manifest_hash"))

    # ════════════════════════  GOLD  ══════════════════════════════════════
    logger.info("=" * 60)
    logger.info("STAGE 3 / 3 — GOLD  (Final Layer Load)")
    logger.info("=" * 60)

    gold = run_gold(
        data_dir=data_dir,
        silver_output_dir=silver["output_dir"],
        expected_manifest_hash=silver_check["manifest_hash"],
    )
    logger.info("Gold finished — manifest_hash=%s", gold.get("manifest_hash", "N/A"))

    overwatcher(
        stage="gold",
        manifest_path=gold["manifest"],
        log_path=gold["log"],
        data_dir=data_dir,
        use_ai=use_ai,
        model=ai_model,
    )

    # ════════════════════════  DONE  ══════════════════════════════════════
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 60)
    logger.close()

    return {"bronze": bronze, "silver": silver, "gold": gold}


# ---------------------------------------------------------------------- #
#  Entry point                                                            #
# ---------------------------------------------------------------------- #

if __name__ == "__main__":
    cron = os.getenv("PIPELINE_CRON", "0 6 * * *")

    print(f"Pipeline serving  — schedule: {cron}")
    print(f"  Manual trigger: prefect deployment run 'data-pipeline/daily-pipeline'")
    print(f"  Dashboard:      http://localhost:4200")
    print()

    data_pipeline.serve(
        name="daily-pipeline",
        cron=cron,
        tags=["production"],
    )
