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

import os

from prefect import flow, get_run_logger

from tasks.bronze_task import run_bronze
from tasks.silver_task import run_silver
from tasks.gold_task import run_gold
from tasks.overwatcher import overwatcher


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
    logger = get_run_logger()

    # ════════════════════════  BRONZE  ════════════════════════════════════
    logger.info("=" * 60)
    logger.info("STAGE 1 / 3 — BRONZE  (Raw Data Ingestion)")
    logger.info("=" * 60)

    bronze = run_bronze(data_dir=data_dir)

    bronze_check = overwatcher(
        stage="bronze",
        manifest_path=bronze["manifest"],
        log_path=bronze["log"],
        data_dir=data_dir,
        use_ai=use_ai,
        model=ai_model,
    )

    # ════════════════════════  SILVER  ════════════════════════════════════
    logger.info("=" * 60)
    logger.info("STAGE 2 / 3 — SILVER  (Transform & Clean)")
    logger.info("=" * 60)

    silver = run_silver(
        data_dir=data_dir,
        bronze_output_dir=bronze["output_dir"],
        expected_manifest_hash=bronze_check["manifest_hash"],
    )

    silver_check = overwatcher(
        stage="silver",
        manifest_path=silver["manifest"],
        log_path=silver["log"],
        data_dir=data_dir,
        use_ai=use_ai,
        model=ai_model,
    )

    # ════════════════════════  GOLD  ══════════════════════════════════════
    logger.info("=" * 60)
    logger.info("STAGE 3 / 3 — GOLD  (Final Layer Load)")
    logger.info("=" * 60)

    gold = run_gold(
        data_dir=data_dir,
        silver_output_dir=silver["output_dir"],
        expected_manifest_hash=silver_check["manifest_hash"],
    )

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
