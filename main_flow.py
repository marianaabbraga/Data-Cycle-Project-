"""
Prefect Orchestrator — Data Pipeline
=====================================

Chains three stages with an AI integrity gatekeeper between each:

    Bronze  ──►  Overwatcher  ──►  Silver  ──►  Overwatcher  ──►  Gold

The process stays alive after startup and waits for triggers:
  • Automatic — daily schedule (default 06:00 UTC, configurable via CRON)
  • Manual    — Prefect UI at http://localhost:4200
  • CLI       — prefect deployment run 'data-pipeline/daily-pipeline'

Environment variables (all optional):

    DATA_DIR              — shared data folder          (default: ./data)
    DB_SERVER             — SQL Server host              (default: localhost)
    DB_NAME               — target database              (default: DataCycle)
    DB_USER / DB_PASSWORD — SQL credentials              (or TRUSTED_CONNECTION=true)
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
    data_dir: str            = os.getenv("DATA_DIR", "./data"),
    db_server: str           = os.getenv("DB_SERVER", "localhost"),
    db_name: str             = os.getenv("DB_NAME", "DataCycle"),
    db_user: str             = os.getenv("DB_USER", "sa"),
    db_password: str | None  = os.getenv("DB_PASSWORD"),
    trusted_connection: bool = os.getenv("TRUSTED_CONNECTION", "").lower() == "true",
    sap_script: str | None   = os.getenv("SAP_SCRIPT"),
    knime_batch: str | None  = os.getenv("KNIME_BATCH"),
    use_ai: bool             = os.getenv("USE_AI_ANALYSIS", "").lower() == "true",
    ai_model: str | None     = os.getenv("MARVIN_AGENT_MODEL"),
):
    """
    Main orchestrator.

    Every parameter falls back to an environment variable, so the same
    code works unchanged on bare-metal Windows *and* inside Docker.
    """
    logger = get_run_logger()

    # ── Resolve DB password from Prefect Secrets if needed ────────────────
    db_pass = db_password
    if not db_pass and not trusted_connection:
        db_pass = _try_prefect_secret("db-password", logger)

    # ════════════════════════  BRONZE  ════════════════════════════════════
    logger.info("=" * 60)
    logger.info("STAGE 1 / 3 — BRONZE  (SAP Extraction)")
    logger.info("=" * 60)

    bronze = run_bronze(data_dir=data_dir, sap_script=sap_script)

    overwatcher(
        stage="bronze",
        file_path=bronze["file"],
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
        bronze_file=bronze["file"],
        knime_batch=knime_batch,
    )

    overwatcher(
        stage="silver",
        file_path=silver["file"],
        log_path=silver["log"],
        data_dir=data_dir,
        use_ai=use_ai,
        model=ai_model,
    )

    # ════════════════════════  GOLD  ══════════════════════════════════════
    logger.info("=" * 60)
    logger.info("STAGE 3 / 3 — GOLD  (SQL Server Load)")
    logger.info("=" * 60)

    gold = run_gold(
        data_dir=data_dir,
        silver_file=silver["file"],
        db_server=db_server,
        db_name=db_name,
        db_user=db_user,
        db_password=db_pass,
        trusted=trusted_connection,
    )

    # ════════════════════════  DONE  ══════════════════════════════════════
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 60)

    return {"bronze": bronze, "silver": silver, "gold": gold}


# ---------------------------------------------------------------------- #
#  Helpers                                                                #
# ---------------------------------------------------------------------- #

def _try_prefect_secret(name: str, logger) -> str | None:
    """Attempt to load a Prefect Secret Block; return None on failure."""
    try:
        from prefect.blocks.system import Secret
        return Secret.load(name).get()
    except Exception:
        logger.warning(f"Prefect Secret '{name}' not found — continuing without it")
        return None


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
