"""
Overwatcher — AI-driven Integrity Agent (powered by Marvin)
===========================================================

Sits between every pipeline stage and acts as a gatekeeper:
1.  Computes a SHA-256 hash of the stage's output file.
2.  Optionally verifies the hash against an expected value.
3.  Reads the stage's .log file and hands it to a Marvin AI function
    for analysis.  Falls back to rule-based scanning when no API key
    is configured.
4.  Records every check in a Chain-of-Custody manifest (JSON).
5.  Raises on failure — blocking the pipeline before bad data
    reaches downstream stages or the database.

Configuration (env vars):
    OPENAI_API_KEY          — required for AI analysis
    MARVIN_AGENT_MODEL      — model to use (default: openai:gpt-4o-mini)
"""

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import TypedDict

from prefect import task
from prefect.logging import get_run_logger

# Keywords that the rule-based fallback scans for
_CRITICAL_PATTERNS = [
    "FATAL", "CRITICAL", "Traceback", "Exception:",
    "PermissionError", "ConnectionRefused", "0 rows",
]


# ========================================================================== #
#  Marvin AI log analysis                                                    #
# ========================================================================== #

class LogVerdict(TypedDict):
    status: str    # "OK" or "CRITICAL"
    issues: list[str]


def _configure_marvin(model: str | None = None):
    """Set the Marvin model at runtime.  Reads MARVIN_AGENT_MODEL env var
    if *model* is not provided explicitly."""
    import marvin

    target = model or os.getenv("MARVIN_AGENT_MODEL", "openai:gpt-4o-mini")
    marvin.settings.agent_model = target


def _analyse_log_marvin(log_content: str, model: str | None = None) -> LogVerdict:
    """Use Marvin's @fn decorator to analyse a pipeline log via LLM."""
    import marvin

    _configure_marvin(model)

    @marvin.fn
    def analyse_pipeline_log(log_text: str) -> LogVerdict:
        """You are a data-pipeline integrity monitor.

        Analyse the pipeline log below and look for:
          - Errors, exceptions, or stack traces
          - Data-quality warnings (missing values, type mismatches, encoding)
          - Unexpected row counts or empty outputs
          - Permission / access / timeout / connection errors

        Return:
          status  — "OK" if nothing is wrong, "CRITICAL" if problems found
          issues  — list of short issue descriptions (empty when status is OK)
        """

    return analyse_pipeline_log(log_content[-4000:])


def _analyse_log_rules(log_content: str) -> LogVerdict:
    """Rule-based fallback when no API key is available."""
    issues: list[str] = []
    for line in log_content.splitlines():
        for kw in _CRITICAL_PATTERNS:
            if kw.lower() in line.lower():
                issues.append(line.strip())
                break
    return LogVerdict(
        status="CRITICAL" if issues else "OK",
        issues=issues,
    )


# ========================================================================== #
#  Hashing                                                                   #
# ========================================================================== #

def compute_sha256(file_path: str) -> str:
    """Return the hex-encoded SHA-256 digest of *file_path*."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ========================================================================== #
#  Chain-of-Custody manifest                                                 #
# ========================================================================== #

def _manifest_path(data_dir: str) -> str:
    return os.path.join(data_dir, "chain_of_custody.json")


def _load_manifest(data_dir: str) -> dict:
    path = _manifest_path(data_dir)
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {"chain_of_custody": []}


def _save_manifest(data_dir: str, manifest: dict):
    path = _manifest_path(data_dir)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(manifest, f, indent=2)


# ========================================================================== #
#  Prefect task                                                              #
# ========================================================================== #

@task(name="overwatcher-integrity-check", retries=0)
def overwatcher(
    stage: str,
    file_path: str,
    log_path: str,
    data_dir: str = "./data",
    expected_hash: str | None = None,
    use_ai: bool = True,
    model: str | None = None,
) -> dict:
    """
    Gatekeeper task invoked after every pipeline stage.

    Parameters
    ----------
    stage         : human label for the stage ("bronze", "silver", …)
    file_path     : path to the stage's output file
    log_path      : path to the stage's .log file
    data_dir      : root data directory (for the custody manifest)
    expected_hash : optional expected SHA-256 hex digest
    use_ai        : if True *and* OPENAI_API_KEY is set, use Marvin;
                    otherwise fall back to rule-based analysis
    model         : override the LLM model (e.g. "openai:gpt-4o-mini")

    Returns
    -------
    dict with ``stage``, ``hash``, and ``status`` on success.
    Raises on hash mismatch or critical log findings.
    """
    logger = get_run_logger()
    logger.info(f"Overwatcher checking [{stage}] -> {file_path}")

    # 1. File must exist -------------------------------------------------
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"[{stage}] output file missing: {file_path}")

    # 2. SHA-256 hash -----------------------------------------------------
    sha = compute_sha256(file_path)
    logger.info(f"SHA-256: {sha}")

    if expected_hash and sha != expected_hash:
        raise ValueError(
            f"[{stage}] Hash mismatch — expected {expected_hash}, got {sha}. "
            "Data may have been tampered with."
        )

    # 3. Log analysis -----------------------------------------------------
    analysis: LogVerdict = {"status": "OK", "issues": []}

    if os.path.exists(log_path):
        with open(log_path, "r") as f:
            log_content = f.read()

        api_key = os.getenv("OPENAI_API_KEY")

        if use_ai and api_key:
            logger.info("Using Marvin AI for log analysis")
            try:
                analysis = _analyse_log_marvin(log_content, model)
            except Exception as exc:
                logger.warning(f"Marvin analysis failed ({exc}), falling back to rules")
                analysis = _analyse_log_rules(log_content)
        else:
            if use_ai:
                logger.info("OPENAI_API_KEY not set — using rule-based analysis")
            else:
                logger.info("AI disabled — using rule-based analysis")
            analysis = _analyse_log_rules(log_content)
    else:
        logger.warning(f"No log file at {log_path}")

    # 4. Record in chain of custody ---------------------------------------
    manifest = _load_manifest(data_dir)
    manifest["chain_of_custody"].append({
        "stage": stage,
        "file": file_path,
        "sha256": sha,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "log_analysis": analysis["status"],
        "issues": analysis.get("issues", []),
        "verdict": "PASSED" if analysis["status"] == "OK" else "FAILED",
    })
    _save_manifest(data_dir, manifest)

    # 5. Gate decision ----------------------------------------------------
    if analysis["status"] == "CRITICAL":
        msg = (
            f"[{stage}] Overwatcher BLOCKED the pipeline.\n"
            + "\n".join(f"  - {i}" for i in analysis["issues"])
        )
        logger.error(msg)
        raise RuntimeError(msg)

    logger.info(f"[{stage}] passed integrity check")
    return {"stage": stage, "hash": sha, "status": "PASSED"}
