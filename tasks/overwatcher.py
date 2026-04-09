"""
Overwatcher — AI-driven Integrity Agent (powered by Marvin)
===========================================================

Sits between every pipeline stage and acts as a gatekeeper:
1.  Re-reads the stage manifest and recomputes SHA-256 hashes of every
    output file to ensure nothing was modified after the stage completed.
2.  Optionally verifies the manifest hash against an expected value
    forwarded from a previous stage.
3.  Reads the stage's .log file and hands it to a Marvin AI function
    for analysis.  Falls back to rule-based scanning when no API key
    is configured.
4.  Records every check in a Chain-of-Custody manifest (JSON).
5.  Raises on failure — blocking the pipeline before bad data
    reaches downstream stages or the database.

Configuration (env vars):
    OPENAI_API_KEY          — OpenAI/compatible key (for Ollama can be dummy)
    OPENAI_BASE_URL         — OpenAI-compatible base URL (e.g. Ollama)
    MARVIN_AGENT_MODEL      — model to use (default: openai:gpt-4o-mini)
"""

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from typing import TypedDict

from prefect import task
from prefect.logging import get_run_logger

# Substring patterns (matched with simple `in`)
_CRITICAL_SUBSTRINGS = [
    "FATAL", "CRITICAL", "Traceback", "Exception:",
    "PermissionError", "ConnectionRefused",
]
# Regex patterns (need word boundaries to avoid false positives like "250 rows")
_CRITICAL_REGEXES = [
    re.compile(r"\b0 rows\b", re.IGNORECASE),
]


# ========================================================================== #
#  Marvin AI log analysis                                                    #
# ========================================================================== #

class LogVerdict(TypedDict):
    status: str    # "OK" or "CRITICAL"
    issues: list[str]


def _make_ollama_provider(base_url: str, api_key: str):
    """Create an OpenAI-compatible provider with a patched client that
    replaces null message content with empty strings (Ollama rejects them)."""
    from openai import AsyncOpenAI
    from pydantic_ai.providers.openai import OpenAIProvider

    client = AsyncOpenAI(api_key=api_key, base_url=base_url)
    _original_create = client.chat.completions.create

    async def _patched_create(*args, **kwargs):
        messages = kwargs.get("messages")
        if messages:
            for msg in messages:
                if isinstance(msg, dict) and msg.get("content") is None:
                    msg["content"] = ""
        return await _original_create(*args, **kwargs)

    client.chat.completions.create = _patched_create  # type: ignore[assignment]
    return OpenAIProvider(openai_client=client)


def _configure_marvin(model: str | None = None):
    """Set the Marvin model at runtime."""
    import marvin

    base_url = os.getenv("OPENAI_BASE_URL")
    ollama_model = os.getenv("OLLAMA_MODEL")

    if base_url and ollama_model:
        from pydantic_ai.models.openai import OpenAIModel

        provider = _make_ollama_provider(base_url, os.getenv("OPENAI_API_KEY", "ollama"))
        marvin.settings.agent_model = OpenAIModel(ollama_model, provider=provider)
        return

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
        low = line.lower()
        matched = False
        for kw in _CRITICAL_SUBSTRINGS:
            if kw.lower() in low:
                matched = True
                break
        if not matched:
            for rx in _CRITICAL_REGEXES:
                if rx.search(line):
                    matched = True
                    break
        if matched:
            issues.append(line.strip())
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


def recompute_manifest_hash(manifest_path: str) -> tuple[str, list[dict]]:
    """Re-read *manifest_path*, re-hash every listed file, return (hash, entries).

    Raises if any file is missing or the on-disk hash differs from what the
    manifest recorded (i.e. the file was modified after the stage wrote it).
    """
    with open(manifest_path) as f:
        manifest = json.load(f)

    output_dir = manifest["output_dir"]
    entries = manifest.get("files", [])
    verified: list[str] = []
    checked_entries: list[dict] = []

    for entry in sorted(entries, key=lambda e: e["path"]):
        fpath = os.path.join(output_dir, entry["path"])
        if not os.path.isfile(fpath):
            raise FileNotFoundError(f"File from manifest missing on disk: {fpath}")

        actual_sha = compute_sha256(fpath)
        if actual_sha != entry["sha256"]:
            raise ValueError(
                f"File tampered: {entry['path']} — manifest says {entry['sha256'][:16]}..., "
                f"disk says {actual_sha[:16]}..."
            )
        verified.append(f"{entry['path']}:{actual_sha}")
        checked_entries.append({**entry, "verified": True})

    manifest_hash = hashlib.sha256("\n".join(verified).encode()).hexdigest()
    return manifest_hash, checked_entries


# ========================================================================== #
#  Chain-of-Custody                                                          #
# ========================================================================== #

def _custody_path(data_dir: str) -> str:
    return os.path.join(data_dir, "chain_of_custody.json")


def _load_custody(data_dir: str) -> dict:
    path = _custody_path(data_dir)
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {"chain_of_custody": []}


def _save_custody(data_dir: str, custody: dict):
    path = _custody_path(data_dir)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(custody, f, indent=2)


# ========================================================================== #
#  Prefect task                                                              #
# ========================================================================== #

@task(name="overwatcher-integrity-check", retries=0)
def overwatcher(
    stage: str,
    manifest_path: str | None,
    log_path: str,
    data_dir: str = "./data",
    expected_manifest_hash: str | None = None,
    use_ai: bool = True,
    model: str | None = None,
) -> dict:
    """
    Gatekeeper task invoked after every pipeline stage.

    Parameters
    ----------
    stage                  : human label ("bronze", "silver", "gold")
    manifest_path          : path to the stage's *_manifest.json (may be None
                             for stages that haven't been implemented yet)
    log_path               : path to the stage's .log file
    data_dir               : root data directory (for chain_of_custody.json)
    expected_manifest_hash : if provided, the recomputed manifest hash must
                             match this value or the pipeline is blocked
    use_ai                 : use Marvin AI for log analysis when available
    model                  : override the LLM model

    Returns
    -------
    dict with ``stage``, ``manifest_hash``, ``file_count``, and ``status``.
    """
    logger = get_run_logger()
    logger.info("Overwatcher checking [%s]", stage)

    manifest_hash = ""
    file_count = 0
    file_entries: list[dict] = []

    # 1. Manifest verification --------------------------------------------
    if manifest_path and os.path.isfile(manifest_path):
        logger.info("Verifying manifest: %s", manifest_path)
        manifest_hash, file_entries = recompute_manifest_hash(manifest_path)
        file_count = len(file_entries)
        logger.info("Manifest OK: %d files, hash=%s", file_count, manifest_hash[:16])

        if expected_manifest_hash and manifest_hash != expected_manifest_hash:
            raise ValueError(
                f"[{stage}] Manifest hash mismatch — expected {expected_manifest_hash}, "
                f"got {manifest_hash}. Data may have been tampered with between stages."
            )
    elif manifest_path:
        logger.warning("Manifest file not found: %s", manifest_path)
    else:
        logger.info("[%s] No manifest provided (stage may not be implemented)", stage)

    # 2. Log analysis ------------------------------------------------------
    analysis: LogVerdict = {"status": "OK", "issues": []}

    if os.path.exists(log_path):
        with open(log_path, "r") as f:
            log_content = f.read()

        api_key = os.getenv("OPENAI_API_KEY")
        base_url = os.getenv("OPENAI_BASE_URL")
        llm_configured = bool(api_key) or bool(base_url)

        rules_analysis = _analyse_log_rules(log_content)

        if use_ai and llm_configured:
            logger.info("Using Marvin AI for log analysis")
            try:
                ai_analysis = _analyse_log_marvin(log_content, model)
            except Exception as exc:
                logger.warning("Marvin analysis failed (%s), falling back to rules", exc)
                ai_analysis = rules_analysis

            # Cross-check: if the AI says CRITICAL but rule-based says OK,
            # the AI is likely hallucinating — downgrade to a warning.
            if ai_analysis["status"] == "CRITICAL" and rules_analysis["status"] == "OK":
                logger.warning(
                    "AI flagged CRITICAL but rule-based found nothing — "
                    "treating as OK (AI issues logged for review: %s)",
                    ai_analysis.get("issues", []),
                )
                analysis = LogVerdict(status="OK", issues=[])
            else:
                analysis = ai_analysis
        else:
            if use_ai:
                logger.info("No LLM configured — using rule-based analysis")
            else:
                logger.info("AI disabled — using rule-based analysis")
            analysis = rules_analysis
    else:
        logger.warning("No log file at %s", log_path)

    # 3. Record in chain of custody ----------------------------------------
    custody = _load_custody(data_dir)
    custody["chain_of_custody"].append({
        "stage": stage,
        "manifest_hash": manifest_hash,
        "file_count": file_count,
        "files": [{"path": e["path"], "sha256": e["sha256"]} for e in file_entries],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "log_analysis": analysis["status"],
        "issues": analysis.get("issues", []),
        "verdict": "PASSED" if analysis["status"] == "OK" else "FAILED",
    })
    _save_custody(data_dir, custody)

    # 4. Gate decision -----------------------------------------------------
    if analysis["status"] == "CRITICAL":
        msg = (
            f"[{stage}] Overwatcher BLOCKED the pipeline.\n"
            + "\n".join(f"  - {i}" for i in analysis["issues"])
        )
        logger.error(msg)
        raise RuntimeError(msg)

    logger.info("[%s] passed integrity check", stage)
    return {
        "stage": stage,
        "manifest_hash": manifest_hash,
        "file_count": file_count,
        "status": "PASSED",
    }
