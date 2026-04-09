"""
Chat-agent tools — callable functions the Marvin Agent can invoke.

Each tool is a plain Python function with type hints and a docstring.
Marvin reads the signature + docstring to decide when to call each one.
"""

import json
import os
from datetime import datetime

import httpx
import pandas as pd

PREFECT_API = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")
DATA_DIR = os.getenv("DATA_DIR", "./data")


# ========================================================================== #
#  Prefect API helpers                                                       #
# ========================================================================== #

def get_pipeline_status() -> str:
    """Get the current pipeline status: last run result, state, and next scheduled run."""
    try:
        r = httpx.post(
            f"{PREFECT_API}/flow_runs/filter",
            json={
                "sort": "EXPECTED_START_TIME_DESC",
                "limit": 5,
            },
            timeout=10,
        )
        r.raise_for_status()
        runs = r.json()
    except Exception as exc:
        return f"Could not reach Prefect API: {exc}"

    if not runs:
        return "No pipeline runs found yet."

    lines = []
    for run in runs:
        name = run.get("name", "unknown")
        state = run.get("state", {}).get("type", "UNKNOWN")
        started = run.get("start_time") or run.get("expected_start_time") or "N/A"
        lines.append(f"  - {name}: {state} (at {started})")

    return "Recent pipeline runs:\n" + "\n".join(lines)


def trigger_pipeline(stage: str = "full") -> str:
    """Trigger a pipeline run. stage can be 'full', 'bronze', 'silver', or 'gold'."""
    try:
        r = httpx.post(
            f"{PREFECT_API}/deployments/filter",
            json={"deployments": {"name": {"any_": ["daily-pipeline"]}}},
            timeout=10,
        )
        r.raise_for_status()
        deployments = r.json()
    except Exception as exc:
        return f"Could not reach Prefect API: {exc}"

    if not deployments:
        return "Deployment 'daily-pipeline' not found. Is the pipeline container running?"

    dep_id = deployments[0]["id"]

    try:
        r = httpx.post(
            f"{PREFECT_API}/deployments/{dep_id}/create_flow_run",
            json={"state": {"type": "SCHEDULED"}},
            timeout=10,
        )
        r.raise_for_status()
        run_id = r.json().get("id", "unknown")
    except Exception as exc:
        return f"Failed to trigger run: {exc}"

    return f"Pipeline run triggered (stage={stage}). Run ID: {run_id}"


# ========================================================================== #
#  Data queries                                                              #
# ========================================================================== #

def get_row_counts() -> str:
    """Get row counts for bronze CSV and silver Parquet files."""
    counts = {}

    bronze_csv = os.path.join(DATA_DIR, "bronze", "sap_extract.csv")
    if os.path.exists(bronze_csv):
        try:
            df = pd.read_csv(bronze_csv)
            counts["bronze (sap_extract.csv)"] = len(df)
        except Exception as e:
            counts["bronze"] = f"error reading: {e}"
    else:
        counts["bronze"] = "no file yet"

    silver_parquet = os.path.join(DATA_DIR, "silver", "cleaned_data.parquet")
    if os.path.exists(silver_parquet):
        try:
            df = pd.read_parquet(silver_parquet)
            counts["silver (cleaned_data.parquet)"] = len(df)
        except Exception as e:
            counts["silver"] = f"error reading: {e}"
    else:
        counts["silver"] = "no file yet"

    return "\n".join(f"  {k}: {v} rows" if isinstance(v, int) else f"  {k}: {v}"
                     for k, v in counts.items())


def preview_data(stage: str = "bronze", rows: int = 5) -> str:
    """Preview the first N rows of a stage's output. stage: 'bronze' or 'silver'."""
    if stage == "bronze":
        path = os.path.join(DATA_DIR, "bronze", "sap_extract.csv")
        if not os.path.exists(path):
            return "No bronze data yet."
        df = pd.read_csv(path)
    elif stage == "silver":
        path = os.path.join(DATA_DIR, "silver", "cleaned_data.parquet")
        if not os.path.exists(path):
            return "No silver data yet."
        df = pd.read_parquet(path)
    else:
        return f"Unknown stage '{stage}'. Use 'bronze' or 'silver'."

    return df.head(rows).to_string(index=False)


# ========================================================================== #
#  Log tools                                                                 #
# ========================================================================== #

def get_latest_logs(stage: str = "bronze") -> str:
    """Read the latest log file for a stage. stage: 'bronze', 'silver', or 'gold'."""
    log_path = os.path.join(DATA_DIR, stage, f"{stage}.log")
    if not os.path.exists(log_path):
        return f"No log file found for stage '{stage}' at {log_path}"

    with open(log_path, "r") as f:
        content = f.read()

    if not content.strip():
        return f"{stage}.log exists but is empty."

    return f"--- {stage}.log ---\n{content[-3000:]}"


def analyse_logs() -> str:
    """Read all available log files and the chain-of-custody manifest.
    Return the raw content so the agent can summarise issues."""
    parts = []

    for stage in ("bronze", "silver", "gold"):
        log_path = os.path.join(DATA_DIR, stage, f"{stage}.log")
        if os.path.exists(log_path):
            with open(log_path, "r") as f:
                content = f.read()
            parts.append(f"--- {stage}.log ---\n{content[-2000:]}")

    custody = os.path.join(DATA_DIR, "chain_of_custody.json")
    if os.path.exists(custody):
        with open(custody, "r") as f:
            data = json.load(f)
        entries = data.get("chain_of_custody", [])[-5:]
        parts.append(f"--- chain_of_custody (last {len(entries)} entries) ---\n"
                     + json.dumps(entries, indent=2))

    if not parts:
        return "No logs or custody data found yet. Has the pipeline run?"

    return "\n\n".join(parts)


# All tools exposed to the agent
ALL_TOOLS = [
    get_pipeline_status,
    trigger_pipeline,
    get_row_counts,
    preview_data,
    get_latest_logs,
    analyse_logs,
]
