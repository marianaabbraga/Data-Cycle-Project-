# Prefect Data Pipeline — Documentation

## Architecture

```
┌──────────┐     ┌─────────────┐     ┌──────────┐     ┌─────────────┐     ┌──────────┐     ┌─────────────┐
│  BRONZE  │────►│ OVERWATCHER │────►│  SILVER  │────►│ OVERWATCHER │────►│   GOLD   │────►│ OVERWATCHER │
│ yfinance │     │ SHA + AI    │     │ Hive/Pq  │     │ SHA + AI    │     │ (stub)   │     │ SHA + AI    │
└──────────┘     └─────────────┘     └──────────┘     └─────────────┘     └──────────┘     └─────────────┘
       │                                    │                                    │
       └────────────── ./data ──────────────┘────────────────────────────────────┘
                   (shared bind mount)
```

| Stage | Task | Dataflow Script | Input | Output |
|-------|------|-----------------|-------|--------|
| Bronze | `tasks/bronze_task.py` | `Dataflows/ToBronze/sourceToBronzeDataFlow.py` | yfinance API | `data/bronze/lake/**/*.parquet` |
| Silver | `tasks/silver_task.py` | `Dataflows/ToSilver/bronzeToSilverDataFlow.py` | Bronze parquets | `data/silver/lake/**/*.parquet` (Hive-partitioned) |
| Gold   | `tasks/gold_task.py` | `Dataflows/ToGold/silverToGoldDataFlow.py` | Silver parquets | **Not implemented yet** — skips gracefully |
| Overwatcher | `tasks/overwatcher.py` | — | Stage manifest + log | `data/chain_of_custody.json` |

Each Prefect task runs its corresponding Dataflow script as a **subprocess**.
The scripts produce parquet files and a `<stage>_manifest.json` that lists
every output file with its SHA-256 hash.

> **Placeholder note — SAP & KNIME:**
> The Bronze stage currently ingests data from yfinance. In a production
> environment this would be replaced by an SAP extraction script (passed
> via `SAP_SCRIPT` env var). Similarly, the Silver stage could optionally
> run a KNIME batch workflow before the Python cleaning step (via
> `KNIME_BATCH` env var). Both are **placeholders for future integration**
> and are not wired up at present.

---

## Hash-Chain Verification

Every stage produces a **manifest** (`bronze_manifest.json`, `silver_manifest.json`)
containing SHA-256 hashes for every output parquet file plus an overall
`manifest_hash`.

The data flows through a chain of custody:

1. **Bronze** runs → writes parquets + manifest
2. **Overwatcher** re-hashes every file, verifies they match the manifest,
   records result in `chain_of_custody.json`, returns `manifest_hash`
3. **Silver** receives `manifest_hash` from the overwatcher, re-verifies the
   bronze files before processing, then writes its own parquets + manifest
4. **Overwatcher** verifies the silver manifest the same way
5. **Gold** receives the silver `manifest_hash`, verifies input, then runs

If any file was modified between stages, the hash won't match and the
pipeline stops immediately.

---

## Quick Start — Docker (recommended)

**Prerequisites:** Docker & Docker Compose installed.

```bash
docker compose -f docker-compose.prefect.yml up --build
```

This will:
1. Start a SQL Server 2022 container on port `1433`
2. Create the `DataCycle` database automatically
3. Run Bronze → Overwatcher → Silver → Overwatcher → Gold → Overwatcher

Data is written to `./data/` on your host via bind mount.

### AI analysis (Ollama / OpenAI)

```bash
# Using local Ollama
OPENAI_BASE_URL=http://host.docker.internal:11434/v1 \
OPENAI_API_KEY=ollama \
OLLAMA_MODEL=qwen2.5:7b-instruct \
USE_AI_ANALYSIS=true \
docker compose -f docker-compose.prefect.yml up --build
```

---

## Quick Start — Local (no Docker)

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Run

```bash
python main_flow.py
```

---

## Chain of Custody

Every time the **Overwatcher** runs, it:

1. **Re-reads the manifest** produced by the stage
2. **Re-hashes every file** listed in the manifest and compares to the recorded hash
3. **Computes a manifest hash** over all file hashes
4. **Compares** against the expected manifest hash (forwarded from the previous stage)
5. **Analyses the log** via AI (Marvin) or rule-based keyword scanning
6. **Records** the result in `data/chain_of_custody.json`:

```json
{
  "chain_of_custody": [
    {
      "stage": "bronze",
      "manifest_hash": "a1b2c3d4...",
      "file_count": 15,
      "files": [{"path": "ticker/AAPL/AAPL_prices_....parquet", "sha256": "..."}],
      "timestamp": "2026-04-09T12:00:00+00:00",
      "log_analysis": "OK",
      "issues": [],
      "verdict": "PASSED"
    }
  ]
}
```

If any file hash doesn't match or the log analysis finds critical errors,
the Overwatcher **raises an exception** and the Prefect flow stops.

---

## Overwatcher — AI Log Analysis (Marvin)

The Overwatcher uses [Marvin](https://askmarvin.ai/) for AI-powered log
analysis. Two modes:

| Mode | When | How |
|------|------|-----|
| **AI (Marvin)** | `OPENAI_API_KEY` or `OPENAI_BASE_URL` is set + `USE_AI_ANALYSIS=true` | Sends last 4000 chars of the log to the configured model |
| **Rule-based** | No LLM configured or AI disabled | Scans for keywords: `FATAL`, `CRITICAL`, `Traceback`, `Exception:`, `PermissionError`, `ConnectionRefused`, `0 rows` |

If the Marvin call fails, the Overwatcher falls back to rule-based analysis.

---

## Chat Agent

A conversational AI agent at **http://localhost:8501** after `docker compose up`.

Questions you can ask:
- "Did the last run succeed?"
- "How many rows are in silver?"
- "Show me the bronze data"
- "Run the pipeline now"
- "What went wrong in the last run?"

The UI also has a **Run Pipeline** button for manual triggers.

---

## Project Structure

```
├── main_flow.py                 # Prefect flow orchestrator
├── Dataflows/
│   ├── ToBronze/
│   │   └── sourceToBronzeDataFlow.py   # yfinance raw ingestion
│   ├── ToSilver/
│   │   └── bronzeToSilverDataFlow.py   # Cleaning, indicators, Hive partitioning
│   └── ToGold/
│       └── silverToGoldDataFlow.py     # (empty — not yet implemented)
├── tasks/
│   ├── __init__.py
│   ├── bronze_task.py           # Prefect task: subprocess ToBronze
│   ├── silver_task.py           # Prefect task: subprocess ToSilver
│   ├── gold_task.py             # Prefect task: subprocess ToGold (graceful no-op)
│   └── overwatcher.py           # Integrity gatekeeper (manifest hashing + AI)
├── agent/
│   ├── __init__.py
│   ├── tools.py                 # Tool functions (Prefect API, file reads)
│   ├── chat_agent.py            # Marvin Agent definition
│   ├── cli.py                   # Terminal chat loop
│   └── web.py                   # FastAPI web chat UI (port 8501)
├── Converters/
│   └── fileConverter.py         # CSV ↔ Parquet converter utility
├── Dockerfile.prefect           # Container image
├── docker-compose.prefect.yml   # SQL Server + pipeline + chat agent
├── data/                        # Shared data directory (bind-mounted by Docker)
│   ├── bronze/
│   │   ├── lake/                # yfinance parquets + bronze_manifest.json
│   │   └── bronze.log
│   ├── silver/
│   │   ├── lake/                # Hive-partitioned parquets + silver_manifest.json
│   │   └── silver.log
│   ├── gold/
│   │   └── gold.log
│   └── chain_of_custody.json
└── requirements.txt
```

---

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Subprocess** for Dataflow scripts | Keeps existing extraction/transform logic intact — Prefect orchestrates, doesn't rewrite |
| **Manifest-based hashing** | Each stage produces many parquet files; a manifest covers them all with a single verifiable hash |
| **Hash chain between stages** | Overwatcher hash → next stage verifies input → detects any tampering between stages |
| **Bind mounts** over cloud storage | Simplicity for POC; scripts read/write a shared `./data` folder |
| **Configurable paths via env vars** | Scripts work both standalone (original relative paths) and inside Docker |
| **Graceful Gold no-op** | `silverToGoldDataFlow.py` is empty; the task detects this and skips instead of crashing |
| **SAP / KNIME as placeholders** | Future integration points; Bronze currently uses yfinance, Silver uses pure Python |
| **Marvin** for AI agent | Built by Prefect team; function + docstring = LLM call; zero boilerplate |
