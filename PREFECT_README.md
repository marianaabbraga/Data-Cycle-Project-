# Prefect Data Pipeline — Documentation

## Architecture

```
┌──────────┐     ┌─────────────┐     ┌──────────┐     ┌─────────────┐     ┌──────────┐
│  BRONZE  │────►│ OVERWATCHER │────►│  SILVER  │────►│ OVERWATCHER │────►│   GOLD   │
│ SAP CSV  │     │ SHA + AI    │     │ Parquet  │     │ SHA + AI    │     │ SQL Srv  │
└──────────┘     └─────────────┘     └──────────┘     └─────────────┘     └──────────┘
       │                                    │                                    │
       └────────────── ./data ──────────────┘────────────────────────────────────┘
                   (shared bind mount)
```

| Stage | Script | Input | Output |
|-------|--------|-------|--------|
| Bronze | `tasks/bronze_task.py` | SAP extraction (mocked) | `data/bronze/sap_extract.csv` |
| Silver | `tasks/silver_task.py` | Bronze CSV (+ optional KNIME) | `data/silver/cleaned_data.parquet` |
| Gold   | `tasks/gold_task.py` | Silver Parquet | SQL Server `dbo.material_movements` |
| Overwatcher | `tasks/overwatcher.py` | Stage output + log | Chain-of-custody manifest |

The **Overwatcher** sits between every stage. It computes a SHA-256 hash
of the output file, analyses the stage log for anomalies, and blocks the
pipeline if anything looks wrong — so no bad data ever reaches the database.

---

## Quick Start — Docker (recommended)

**Prerequisites:** Docker & Docker Compose installed.

```bash
# Start SQL Server + run the full pipeline
docker compose -f docker-compose.prefect.yml up --build
```

This will:
1. Start a SQL Server 2022 container on port `1433`
2. Create the `DataCycle` database automatically
3. Run Bronze → Overwatcher → Silver → Overwatcher → Gold

Data is written to `./data/` on your host via bind mount.

### Custom passwords / AI analysis / model

```bash
# Override defaults via env vars
MSSQL_SA_PASSWORD="MyS3cure!Pass" \
OPENAI_API_KEY="sk-..." \
USE_AI_ANALYSIS=true \
MARVIN_AGENT_MODEL="openai:gpt-4o-mini" \
docker compose -f docker-compose.prefect.yml up --build
```

### Windows bind mount

If your data lives at `C:\Project\Data`, edit `docker-compose.prefect.yml`:

```yaml
volumes:
  - C:/Project/Data:/app/data
```

---

## Quick Start — Local (Windows, no Docker)

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

You also need the **ODBC Driver 17 for SQL Server**:
https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

### 2. Set environment variables

```powershell
$env:DB_SERVER = "localhost\SQLEXPRESS"
$env:DB_NAME   = "DataCycle"
$env:TRUSTED_CONNECTION = "true"   # Windows Auth
```

### 3. Run

```bash
python main_flow.py
```

---

## Chain of Custody (Hashing)

Every time the **Overwatcher** runs, it:

1. **Computes SHA-256** of the stage output file (`sap_extract.csv` or `cleaned_data.parquet`).
2. **Compares** the hash against an expected value (optional).
3. **Records** the result in `data/chain_of_custody.json`:

```json
{
  "chain_of_custody": [
    {
      "stage": "bronze",
      "file": "data/bronze/sap_extract.csv",
      "sha256": "a1b2c3d4...",
      "timestamp": "2026-03-18T12:00:00+00:00",
      "log_analysis": "OK",
      "issues": [],
      "verdict": "PASSED"
    }
  ]
}
```

If the hash doesn't match or the log analysis finds critical errors, the
Overwatcher **raises an exception** and the Prefect flow stops — the
Gold stage never executes and no corrupt data reaches SQL Server.

---

## Overwatcher — AI Log Analysis (Marvin)

The Overwatcher uses [Marvin](https://askmarvin.ai/) (by Prefect) for
AI-powered log analysis.  Marvin turns a Python function + docstring into
an LLM call automatically — no manual prompt engineering or response
parsing needed.

### How it works

```python
@marvin.fn
def analyse_pipeline_log(log_text: str) -> LogVerdict:
    """You are a data-pipeline integrity monitor.
    Analyse the pipeline log and look for errors, data-quality issues,
    unexpected row counts, permission errors, timeouts…
    Return: status ("OK" / "CRITICAL") and a list of issues."""
```

That's it.  Marvin handles the LLM call, parses the response into the
typed `LogVerdict` dict, and returns it.

### Two modes

| Mode | When | How |
|------|------|-----|
| **AI (Marvin)** | `OPENAI_API_KEY` is set + `USE_AI_ANALYSIS=true` | Sends last 4 000 chars of the log to the configured model |
| **Rule-based** | No API key or AI disabled | Scans for keywords: `FATAL`, `CRITICAL`, `Traceback`, `Exception:`, `PermissionError`, `ConnectionRefused`, `0 rows` |

If the Marvin call fails for any reason, the Overwatcher falls back to
rule-based analysis automatically.

### Choosing a model

Set the model via the `MARVIN_AGENT_MODEL` environment variable:

```bash
# Cheap and fast (default)
MARVIN_AGENT_MODEL="openai:gpt-4o-mini"

# Even cheaper
MARVIN_AGENT_MODEL="openai:gpt-3.5-turbo"

# More capable
MARVIN_AGENT_MODEL="openai:gpt-4o"
```

This can also be passed as `ai_model` parameter to the Prefect flow, or
set in `docker-compose.prefect.yml`.

### Where does the API key go?

The `OPENAI_API_KEY` env var is read by Marvin automatically.  Set it in
one of these places (in priority order):

1. **docker-compose.prefect.yml** → `environment:` block (already wired)
2. **Shell** → `export OPENAI_API_KEY="sk-..."`
3. **`.env` file** → Docker Compose reads `.env` from the project root
4. **Prefect Secret Block** → `prefect block create secret/openai-api-key`

---

## Prefect Secret Blocks (production credentials)

For production, store credentials in Prefect Secret Blocks instead of
environment variables:

```bash
# One-time setup (Prefect CLI or UI)
prefect block register -m prefect.blocks.system

prefect block create secret/db-password
# → enter the SQL Server password when prompted

prefect block create secret/openai-api-key
# → enter the OpenAI key when prompted
```

The `main_flow.py` automatically checks for these blocks when env vars
are not set:

```python
# Resolved in this order:
# 1. Function parameter (highest priority)
# 2. Environment variable
# 3. Prefect Secret Block
```

---

## Chat Agent

A conversational AI agent that lets you interact with the pipeline using
natural language — check status, trigger runs, query data, and analyse logs.

### Web UI

After `docker compose up`, open **http://localhost:8501**.  Type questions like:
- "Did the last run succeed?"
- "How many rows are in silver?"
- "Show me the bronze data"
- "Run the pipeline now"
- "What went wrong in the last run?"

### CLI

```bash
docker compose -f docker-compose.prefect.yml exec chat python -m agent.cli
```

### Architecture

The agent uses a Marvin `Agent` with six tools:

| Tool | What it does |
|------|-------------|
| `get_pipeline_status` | Queries Prefect API for recent run states |
| `trigger_pipeline` | Creates a new flow run via Prefect API |
| `get_row_counts` | Reads bronze CSV / silver Parquet for row counts |
| `preview_data` | Shows first N rows of a stage's output |
| `get_latest_logs` | Returns contents of stage `.log` files |
| `analyse_logs` | Reads all logs + chain of custody for LLM summary |

The LLM decides which tool(s) to call based on the user's question,
executes them, and returns a natural-language answer.

---

## Project Structure

```
├── main_flow.py                 # Prefect flow orchestrator
├── tasks/
│   ├── __init__.py
│   ├── bronze_task.py           # Stage 1: SAP extraction
│   ├── silver_task.py           # Stage 2: KNIME + cleaning
│   ├── gold_task.py             # Stage 3: SQL Server load
│   └── overwatcher.py           # Integrity gatekeeper
├── agent/
│   ├── __init__.py
│   ├── tools.py                 # Tool functions (Prefect API, file reads)
│   ├── chat_agent.py            # Marvin Agent definition
│   ├── cli.py                   # Terminal chat loop
│   └── web.py                   # FastAPI web chat UI (port 8501)
├── Dockerfile.prefect           # Container image (Python + ODBC)
├── docker-compose.prefect.yml   # SQL Server + pipeline + chat agent
├── data/                        # Shared data directory (in-project, bind-mounted by Docker)
│   ├── bronze/
│   │   ├── sap_extract.csv
│   │   └── bronze.log
│   ├── silver/
│   │   ├── cleaned_data.parquet
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
| **Bind mounts** over cloud storage | Simplicity for POC; scripts read/write a shared `./data` folder |
| **Subprocess** for SAP & KNIME | Keeps existing extraction logic intact — Prefect orchestrates, not rewrites |
| **File paths as parameters** | Loose coupling; each stage only needs to know *where* to read/write |
| **Incremental INSERT** (Gold) | PK violations are silently skipped — the pipeline only adds data |
| **Prefect Secrets** for credentials | No passwords in code or compose files for production |
| **DB encryption (TLS)** | SQL Server supports TLS natively; enable `Encrypt=yes` in the connection string for prod |
| **Docker for scaling** | Start on Windows for convenience, deploy via Docker for reproducibility |
| **Marvin** for AI agent | Built by Prefect team; function + docstring = LLM call; zero boilerplate |
| **Configurable model** | Default `gpt-4o-mini` (cheap); swap via `MARVIN_AGENT_MODEL` env var |
