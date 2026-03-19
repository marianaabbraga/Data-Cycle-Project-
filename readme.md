# Data Cycle Project

A data pipeline that follows the **Medallion Architecture** (Bronze -> Silver -> Gold) to ingest, clean, and analyze stock market data from Yahoo Finance.

## Architecture Overview

```
┌─────────────────────┐
│   Yahoo Finance API  │
│  (yfinance library)  │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐     ┌─────────────────────────────────────┐
│   BRONZE LAYER      │     │  Raw data ingestion                 │
│   sourceToBronze    │────▶│  output_raw/                        │
│   DataFlow.py       │     │  ├── ticker/     (prices, calendar) │
│                     │     │  ├── market/     (status, summary)  │
│                     │     │  ├── sector_industry/               │
│                     │     │  ├── equity/     (financials)       │
│                     │     │  └── funds/      (ETF data)         │
└─────────────────────┘     └──────────────┬──────────────────────┘
                                           │
                                           ▼
┌─────────────────────┐     ┌─────────────────────────────────────┐
│   SILVER LAYER      │     │  Cleaned & enriched data            │
│   bronzeToSilver    │────▶│  output_silver/                     │
│   DataFlow.py       │     │  ├── stocks_master/                 │
│                     │     │  ├── price_history/                 │
│                     │     │  │   └── ticker=X/year=Y/data.pqt   │
│                     │     │  └── technical_indicators/           │
│                     │     │      └── ticker=X/year=Y/data.pqt   │
└─────────────────────┘     └──────────────┬──────────────────────┘
                                           │
                                           ▼
┌─────────────────────┐
│   GOLD LAYER        │
│   silverToGold      │     (not implemented yet)
│   DataFlow.py       │
└─────────────────────┘
```

---

## Bronze Layer — Raw Data Ingestion

**Script:** `Dataflows/ToBronze/sourceToBronzeDataFlow.py`

Pulls raw data from Yahoo Finance and saves it as Parquet files in `output_raw/`. No transformations — just a faithful copy of the source data.

| Step | What it does | Tickers | Output folder |
|------|-------------|---------|---------------|
| **US-4** | Price history, calendar, company info | AAPL, MSFT, NVDA | `ticker/` |
| **US-5** | Market status and summary | us_market, eu_market | `market/` |
| **US-6** | Sector and industry overviews | Derived from portfolio | `sector_industry/` |
| **US-7** | Income statement, balance sheet, cashflow, ETF data | AAPL, MSFT, NVDA + SPY, QQQ | `equity/`, `funds/` |

Each file is timestamped (e.g. `AAPL_prices_2026-03-18_14-30-00.parquet`) so multiple runs don't overwrite each other.

---

## Silver Layer — Cleaning & Enrichment

**Script:** `Dataflows/ToSilver/bronzeToSilverDataFlow.py`

Reads the raw Bronze data, cleans it, computes technical indicators, and writes structured tables using Hive-style partitioning (`ticker=X/year=Y/`).

### Cleaning steps

1. **Type fixing** — prices and volumes cast to numeric, dates normalized to UTC
2. **Null removal** — rows missing `Close` or `Date` are dropped
3. **Deduplication** — duplicate `(Date, ticker)` pairs removed
4. **OHLC validation** — ensures `High >= Low`, `High >= Open/Close`, `Low <= Open/Close`
5. **IQR outlier removal** — prices outside `Q1 - 3*IQR` to `Q3 + 3*IQR` are filtered out

### Technical indicators computed

| Indicator | Description |
|-----------|-------------|
| **SMA 20/50** | Simple Moving Average (20-day short-term, 50-day medium-term) |
| **RSI** | Relative Strength Index (14-day). >70 = overbought, <30 = oversold |
| **MACD** | Moving Average Convergence Divergence (EMA12 - EMA26) |
| **Bollinger Bands** | SMA20 +/- 2 standard deviations — price fluctuation range |
| **ATR** | Average True Range (14-day) — measures volatility |

### Output tables

| Table | Description |
|-------|-------------|
| `stocks_master` | Ticker, company name, sector, industry |
| `price_history` | Daily OHLCV data, Hive-partitioned by ticker and year |
| `technical_indicators` | All computed indicators, Hive-partitioned by ticker and year |

### Analytics (printed to console)

- Descriptive statistics per ticker
- Volatility (standard deviation of daily returns)
- K-Means clustering (groups stocks into 3 categories by average close price and volume)

---

## Gold Layer

**Script:** `Dataflows/ToGold/silverToGoldDataFlow.py`

Not implemented yet. This layer will contain aggregated, business-ready datasets.

---

## Utilities

| File | Purpose |
|------|---------|
| `Converters/fileConverter.py` | Converts between CSV and Parquet formats |
| `Converters/TestFileConverter.py` | Test script for the converter |
| `Dataflows/ToBronze/checkFileContent.py` | Inspects a Parquet file (head, columns, nulls, date range) |

---

## Quick Start — GUI (recommended)

**Prerequisites:** [Docker](https://docs.docker.com/get-docker/) installed and running, Python 3 (tkinter comes built-in).

```bash
python pipeline_gui.py
```

Everything runs inside Docker — no need to install pandas, yfinance, or any other package. The GUI:

- **Checks Docker** on startup and shows status in the header
- **Status cards** for Bronze / Silver / Gold showing file counts and state
- **Run Bronze / Run Silver / Run Full Pipeline** buttons
- **Incremental Silver** — only re-processes tickers that have new bronze data since the last silver run (tracked via a manifest). Use "Force Full Refresh" to reprocess everything.
- **Live console** showing real-time Docker output
- **First-run detection** — if no data exists, guides you to start; first run builds the Docker image automatically

### Output

Data is written to folders on your host machine (Docker volumes):

- `./output_raw/` — Bronze layer Parquet files
- `./output_silver/` — Silver layer Parquet files (Hive-partitioned)

---

## Alternative: Docker CLI (no GUI)

```bash
# Full pipeline (Bronze + Silver)
docker compose --profile full up --build pipeline

# Individual layers
docker compose up --build bronze
docker compose up --build silver

# Interactive dev shell
docker compose --profile dev run --rm dev
```

---

## Alternative: Local Python (no Docker)

```bash
git clone <repo-url>
cd Data-Cycle-Project-

python -m venv .venv
source .venv/bin/activate        # macOS/Linux
# .venv\Scripts\activate         # Windows

pip install -r requirements.txt
```

```bash
# CLI orchestrator
python run_pipeline.py              # full pipeline
python run_pipeline.py bronze       # bronze only
python run_pipeline.py silver       # silver only
python run_pipeline.py --check      # check dependencies

# Or run scripts directly
python Dataflows/ToBronze/sourceToBronzeDataFlow.py
python Dataflows/ToSilver/bronzeToSilverDataFlow.py
python Dataflows/ToSilver/bronzeToSilverDataFlow.py --tickers AAPL NVDA  # incremental
```

---

## Dependencies

| Package | Purpose |
|---------|---------|
| `pandas` | DataFrames and Parquet I/O |
| `numpy` | Numerical operations |
| `yfinance` | Yahoo Finance API wrapper |
| `scikit-learn` | K-Means clustering in analytics |
| `pyarrow` | Parquet file engine |
