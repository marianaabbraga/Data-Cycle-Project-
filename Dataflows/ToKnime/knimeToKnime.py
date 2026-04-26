import os
import json
import math
import base64
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

KNIME_API_ID       = os.environ.get("KNIME_API_ID", "")
KNIME_API_PASSWORD = os.environ.get("KNIME_API_PASSWORD", "")

if not KNIME_API_ID or not KNIME_API_PASSWORD:
    raise EnvironmentError(
        "❌ KNIME_API_ID or KNIME_API_PASSWORD missing in .env file"
    )

KNIME_URL       = (
    "https://api.edu-hub.knime.com/deployments/"
    "rest:ed52ee56-066e-4627-9fb2-f0b6a6e0c1bd/"
    "execution?reset=false&timeout=-1"
)

INPUT_DIR       = Path(__file__).parent / "../../data/knime/input"
OUTPUT_DIR      = Path(__file__).parent / "../../data/knime/output"
OUTPUT_CSV      = "predictions.csv"

COLUMN_ORDER = [
    "ticker", "date", "year", "open", "high", "low",
    "close", "volume", "adj_close", "sector", "industry"
]

# ============================================================
# HELPERS
# ============================================================

_credentials = base64.b64encode(f"{KNIME_API_ID}:{KNIME_API_PASSWORD}".encode()).decode()

HEADERS = {
    "accept": "application/vnd.mason+json",
    "Content-Type": "application/json",
    "Authorization": f"Basic {_credentials}",
}


def load_parquet(path: Path) -> pd.DataFrame:
    """Load a Parquet file and returns a dataframe."""
    if not path.exists():
        raise FileNotFoundError(f"File not found : {path}")
    df = pd.read_parquet(path)
    print(f"  📂 {path.name} — {len(df)} rows")
    return df


def df_to_table_data(df: pd.DataFrame) -> list:
    """
    Convert a DataFrame to a list of lists (KNIME table-data format).
    NaN/NaT values are replaced by None (-> null in JSON).
    Dates are formatted as ISO 8601 strings.
    """
    rows = []
    for _, row in df.iterrows():
        record = []
        for col in COLUMN_ORDER:
            val = row.get(col)
            # Pandas Timestamp -> string ISO
            if isinstance(val, (pd.Timestamp,)):
                val = val.strftime("%Y-%m-%dT%H:%M") if not pd.isnull(val) else None
            # float NaN -> None
            elif isinstance(val, float) and math.isnan(val):
                val = None
            # numpy int -> python int
            elif hasattr(val, "item"):
                val = val.item()
            record.append(val)
        rows.append(record)
    return rows


def call_knime(table_data: list, ticker: str) -> list | None:
    """
    Send ticker data to the KNIME workflow via REST.
    Returns (spec, data) on success, or None on error.
    """
    payload = {"table-input": {"table-data": table_data}}

    try:
        response = requests.post(KNIME_URL, headers=HEADERS, json=payload, timeout=120)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"  HTTP error for {ticker} : {e} — {response.text[:300]}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"  Network error for {ticker} : {e}")
        return None

    try:
        result = response.json()
        output = result["outputValues"]["table-output"]
        spec   = [list(col.keys())[0] for col in output["table-spec"]]
        data   = output["table-data"]
        return spec, data
    except (KeyError, json.JSONDecodeError) as e:
        print(f"  Unexpected answer from {ticker} : {e}")
        print(f"     Raw answer : {response.text[:500]}")
        return None


def parse_output(spec: list, data: list) -> pd.DataFrame:
    """Convert KNIME spec + data into a pandas DataFrame."""
    df = pd.DataFrame(data, columns=spec)
    # Convert date columns if present
    for col in df.columns:
        if "date" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


# ============================================================
# MAIN
# ============================================================

def main():
    # 1. List all Parquet files in the input directory
    input_dir = Path(INPUT_DIR)
    parquet_files = sorted(input_dir.glob("*.parquet")) 

    if not parquet_files:
        raise FileNotFoundError(f"No .parquet files found in: {input_dir}")

    print(f"  {len(parquet_files)} file(s) found in {input_dir}\n")

    # 2. Prepare the output directory
    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 3. Process files one by one
    all_results = []
    errors = []

    for parquet_file in parquet_files:
        # Extract ticker from filename (e.g. AAPL.parquet -> AAPL)
        ticker = parquet_file.parent.name.replace("ticker=", "") or parquet_file.stem
        print(f"-> {ticker} ...", end=" ", flush=True)

        df_input = load_parquet(parquet_file)

        # Check that all required columns are present
        missing = [c for c in COLUMN_ORDER if c not in df_input.columns]
        if missing:
            print(f"  Missing columns, file skipped: {missing}")
            errors.append(ticker)
            continue

        table_data = df_to_table_data(df_input)
        result = call_knime(table_data, ticker)

        if result is None:
            errors.append(ticker)
            continue

        spec, data = result
        df_out = parse_output(spec, data)
        all_results.append(df_out)
        print(f"OK — {len(df_out)} rows received")

    # 4. Write final CSV (concatenation of all results)
    if all_results:
        df_final = pd.concat(all_results, ignore_index=True)
        out_path = out_dir / OUTPUT_CSV
        df_final.to_csv(out_path, index=False, date_format="%Y-%m-%d")
        print(f"\nCSV written : {out_path}  ({len(df_final)} rows total)")
    else:
        print("\nNo results to write.")

    if errors:
        print(f"\nFailed tickers ({len(errors)}) : {', '.join(errors)}")

    print("\nPipeline complete.")


if __name__ == "__main__":
    main()