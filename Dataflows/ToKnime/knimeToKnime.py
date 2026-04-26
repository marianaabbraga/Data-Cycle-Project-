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
# CONFIG — à personnaliser
# ============================================================

KNIME_API_ID       = os.environ.get("KNIME_API_ID", "")
KNIME_API_PASSWORD = os.environ.get("KNIME_API_PASSWORD", "")

if not KNIME_API_ID or not KNIME_API_PASSWORD:
    raise EnvironmentError(
        "❌ KNIME_API_ID ou KNIME_API_PASSWORD manquant dans le fichier .env"
    )

KNIME_URL       = (
    "https://api.edu-hub.knime.com/deployments/"
    "rest:ed52ee56-066e-4627-9fb2-f0b6a6e0c1bd/"
    "execution?reset=false&timeout=-1"
)

INPUT_DIR       = Path(__file__).parent / "../../data/knime/input"   # <- dossier contenant les parquets
OUTPUT_DIR      = Path(__file__).parent / "../../data/knime/output"
OUTPUT_CSV      = "predictions.csv"                                   # <- sortie en CSV

COLUMN_ORDER = [
    "ticker", "date", "year", "open", "high", "low",
    "close", "volume", "adj_close", "sector", "industry"
]

# ============================================================
# HELPERS
# ============================================================

# Encodage Basic Auth (ID:Password en base64)
_credentials = base64.b64encode(f"{KNIME_API_ID}:{KNIME_API_PASSWORD}".encode()).decode()

HEADERS = {
    "accept": "application/vnd.mason+json",
    "Content-Type": "application/json",
    "Authorization": f"Basic {_credentials}",
}


def load_parquet(path: Path) -> pd.DataFrame:
    """Charge un fichier Parquet et retourne un DataFrame."""
    if not path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {path}")
    df = pd.read_parquet(path)
    print(f"  📂 {path.name} — {len(df)} lignes")
    return df


def df_to_table_data(df: pd.DataFrame) -> list:
    """
    Convertit un DataFrame en liste de listes (format table-data KNIME).
    Les valeurs NaN/NaT sont remplacées par None (-> null en JSON).
    Les dates sont formatées en ISO 8601.
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
    Envoie les données d'un ticker au workflow KNIME.
    Retourne la liste de listes de l'output, ou None en cas d'erreur.
    """
    payload = {"table-input": {"table-data": table_data}}

    try:
        response = requests.post(KNIME_URL, headers=HEADERS, json=payload, timeout=120)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"  Erreur HTTP pour {ticker} : {e} — {response.text[:300]}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"  Erreur réseau pour {ticker} : {e}")
        return None

    try:
        result = response.json()
        output = result["outputValues"]["table-output"]
        spec   = [list(col.keys())[0] for col in output["table-spec"]]
        data   = output["table-data"]
        return spec, data
    except (KeyError, json.JSONDecodeError) as e:
        print(f"  Réponse inattendue pour {ticker} : {e}")
        print(f"     Réponse brute : {response.text[:500]}")
        return None


def parse_output(spec: list, data: list) -> pd.DataFrame:
    """Convertit spec + data KNIME en DataFrame pandas."""
    df = pd.DataFrame(data, columns=spec)
    # Conversion des colonnes de dates si présentes
    for col in df.columns:
        if "date" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


# ============================================================
# MAIN
# ============================================================

def main():
    # 1. Lister tous les fichiers Parquet du dossier input
    input_dir = Path(INPUT_DIR)
    parquet_files = sorted(input_dir.glob("*.parquet"))  # non-recursif, meme niveau

    if not parquet_files:
        raise FileNotFoundError(f"Aucun fichier .parquet trouve dans : {input_dir}")

    print(f"  {len(parquet_files)} fichier(s) trouve(s) dans {input_dir}\n")

    # 2. Preparer le dossier de sortie
    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 3. Boucle fichier par fichier
    all_results = []
    errors = []

    for parquet_file in parquet_files:
        # Extraire le ticker depuis le nom du dossier parent (ex: ticker=AAPL) ou le nom du fichier
        ticker = parquet_file.parent.name.replace("ticker=", "") or parquet_file.stem
        print(f"-> {ticker} ...", end=" ", flush=True)

        df_input = load_parquet(parquet_file)

        # Verification des colonnes
        missing = [c for c in COLUMN_ORDER if c not in df_input.columns]
        if missing:
            print(f"  Colonnes manquantes, fichier ignore : {missing}")
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
        print(f"OK — {len(df_out)} lignes recues")

    # 4. Ecriture du Parquet final (concatenation de tous les resultats)
    if all_results:
        df_final = pd.concat(all_results, ignore_index=True)
        out_path = out_dir / OUTPUT_CSV
        df_final.to_csv(out_path, index=False, date_format="%Y-%m-%d")
        print(f"\nCSV ecrit : {out_path}  ({len(df_final)} lignes au total)")
    else:
        print("\nAucun resultat a ecrire.")

    if errors:
        print(f"\nTickers en erreur ({len(errors)}) : {', '.join(errors)}")

    print("\nPipeline termine.")


if __name__ == "__main__":
    main()