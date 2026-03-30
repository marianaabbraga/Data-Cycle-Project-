"""
export_to_csv.py
────────────────
Exporte la gold layer du datawarehouse SQL Server vers deux fichiers CSV
destinés à l'import dans SAP Analytics Cloud (SAC).

Fichiers générés :
  - stock_prices_daily.csv
  - technical_indicators_daily.csv

Dépendances :
  pip install pyodbc pandas python-dotenv
"""

import os
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyodbc
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# Configuration du logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Chargement des variables d'environnement
# Créer un fichier .env à la racine du projet :
#bc2Dm3hjSbaLqe~O)sH
#   DB_SERVER=mon_serveur\\SQLEXPRESS   (ou IP)
#   DB_NAME=StockWarehouse
#   DB_USER=mon_user                    (laisser vide pour Windows Auth)
#   DB_PASSWORD=mon_mdp                 (laisser vide pour Windows Auth)
#   OUTPUT_DIR=./output
# ─────────────────────────────────────────────
load_dotenv()

DB_SERVER   = os.getenv("DB_SERVER", ".\STOCKSQLSERVER")
DB_NAME     = os.getenv("DB_NAME",   "stockmarketdb2")
DB_USER     = os.getenv("DB_USER",   "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
OUTPUT_DIR  = Path(os.getenv("OUTPUT_DIR", "../DataLake/SAP"))


# ─────────────────────────────────────────────
# Requêtes SQL dénormalisées
# ─────────────────────────────────────────────
QUERY_PRICES = """
SELECT
    -- Dimensions temporelles
    d.Year,
    d.Quarter,
    d.Month,
    d.Week,
    d.Day,

    -- Dimensions ticker / secteur
    t.Symbol,
    t.Name,
    s.Sector,
    i.Industry,

    -- Métriques de prix
    f.OpenPrice,
    f.HighPrice,
    f.LowPrice,
    f.ClosePrice,
    f.AdjustedClosePrice,
    f.Volume

FROM FactStockPrices f
JOIN DimDate     d ON f.DateId     = d.DateId
JOIN DimTicker   t ON f.TickerId   = t.TickerId
JOIN DimSector   s ON f.SectorId   = s.SectorId
JOIN DimIndustry i ON f.IndustryId = i.IndustryId

ORDER BY d.Year, d.Month, d.Day, t.Symbol;
"""

QUERY_INDICATORS = """
SELECT
    -- Dimensions temporelles
    d.Year,
    d.Quarter,
    d.Month,
    d.Week,
    d.Day,

    -- Dimensions ticker / secteur
    t.Symbol,
    t.Name,
    s.Sector,
    i.Industry,

    -- Indicateurs techniques
    f.sma20,
    f.sma50,
    f.atr,
    f.rsi,
    f.macd,
    f.bollinger_bands_down,
    f.bollinger_bands_up

FROM FactTechnicalIndicators f
JOIN DimDate     d ON f.DateId     = d.DateId
JOIN DimTicker   t ON f.TickerId   = t.TickerId
JOIN DimSector   s ON f.SectorId   = s.SectorId
JOIN DimIndustry i ON f.IndustryId = i.IndustryId

ORDER BY d.Year, d.Month, d.Day, t.Symbol;
"""


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def build_connection_string() -> str:
    """Construit la chaîne de connexion ODBC selon l'auth disponible."""
    driver = "ODBC Driver 17 for SQL Server"

    if DB_USER and DB_PASSWORD:
        # Authentification SQL Server
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={DB_SERVER};"
            f"DATABASE={DB_NAME};"
            f"UID={DB_USER};"
            f"PWD={DB_PASSWORD};"
        )
    else:
        # Authentification Windows (Trusted Connection)
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={DB_SERVER};"
            f"DATABASE={DB_NAME};"
            f"Trusted_Connection=yes;"
        )


def get_connection() -> pyodbc.Connection:
    """Ouvre et retourne une connexion SQL Server."""
    conn_str = build_connection_string()
    log.info("Connexion à %s / %s ...", DB_SERVER, DB_NAME)
    conn = pyodbc.connect(conn_str, timeout=30)
    log.info("Connexion établie.")
    return conn


def export_query_to_csv(
    conn: pyodbc.Connection,
    query: str,
    filename: str,
    output_dir: Path,
    chunk_size: int = 50_000,
) -> Path:
    """
    Exécute `query`, lit les résultats par chunks et écrit le CSV.
    Retourne le chemin du fichier créé.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / filename

    log.info("Export → %s", filepath)
    first_chunk = True
    total_rows  = 0

    for chunk in pd.read_sql(query, conn, chunksize=chunk_size):
        chunk.to_csv(
            filepath,
            mode="w" if first_chunk else "a",
            header=first_chunk,
            index=False,
            encoding="utf-8-sig",   # BOM UTF-8 — compatibilité Excel / SAC
            sep=",",
            float_format="%.4f",    # 4 décimales pour les indicateurs financiers
        )
        total_rows  += len(chunk)
        first_chunk  = False
        log.info("  … %d lignes écrites", total_rows)

    log.info("✓ %s — %d lignes au total", filename, total_rows)
    return filepath


def add_export_metadata(output_dir: Path, files: list[dict]) -> None:
    """Génère un petit fichier de log d'export pour traçabilité."""
    meta_path = output_dir / "export_metadata.txt"
    with open(meta_path, "w", encoding="utf-8") as f:
        f.write(f"Export SAP SAC — {datetime.now().isoformat()}\n")
        f.write(f"Source : {DB_SERVER} / {DB_NAME}\n\n")
        for item in files:
            f.write(f"{item['file']}  —  {item['rows']} lignes\n")
    log.info("Métadonnées écrites → %s", meta_path)


# ─────────────────────────────────────────────
# Point d'entrée
# ─────────────────────────────────────────────
def main() -> None:
    start = datetime.now()
    log.info("=== Début de l'export ===")

    conn = get_connection()
    exported = []

    try:
        # 1. Prix boursiers
        path_prices = export_query_to_csv(
            conn,
            query=QUERY_PRICES,
            filename="stock_prices_daily.csv",
            output_dir=OUTPUT_DIR,
        )
        exported.append({"file": path_prices.name, "rows": sum(1 for _ in open(path_prices)) - 1})

        # 2. Indicateurs techniques
        path_indicators = export_query_to_csv(
            conn,
            query=QUERY_INDICATORS,
            filename="technical_indicators_daily.csv",
            output_dir=OUTPUT_DIR,
        )
        exported.append({"file": path_indicators.name, "rows": sum(1 for _ in open(path_indicators)) - 1})

    finally:
        conn.close()
        log.info("Connexion fermée.")

    add_export_metadata(OUTPUT_DIR, exported)

    elapsed = (datetime.now() - start).total_seconds()
    log.info("=== Export terminé en %.1f secondes ===", elapsed)
    log.info("Fichiers disponibles dans : %s", OUTPUT_DIR.resolve())


if __name__ == "__main__":
    main()