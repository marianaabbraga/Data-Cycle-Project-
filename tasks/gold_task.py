"""
Gold Task — Incremental SQL Server Load
========================================

Reads the cleaned Parquet from Silver, ensures the target table exists,
and performs an incremental INSERT into Microsoft SQL Server.

Rows that violate a PK/unique constraint are skipped — the philosophy is
that the pipeline only *adds* data; constraint violations are a DB-level
concern, not a pipeline error.
"""

import os
from datetime import datetime

import pandas as pd
import pyodbc
from prefect import task
from prefect.logging import get_run_logger


# Schema for the mock SAP material-movements table.
# Extend or replace this when real SAP tables are defined.
MATERIAL_MOVEMENTS = {
    "columns": {
        "material_id":       "NVARCHAR(20)",
        "plant":             "NVARCHAR(10)",
        "quantity":          "FLOAT",
        "unit":              "NVARCHAR(10)",
        "posting_date":      "DATE",
        "document_number":   "NVARCHAR(20)",
        "movement_type":     "NVARCHAR(10)",
        "_silver_timestamp": "NVARCHAR(50)",
        "_source_file":      "NVARCHAR(255)",
    },
    "primary_key": ["document_number", "material_id"],
}


# ========================================================================== #
#  Prefect task                                                              #
# ========================================================================== #

@task(name="gold-sql-load", retries=1, retry_delay_seconds=30)
def run_gold(
    data_dir: str = "./data",
    silver_file: str | None = None,
    db_server: str = "localhost",
    db_name: str = "DataCycle",
    db_user: str = "sa",
    db_password: str | None = None,
    trusted: bool = False,
) -> dict:
    """
    Execute the Gold stage.

    Returns
    -------
    dict with ``log`` (log path) and ``rows_inserted`` count
    """
    logger = get_run_logger()

    if silver_file is None:
        silver_file = os.path.join(data_dir, "silver", "cleaned_data.parquet")

    gold_dir = os.path.join(data_dir, "gold")
    os.makedirs(gold_dir, exist_ok=True)
    log_file = os.path.join(gold_dir, "gold.log")
    log: list[str] = []

    log.append(f"[{datetime.now().isoformat()}] Gold stage started")
    log.append(f"Input: {silver_file}")
    log.append(f"Target: {db_server}/{db_name}")

    df = pd.read_parquet(silver_file)
    log.append(f"Loaded {len(df)} rows from silver")
    logger.info(f"Gold: loading {len(df)} rows into SQL Server")

    conn = _connect(db_server, db_name, db_user, db_password, trusted)
    log.append("Connected to SQL Server")

    try:
        table_name = "dbo.material_movements"

        _create_table(conn, table_name,
                       MATERIAL_MOVEMENTS["columns"],
                       MATERIAL_MOVEMENTS["primary_key"])
        log.append(f"Table [{table_name}] ready")

        inserted = _insert_rows(conn, table_name, df)
        log.append(f"Inserted {inserted}/{len(df)} rows")
        logger.info(f"Gold: {inserted}/{len(df)} rows inserted")
    finally:
        conn.close()
        log.append("Connection closed")

    log.append(f"[{datetime.now().isoformat()}] Gold stage completed")

    with open(log_file, "w") as fh:
        fh.write("\n".join(log))

    return {"log": log_file, "rows_inserted": inserted}


# ========================================================================== #
#  DB helpers (mirrors Dataflows/ToGold/factTables.py)                       #
# ========================================================================== #

def _connect(server, database, user, password, trusted):
    driver = "{ODBC Driver 18 for SQL Server}"
    if trusted:
        cs = (f"DRIVER={driver};SERVER={server};"
              f"DATABASE={database};Trusted_Connection=yes;"
              f"TrustServerCertificate=yes;")
    else:
        cs = (f"DRIVER={driver};SERVER={server};"
              f"DATABASE={database};UID={user};PWD={password};"
              f"TrustServerCertificate=yes;")
    return pyodbc.connect(cs)


def _create_table(conn, table_name, columns, primary_key=None):
    col_defs = ", ".join(f"[{c}] {t}" for c, t in columns.items())
    pk = ""
    if primary_key:
        pk_cols = ", ".join(f"[{c}]" for c in primary_key)
        pk = f", CONSTRAINT PK_{table_name.replace('.', '_')} PRIMARY KEY ({pk_cols})"

    sql = f"""
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = '{table_name.split('.')[-1]}'
    )
    BEGIN
        CREATE TABLE {table_name} ({col_defs}{pk})
    END"""
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def _insert_rows(conn, table_name, df, batch_size=1000):
    if df.empty:
        return 0

    cols = df.columns.tolist()
    col_list = ", ".join(f"[{c}]" for c in cols)
    placeholders = ", ".join("?" for _ in cols)
    sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"

    cur = conn.cursor()
    cur.fast_executemany = True
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
    inserted = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        try:
            cur.executemany(sql, batch)
            conn.commit()
            inserted += len(batch)
        except pyodbc.IntegrityError:
            conn.rollback()
            for row in batch:
                try:
                    cur.execute(sql, row)
                    conn.commit()
                    inserted += 1
                except pyodbc.IntegrityError:
                    conn.rollback()

    return inserted
