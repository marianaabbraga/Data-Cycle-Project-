import numpy as np
import pyodbc
import pandas as pd


# ==============================================================================
# CONNECTION
# ==============================================================================

def connect(server: str, database: str, driver: str = "{ODBC Driver 17 for SQL Server}",
            trusted: bool = True, username: str = None, password: str = None):
    """
    Open and return a connection to a SQL Server instance.

    Parameters:
        server   : hostname or IP  (e.g. "localhost", "192.168.1.10\\SQLEXPRESS")
        database : target database name
        driver   : ODBC driver string (default works for most SQL Server installs)
        trusted  : if True, use Windows Authentication (ignores username/password)
        username : SQL Server login (only used when trusted=False)
        password : SQL Server password (only used when trusted=False)

    Returns:
        A pyodbc Connection object.
    """
    if trusted:
        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
            f"TrustServerCertificate=yes;"
        )
    else:
        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
        )

    conn = pyodbc.connect(conn_str)
    print(f"Connected to [{database}] on {server}")
    return conn


# ==============================================================================
# CREATE
# ==============================================================================

def create_table(conn, table_name: str, columns: dict, primary_key: list = None):
    """
    Create a table if it doesn't already exist.

    Parameters:
        conn        : pyodbc Connection
        table_name  : fully qualified name (e.g. "dbo.price_history")
        columns     : ordered dict of {column_name: sql_type}
                      e.g. {"ticker": "NVARCHAR(10)", "date": "DATE", "close": "FLOAT"}
        primary_key : optional list of column names for the PK constraint

    The function is idempotent — calling it twice on the same table does nothing.
    """
    col_defs = ",\n        ".join(f"[{col}] {sql_type}" for col, sql_type in columns.items())

    pk_clause = ""
    if primary_key:
        pk_cols = ", ".join(f"[{c}]" for c in primary_key)
        pk_clause = f",\n        CONSTRAINT PK_{table_name.replace('.', '_')} PRIMARY KEY ({pk_cols})"

    sql = f"""
    IF NOT EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = '{table_name.split('.')[-1]}'
    )
    BEGIN
        CREATE TABLE {table_name} (
        {col_defs}{pk_clause}
        )
    END
    """

    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    print(f"Table [{table_name}] ready")


# ==============================================================================
# INSERT (bulk, with per-row fallback)
# ==============================================================================

def insert_from_dataframe(conn, table_name: str, df: pd.DataFrame, batch_size: int = 1000):
    if df.empty:
        print(f"  [{table_name}] nothing to insert (empty DataFrame)")
        return
    
    df = df.where(pd.notnull(df), None)
    import numpy as np
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        df[col] = df[col].replace([np.inf, -np.inf], None)
    
    for col in df.columns:
        if df[col].dtype == 'int64' or df[col].dtype == 'Int64':
            df[col] = df[col].astype(object)
    
    df = df.astype(object)

    cols = df.columns.tolist()
    col_list = ", ".join(f"[{c}]" for c in cols)
    placeholders = ", ".join("?" for _ in cols)
    sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"

    cursor = conn.cursor()
    cursor.fast_executemany = True

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    inserted = 0
    skipped = 0
    first_error = None

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        try:
            cursor.executemany(sql, batch)
            conn.commit()
            inserted += len(batch)
        except pyodbc.Error as e:
            conn.rollback()
            if not first_error:
                first_error = str(e)
            for row in batch:
                try:
                    cursor.execute(sql, row)
                    conn.commit()
                    inserted += 1
                except pyodbc.Error as e2:
                    conn.rollback()
                    skipped += 1

    print(f"  [{table_name}] {inserted}/{len(rows)} rows inserted, {skipped} skipped")
    if first_error and skipped > 0:
        print(f"    First error: {first_error[:100]}...")


# ==============================================================================
# MERGE / UPSERT  (for dimension tables)
# ==============================================================================

def merge_into_table(conn, table_name: str, df: pd.DataFrame,
                     key_cols: list, update_cols: list = None):
    """
    MERGE (upsert) df into table_name using SQL Server MERGE statement.

    Parameters:
        conn        : pyodbc Connection
        table_name  : fully qualified name (e.g. "dbo.DimTicker")
        df          : DataFrame whose columns match the target table exactly
        key_cols    : columns that identify a unique row (the ON match condition)
        update_cols : columns to overwrite when a match is found.
                      Pass None to update all non-key columns automatically.

    Behaviour:
        - WHEN MATCHED     → UPDATE the update_cols
        - WHEN NOT MATCHED → INSERT the full row

    Notes:
        - Do NOT include IDENTITY columns in key_cols or update_cols.
          SQL Server rejects attempts to update identity columns.
        - keep="last" semantics: if df itself has duplicates on key_cols,
          the last row wins (consistent with upsert_partition in storage.py).
    """
    if df.empty:
        print(f"  [{table_name}] nothing to merge (empty DataFrame)")
        return

    # Normalise NaN → None so pyodbc sends NULL
    df = df.where(pd.notnull(df), None)
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        df[col] = df[col].replace([np.inf, -np.inf], None)
    df = df.astype(object)

    all_cols = df.columns.tolist()
    if update_cols is None:
        update_cols = [c for c in all_cols if c not in key_cols]

    col_list   = ", ".join(f"[{c}]" for c in all_cols)
    val_list   = ", ".join("?" for _ in all_cols)
    match_cond = " AND ".join(f"target.[{c}] = source.[{c}]" for c in key_cols)
    update_set = ", ".join(f"target.[{c}] = source.[{c}]" for c in update_cols)
    insert_src = ", ".join(f"source.[{c}]" for c in all_cols)

    # When there are no update columns (e.g. a PK-only table) skip the UPDATE
    # clause so SQL Server does not raise a syntax error.
    if update_cols:
        matched_clause = f"WHEN MATCHED THEN UPDATE SET {update_set}"
    else:
        matched_clause = ""

    sql = f"""
    MERGE {table_name} AS target
    USING (VALUES ({val_list})) AS source ({col_list})
    ON {match_cond}
    {matched_clause}
    WHEN NOT MATCHED THEN
        INSERT ({col_list}) VALUES ({insert_src});
    """

    cursor = conn.cursor()
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
    upserted = skipped = 0
    first_error = None

    for row in rows:
        try:
            cursor.execute(sql, row)
            conn.commit()
            upserted += 1
        except pyodbc.Error as e:
            conn.rollback()
            skipped += 1
            if not first_error:
                first_error = str(e)

    print(f"  [{table_name}] {upserted} upserted, {skipped} skipped")
    if first_error and skipped > 0:
        print(f"    First error: {first_error[:120]}...")


# ==============================================================================
# DELETE + INSERT  (for fact tables — faster than row-by-row MERGE)
# ==============================================================================

def delete_and_insert(conn, table_name: str, df: pd.DataFrame,
                      partition_col: str = "DateId", batch_size: int = 1000):
    """
    Incremental load strategy for fact tables:
        1. DELETE all existing rows whose partition_col value appears in df.
        2. Bulk-INSERT the new rows.

    This avoids slow row-by-row MERGE for large fact tables while still being
    safe to re-run: if a Silver partition is reprocessed (e.g. after a price
    correction), the old Gold rows for those dates are cleanly replaced.

    Parameters:
        conn           : pyodbc Connection
        table_name     : fully qualified name (e.g. "dbo.FactStockPrices")
        df             : DataFrame of new rows to load
        partition_col  : the column used to identify which date-slices to replace
                         (default "DateId", format YYYYMMDD int)
        batch_size     : rows per INSERT batch (passed to insert_from_dataframe)
    """
    if df.empty:
        print(f"  [{table_name}] nothing to load (empty DataFrame)")
        return

    date_ids = df[partition_col].unique().tolist()

    # SQL Server has a 2 100-parameter limit per statement; chunk the IN list
    # to avoid hitting it when many dates are involved.
    CHUNK = 2000
    cursor = conn.cursor()
    deleted_total = 0

    for i in range(0, len(date_ids), CHUNK):
        chunk = date_ids[i : i + CHUNK]
        placeholders = ", ".join("?" for _ in chunk)
        cursor.execute(
            f"DELETE FROM {table_name} WHERE [{partition_col}] IN ({placeholders})",
            chunk,
        )
        deleted_total += cursor.rowcount
        conn.commit()

    print(f"  [{table_name}] deleted {deleted_total} existing rows "
          f"across {len(date_ids)} date(s)")

    insert_from_dataframe(conn, table_name, df, batch_size=batch_size)