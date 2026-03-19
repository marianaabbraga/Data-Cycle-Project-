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
        )
    else:
        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
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


def insert_from_dataframe(conn, table_name: str, df: pd.DataFrame, batch_size: int = 1000):
    """
    Insert rows from a DataFrame into an existing table.

    Uses parameterized INSERT statements in batches. Rows that violate a
    PK/unique constraint are skipped (the workflow only adds data — if
    something goes wrong, it's a database issue).

    Parameters:
        conn        : pyodbc Connection
        table_name  : target table
        df          : data to insert
        batch_size  : rows per executemany call
    """
    if df.empty:
        print(f"  [{table_name}] nothing to insert (empty DataFrame)")
        return

    cols = df.columns.tolist()
    col_list = ", ".join(f"[{c}]" for c in cols)
    placeholders = ", ".join("?" for _ in cols)

    sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"

    cursor = conn.cursor()
    cursor.fast_executemany = True

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    inserted = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        try:
            cursor.executemany(sql, batch)
            conn.commit()
            inserted += len(batch)
        except pyodbc.IntegrityError:
            # Duplicate key — fall back to row-by-row to skip only the dupes
            conn.rollback()
            for row in batch:
                try:
                    cursor.execute(sql, row)
                    conn.commit()
                    inserted += 1
                except pyodbc.IntegrityError:
                    conn.rollback()

    print(f"  [{table_name}] {inserted}/{len(rows)} rows inserted")




# We should test this a little before deploying it into production, we'll see how it works
# if we need this fucntion etc, if we need something else etc




##### Maybe it will help if we kinda model everything according to our db scheme as well
### I'll keep this in mind