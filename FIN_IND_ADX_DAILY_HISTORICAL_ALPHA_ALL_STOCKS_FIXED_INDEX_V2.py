import os
import re
import time
from pathlib import Path
from time import perf_counter
from datetime import datetime

import requests
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

print("Historical DAILY ADX from Alpha Vantage started...", flush=True)

load_dotenv(Path(__file__).with_name(".env"))


def need(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing env var: {name}")
    return value


DB_HOST = need("DB_HOST_MAIN")
DB_PORT = os.getenv("DB_PORT_MAIN", "5432")
DB_NAME = need("DB_NAME_MAIN")
DB_USER = need("DB_USER_MAIN")
DB_PASS = need("DB_PASS_MAIN")
DB_SSLMODE = os.getenv("DB_SSLMODE_MAIN", "require").strip()
API_KEY = need("API_KEY_MAIN")

STOCK_MASTER_SCHEMA = "FIN_IND"
STOCK_MASTER_TABLE = "us_stock_master"

TARGET_SCHEMA = "FIN_IND"
TARGET_TABLE = "adx_data"

INTERVAL = "daily"
TIME_PERIOD = 14

START_AFTER_TICKER = ""
MAX_STOCK_SYMBOLS = 0

# Safe for current 75/min Alpha Vantage plan.
BATCH_SIZE = 1000
INTERVAL_PER_API = 0.06
BATCH_INTERVAL = 0

DB_CONNECT_TIMEOUT_SECONDS = 15
API_TIMEOUT_SECONDS = 60


def safe_identifier(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid SQL identifier: {name}")
    return name


def get_connection():
    args = {
        "dbname": DB_NAME,
        "user": DB_USER,
        "password": DB_PASS,
        "host": DB_HOST,
        "port": int(DB_PORT),
        "connect_timeout": DB_CONNECT_TIMEOUT_SECONDS,
    }
    if DB_SSLMODE:
        args["sslmode"] = DB_SSLMODE
    return psycopg2.connect(**args)


def parse_date(value):
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return None


def parse_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def create_target_table_and_indexes(cursor):
    schema = safe_identifier(TARGET_SCHEMA)
    table = safe_identifier(TARGET_TABLE)

    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            id BIGSERIAL PRIMARY KEY,
            symbol VARCHAR(30) NOT NULL,
            interval VARCHAR(20) NOT NULL,
            time_period INT NOT NULL,
            date DATE NOT NULL,
            adx NUMERIC NOT NULL,
            inserted_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """)

    ensure_unique_index_for_upsert(cursor)

    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_symbol ON {schema}.{table} (symbol);")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_date ON {schema}.{table} (date);")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_interval_period_date ON {schema}.{table} (interval, time_period, date);")


def ensure_unique_index_for_upsert(cursor):
    """
    ON CONFLICT (symbol, interval, time_period, date) requires a unique index
    or unique constraint on exactly those columns.

    If duplicate rows already exist, the unique index creation will fail.
    This function first removes duplicate rows, keeping the lowest id row.
    """
    schema = safe_identifier(TARGET_SCHEMA)
    table = safe_identifier(TARGET_TABLE)

    print("Checking duplicate ADX rows before creating unique index...", flush=True)

    cursor.execute(f"""
        SELECT COUNT(*)
        FROM (
            SELECT symbol, interval, time_period, date
            FROM {schema}.{table}
            GROUP BY symbol, interval, time_period, date
            HAVING COUNT(*) > 1
        ) duplicates;
    """)

    duplicate_groups = cursor.fetchone()[0]

    if duplicate_groups and duplicate_groups > 0:
        print(f"Found {duplicate_groups:,} duplicate ADX key groups. Removing duplicates and keeping lowest id...", flush=True)

        cursor.execute(f"""
            DELETE FROM {schema}.{table} a
            USING {schema}.{table} b
            WHERE a.id > b.id
              AND a.symbol = b.symbol
              AND a.interval = b.interval
              AND a.time_period = b.time_period
              AND a.date = b.date;
        """)

        print(f"Duplicate rows removed: {cursor.rowcount:,}", flush=True)
    else:
        print("No duplicate ADX key groups found.", flush=True)

    print("Creating/checking required unique index for ON CONFLICT on the CURRENT table...", flush=True)

    # Important:
    # PostgreSQL index names are schema-level objects. If you renamed the old table,
    # the old index name may still exist on the renamed table. In that case,
    # CREATE INDEX IF NOT EXISTS ux_adx_data_symbol_interval_period_date would skip
    # creation even though the NEW fin_ind.adx_data table has no unique index.
    #
    # Therefore this script uses a new index name for the current table.
    unique_index_name = f"ux_{table}_symbol_interval_period_date_live"

    cursor.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {unique_index_name}
        ON {schema}.{table} (symbol, interval, time_period, date);
    """)

    print(f"Required unique index is ready on {schema}.{table}: {unique_index_name}", flush=True)


def fetch_all_stock_symbols_from_master(cursor):
    schema = safe_identifier(STOCK_MASTER_SCHEMA)
    table = safe_identifier(STOCK_MASTER_TABLE)

    where_conditions = ["ticker IS NOT NULL", "TRIM(ticker) <> ''"]
    params = []

    if START_AFTER_TICKER:
        where_conditions.append("UPPER(TRIM(ticker)) > %s")
        params.append(START_AFTER_TICKER)

    where_sql = " AND ".join(where_conditions)

    limit_sql = ""
    if MAX_STOCK_SYMBOLS and MAX_STOCK_SYMBOLS > 0:
        limit_sql = f"LIMIT {int(MAX_STOCK_SYMBOLS)}"

    query = f"""
        SELECT DISTINCT UPPER(TRIM(ticker)) AS ticker
        FROM {schema}.{table}
        
        ORDER BY ticker
        ;
    """

    cursor.execute(query, params)
    symbols = [row[0] for row in cursor.fetchall() if row[0]]
    print(f"Fetched {len(symbols):,} symbols from {schema}.{table}.", flush=True)
    return symbols


def fetch_alpha_vantage_adx(session, symbol):
    url = (
        "https://www.alphavantage.co/query"
        f"?function=ADX"
        f"&symbol={symbol}"
        f"&interval={INTERVAL}"
        f"&time_period={TIME_PERIOD}"
        f"&apikey={API_KEY}"
    )

    try:
        response = session.get(url, timeout=API_TIMEOUT_SECONDS)
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        print(f"{symbol}: API request failed: {exc}", flush=True)
        return []

    key = "Technical Analysis: ADX"
    if key not in data:
        message = data.get("Note") or data.get("Information") or data.get("Error Message") or str(data)[:400]
        print(f"{symbol}: no ADX data. Message: {message}", flush=True)
        return []

    rows = []
    for date_str, item in data.get(key, {}).items():
        d = parse_date(date_str)
        adx = parse_float(item.get("ADX"))
        if d is None or adx is None:
            continue

        rows.append((symbol, INTERVAL, TIME_PERIOD, d, round(adx, 4)))

    return rows


def upsert_adx_rows(conn, cursor, rows):
    if not rows:
        return 0

    schema = safe_identifier(TARGET_SCHEMA)
    table = safe_identifier(TARGET_TABLE)

    sql = f"""
        INSERT INTO {schema}.{table} (
            symbol,
            interval,
            time_period,
            date,
            adx
        )
        VALUES %s
        ON CONFLICT (
            symbol,
            interval,
            time_period,
            date
        )
        DO UPDATE SET
            adx = EXCLUDED.adx,
            updated_at_utc = NOW();
    """

    try:
        execute_values(cursor, sql, rows, page_size=1000)
        return len(rows)
    except Exception as exc:
        conn.rollback()
        print(f"Database upsert failed: {exc}", flush=True)
        return 0


def main():
    started = perf_counter()

    conn = get_connection()
    cursor = conn.cursor()

    try:
        create_target_table_and_indexes(cursor)
        conn.commit()

        symbols = fetch_all_stock_symbols_from_master(cursor)

        print("=" * 70, flush=True)
        print("Historical DAILY ADX from Alpha Vantage", flush=True)
        print(f"Target table: {TARGET_SCHEMA}.{TARGET_TABLE}", flush=True)
        print(f"Interval: {INTERVAL}", flush=True)
        print(f"Time period: {TIME_PERIOD}", flush=True)
        print(f"Total symbols: {len(symbols):,}", flush=True)
        print("All non-empty tickers are included.", flush=True)
        print("No TRUNCATE. No DELETE. Historical upsert only.", flush=True)
        print("=" * 70, flush=True)

        session = requests.Session()
        total_upserted = 0

        for i in range(0, len(symbols), BATCH_SIZE):
            batch_start = perf_counter()
            batch = symbols[i:i + BATCH_SIZE]
            batch_number = (i // BATCH_SIZE) + 1

            print(f"\nStarting batch {batch_number} with {len(batch)} symbols", flush=True)

            for j, symbol in enumerate(batch, start=1):
                rows = fetch_alpha_vantage_adx(session, symbol)
                n = upsert_adx_rows(conn, cursor, rows)
                total_upserted += n
                print(f"{symbol}: batch {batch_number}.{j} adx_rows_upserted={n}", flush=True)

                if INTERVAL_PER_API > 0:
                    time.sleep(INTERVAL_PER_API)

            conn.commit()
            print(f"Batch {batch_number} committed. Elapsed: {perf_counter() - batch_start:.2f}s", flush=True)

            if BATCH_INTERVAL > 0:
                time.sleep(BATCH_INTERVAL)

        print("\n" + "=" * 70, flush=True)
        print("Finished historical DAILY ADX load.", flush=True)
        print(f"Total symbols processed: {len(symbols):,}", flush=True)
        print(f"Total ADX rows upserted: {total_upserted:,}", flush=True)
        print(f"Total elapsed time: {perf_counter() - started:.2f}s", flush=True)
        print("=" * 70, flush=True)

    finally:
        cursor.close()
        conn.close()
        print("Database connection closed.", flush=True)


if __name__ == "__main__":
    main()
