import os
import re
import time
from pathlib import Path
from time import perf_counter
from datetime import datetime, date, timedelta

import requests
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

print("Historical DAILY KAMA Alpha Vantage 2-year load started...", flush=True)

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
TARGET_TABLE = "kama_data"

INTERVAL = "daily"
TIME_PERIOD = 14
SERIES_TYPE = "close"
FAST_PERIOD = 2
SLOW_PERIOD = 30

HISTORY_YEARS = 2
HISTORY_START_DATE = date.today() - timedelta(days=365 * HISTORY_YEARS)

START_AFTER_TICKER = ""
MAX_STOCK_SYMBOLS = 0

BATCH_SIZE = 1000
INTERVAL_PER_API = 0.06
BATCH_INTERVAL = 0

DB_CONNECT_TIMEOUT_SECONDS = 15
API_TIMEOUT_SECONDS = 60
CREATE_TARGET_IF_MISSING = True


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
    conn = psycopg2.connect(**args)
    print("Database connected.", flush=True)
    return conn


def safe_rollback(conn):
    try:
        if conn is not None and conn.closed == 0:
            conn.rollback()
    except Exception:
        pass


def reconnect_db(old_cursor=None, old_conn=None):
    try:
        if old_cursor is not None:
            old_cursor.close()
    except Exception:
        pass
    try:
        if old_conn is not None and old_conn.closed == 0:
            old_conn.close()
    except Exception:
        pass
    print("Reconnecting to database...", flush=True)
    conn = get_connection()
    cursor = conn.cursor()
    print("Database reconnected.", flush=True)
    return conn, cursor


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
            fast_period INT NOT NULL,
            slow_period INT NOT NULL,
            series_type VARCHAR(30) NOT NULL,
            date DATE NOT NULL,
            kama NUMERIC NOT NULL,
            inserted_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """)
    ensure_unique_index_for_upsert(cursor)
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_symbol ON {schema}.{table} (symbol);")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_date ON {schema}.{table} (date);")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_symbol_date ON {schema}.{table} (symbol, date);")


def ensure_unique_index_for_upsert(cursor):
    schema = safe_identifier(TARGET_SCHEMA)
    table = safe_identifier(TARGET_TABLE)
    print("Checking duplicate KAMA rows before creating unique index...", flush=True)
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM (
            SELECT symbol, interval, time_period, fast_period, slow_period, series_type, date
            FROM {schema}.{table}
            GROUP BY symbol, interval, time_period, fast_period, slow_period, series_type, date
            HAVING COUNT(*) > 1
        ) duplicates;
    """)
    duplicate_groups = cursor.fetchone()[0]
    if duplicate_groups and duplicate_groups > 0:
        print(f"Found {duplicate_groups:,} duplicate KAMA key groups. Removing duplicates and keeping lowest id...", flush=True)
        cursor.execute(f"""
            DELETE FROM {schema}.{table} a
            USING {schema}.{table} b
            WHERE a.id > b.id
              AND a.symbol = b.symbol
              AND a.interval = b.interval
              AND a.time_period = b.time_period
              AND a.fast_period = b.fast_period
              AND a.slow_period = b.slow_period
              AND a.series_type = b.series_type
              AND a.date = b.date;
        """)
        print(f"Duplicate rows removed: {cursor.rowcount:,}", flush=True)
    else:
        print("No duplicate KAMA key groups found.", flush=True)
    cursor.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_{table}_symbol_interval_periods_series_date_live
        ON {schema}.{table} (symbol, interval, time_period, fast_period, slow_period, series_type, date);
    """)
    print("Required unique index is ready.", flush=True)


def fetch_all_stock_symbols_from_master(cursor):
    schema = safe_identifier(STOCK_MASTER_SCHEMA)
    table = safe_identifier(STOCK_MASTER_TABLE)
    where_conditions = ["ticker IS NOT NULL", "TRIM(ticker) <> ''"]
    params = []
    if START_AFTER_TICKER:
        where_conditions.append("UPPER(TRIM(ticker)) > %s")
        params.append(START_AFTER_TICKER.upper().strip())
    limit_sql = ""
    if MAX_STOCK_SYMBOLS and MAX_STOCK_SYMBOLS > 0:
        limit_sql = f"LIMIT {int(MAX_STOCK_SYMBOLS)}"
    query = f"""
        SELECT DISTINCT UPPER(TRIM(ticker)) AS ticker
        FROM {schema}.{table}
        WHERE {' AND '.join(where_conditions)}
        ORDER BY ticker
        {limit_sql};
    """
    cursor.execute(query, params)
    symbols = [row[0] for row in cursor.fetchall() if row[0]]
    print(f"Fetched {len(symbols):,} symbols from {schema}.{table}.", flush=True)
    return symbols


def fetch_indicator(session, symbol):
    url = (
        "https://www.alphavantage.co/query"
        f"?function=KAMA"
        f"&symbol={symbol}"
        f"&interval={INTERVAL}"
        f"&time_period={TIME_PERIOD}"
        f"&series_type={SERIES_TYPE}"
        f"&apikey={API_KEY}"
    )
    try:
        response = session.get(url, timeout=API_TIMEOUT_SECONDS)
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        print(f"{symbol}: API request failed: {exc}", flush=True)
        return []
    ta = data.get("Technical Analysis: KAMA", {})
    if not ta:
        message = data.get("Note") or data.get("Information") or data.get("Error Message") or str(data)[:300]
        print(f"{symbol}: no KAMA data. Message: {message}", flush=True)
        return []
    rows = []
    for date_str, item in ta.items():
        d = parse_date(date_str)
        if d is None or d < HISTORY_START_DATE:
            continue
        val = parse_float(item.get("KAMA"))
        if val is None:
            continue
        rows.append((symbol, INTERVAL, TIME_PERIOD, FAST_PERIOD, SLOW_PERIOD, SERIES_TYPE, d, round(val, 4)))
    return rows


def upsert(conn, cursor, rows):
    if not rows:
        return 0
    schema = safe_identifier(TARGET_SCHEMA)
    table = safe_identifier(TARGET_TABLE)
    sql = f"""
        INSERT INTO {schema}.{table} (
            symbol, interval, time_period, fast_period, slow_period, series_type, date, kama
        )
        VALUES %s
        ON CONFLICT (symbol, interval, time_period, fast_period, slow_period, series_type, date)
        DO UPDATE SET kama = EXCLUDED.kama, updated_at_utc = NOW();
    """
    try:
        execute_values(cursor, sql, rows, page_size=1000)
        return len(rows)
    except Exception as exc:
        safe_rollback(conn)
        print(f"Database upsert failed: {exc}", flush=True)
        return -1


def main():
    started = perf_counter()
    conn = get_connection()
    cursor = conn.cursor()
    try:
        if CREATE_TARGET_IF_MISSING:
            create_target_table_and_indexes(cursor)
            conn.commit()
        else:
            print("Skipping CREATE SCHEMA / CREATE TABLE / CREATE INDEX.", flush=True)
        symbols = fetch_all_stock_symbols_from_master(cursor)
        print("=" * 70, flush=True)
        print("Historical DAILY KAMA Alpha Vantage 2-year load", flush=True)
        print(f"Start date: {HISTORY_START_DATE}", flush=True)
        print(f"Symbols: {len(symbols):,}", flush=True)
        print(f"Target: {TARGET_SCHEMA}.{TARGET_TABLE}", flush=True)
        print(f"Interval: {INTERVAL}", flush=True)
        print(f"Time period: {TIME_PERIOD}", flush=True)
        print(f"Series type: {SERIES_TYPE}", flush=True)
        print("No TRUNCATE. No DROP.", flush=True)
        print("=" * 70, flush=True)
        session = requests.Session()
        total_upserted = 0
        for i in range(0, len(symbols), BATCH_SIZE):
            batch_start = perf_counter()
            batch = symbols[i:i + BATCH_SIZE]
            batch_number = (i // BATCH_SIZE) + 1
            print(f"\nStarting batch {batch_number} with {len(batch)} symbols", flush=True)
            for j, symbol in enumerate(batch, start=1):
                rows = fetch_indicator(session, symbol)
                n = upsert(conn, cursor, rows)
                if n == -1:
                    print(f"{symbol}: upsert failed. Reconnecting and retrying once...", flush=True)
                    conn, cursor = reconnect_db(cursor, conn)
                    n = upsert(conn, cursor, rows)
                if n == -1:
                    print(f"{symbol}: retry failed. Skipping symbol.", flush=True)
                    n = 0
                else:
                    total_upserted += n
                print(f"{symbol}: batch {batch_number}.{j} kama_rows_upserted={n}", flush=True)
                if INTERVAL_PER_API > 0:
                    time.sleep(INTERVAL_PER_API)
            conn.commit()
            print(f"Batch {batch_number} committed. Elapsed: {perf_counter() - batch_start:.2f}s", flush=True)
            if BATCH_INTERVAL > 0:
                time.sleep(BATCH_INTERVAL)
        print("\n" + "=" * 70, flush=True)
        print("Finished historical DAILY KAMA load.", flush=True)
        print(f"Total symbols processed: {len(symbols):,}", flush=True)
        print(f"Total KAMA rows upserted: {total_upserted:,}", flush=True)
        print(f"Total elapsed time: {perf_counter() - started:.2f}s", flush=True)
        print("=" * 70, flush=True)
    finally:
        cursor.close()
        conn.close()
        print("Database connection closed.", flush=True)


if __name__ == "__main__":
    main()
