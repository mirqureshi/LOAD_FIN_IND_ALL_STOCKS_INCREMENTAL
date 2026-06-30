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

print("Historical weekly AROON from Alpha Vantage started...", flush=True)

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
TARGET_TABLE = "aroon_data"

INTERVAL = "weekly"
TIME_PERIOD = 14

START_AFTER_TICKER = ""
MAX_STOCK_SYMBOLS = 0

BATCH_SIZE = 50
INTERVAL_PER_API = 1
BATCH_INTERVAL = 1

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
    return psycopg2.connect(**args)


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
    new_conn = get_connection()
    new_cursor = new_conn.cursor()
    print("Database reconnected.", flush=True)
    return new_conn, new_cursor


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
            aroon_up NUMERIC,
            aroon_down NUMERIC,
            inserted_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """)

    cursor.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_{table}_symbol_interval_period_date
        ON {schema}.{table} (symbol, interval, time_period, date);
    """)

    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_symbol ON {schema}.{table} (symbol);")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_date ON {schema}.{table} (date);")


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
        WHERE {where_sql}
        ORDER BY ticker
        {limit_sql};
    """

    cursor.execute(query, params)
    symbols = [row[0] for row in cursor.fetchall() if row[0]]
    print(f"Fetched {len(symbols):,} symbols from {schema}.{table}.", flush=True)
    return symbols


def fetch_alpha_vantage_aroon(session, symbol):
    url = (
        "https://www.alphavantage.co/query"
        f"?function=AROON"
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

    key = "Technical Analysis: AROON"
    if key not in data:
        message = data.get("Note") or data.get("Information") or data.get("Error Message") or str(data)[:400]
        print(f"{symbol}: no AROON data. Message: {message}", flush=True)
        return []

    rows = []
    for date_str, item in data.get(key, {}).items():
        d = parse_date(date_str)
        aroon_up = parse_float(item.get("Aroon Up"))
        aroon_down = parse_float(item.get("Aroon Down"))

        if d is None or aroon_up is None or aroon_down is None:
            continue

        rows.append((symbol, INTERVAL, TIME_PERIOD, d, round(aroon_up, 4), round(aroon_down, 4)))

    return rows


def upsert_aroon_rows(conn, cursor, rows):
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
            aroon_up,
            aroon_down
        )
        VALUES %s
        ON CONFLICT (
            symbol,
            interval,
            time_period,
            date
        )
        DO UPDATE SET
            aroon_up = EXCLUDED.aroon_up,
            aroon_down = EXCLUDED.aroon_down,
            updated_at_utc = NOW();
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
        print("Historical weekly AROON from Alpha Vantage", flush=True)
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
                rows = fetch_alpha_vantage_aroon(session, symbol)
                n = upsert_aroon_rows(conn, cursor, rows)

                if n == -1:
                    print(f"{symbol}: first upsert failed. Reconnecting and retrying once...", flush=True)
                    conn, cursor = reconnect_db(cursor, conn)
                    n = upsert_aroon_rows(conn, cursor, rows)

                if n == -1:
                    print(f"{symbol}: retry failed. Skipping symbol.", flush=True)
                    n = 0
                else:
                    conn.commit()

                total_upserted += n
                print(f"{symbol}: batch {batch_number}.{j} aroon_rows_upserted={n}", flush=True)

                if INTERVAL_PER_API > 0:
                    time.sleep(INTERVAL_PER_API)

            print(f"Batch {batch_number} completed. Elapsed: {perf_counter() - batch_start:.2f}s", flush=True)

            if BATCH_INTERVAL > 0:
                time.sleep(BATCH_INTERVAL)

        print("\n" + "=" * 70, flush=True)
        print("Finished historical weekly AROON load.", flush=True)
        print(f"Total symbols processed: {len(symbols):,}", flush=True)
        print(f"Total AROON rows upserted: {total_upserted:,}", flush=True)
        print(f"Total elapsed time: {perf_counter() - started:.2f}s", flush=True)
        print("=" * 70, flush=True)

    finally:
        cursor.close()
        conn.close()
        print("Database connection closed.", flush=True)


if __name__ == "__main__":
    main()
