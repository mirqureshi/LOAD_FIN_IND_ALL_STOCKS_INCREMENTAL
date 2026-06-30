import os
import time
import re
from pathlib import Path
from time import perf_counter
from datetime import datetime

print("Intraday incremental script started...", flush=True)

import requests
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

print("Imports completed.", flush=True)


# ============================================================
# Load .env
#
# Keep only secrets / connection values in .env:
#
# DB_HOST_MAIN=stockdata.postgres.database.azure.com
# DB_PORT_MAIN=5432
# DB_NAME_MAIN=postgres
# DB_USER_MAIN=lilbraveh
# DB_PASS_MAIN=your_password_here
# DB_SSLMODE_MAIN=require
# API_KEY_MAIN=your_alpha_vantage_key_here
# ============================================================

ENV_PATH = Path(__file__).with_name(".env")
print(f"Loading .env from: {ENV_PATH}", flush=True)
load_dotenv(ENV_PATH)
print(".env loaded.", flush=True)


# ============================================================
# Environment helper
# ============================================================

def need(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing env var: {name}")
    return value


# ============================================================
# Database secrets / connection from .env
# ============================================================

DB_HOST = need("DB_HOST_MAIN")
DB_PORT = os.getenv("DB_PORT_MAIN", "5432")
DB_NAME = need("DB_NAME_MAIN")
DB_USER = need("DB_USER_MAIN")
DB_PASS = need("DB_PASS_MAIN")
DB_SSLMODE = os.getenv("DB_SSLMODE_MAIN", "require").strip()

API_KEY = need("API_KEY_MAIN")


# ============================================================
# Fixed configuration in Python
# ============================================================

# Source table containing all tickers
STOCK_MASTER_SCHEMA = "FIN_IND"
STOCK_MASTER_TABLE = "us_stock_master"

# Target intraday table.
# This script reads MAX(price_datetime) from this same table
# and appends/upserts only newer rows.
TARGET_SCHEMA = "FIN_IND"
TARGET_TABLE = "intraday_data"

# Alpha Vantage intraday interval.
# Valid values: 1min, 5min, 15min, 30min, 60min
INTERVAL = "15min"

# compact = latest 100 bars
# full = trailing 30 days for TIME_SERIES_INTRADAY
# For incremental daily/hourly runs, compact is usually enough.
OUTPUTSIZE = "full"

# Production table should already exist.
# Set these to True only if you want the script to create/check table/index.
# This script NEVER truncates, deletes, drops, or rebuilds the table.
CREATE_TARGET_IF_MISSING = False
CREATE_UNIQUE_INDEX_IF_MISSING = False

# IMPORTANT:
# No stock-type filters. This pulls every non-empty ticker from us_stock_master.
START_AFTER_TICKER = ""

# 0 means no limit. Use 5 or 10 for testing only.
MAX_STOCK_SYMBOLS = 0

# Batch/rate settings
BATCH_SIZE = 500
INTERVAL_PER_API = 0.07
BATCH_INTERVAL = 1

# Timeouts
DB_CONNECT_TIMEOUT_SECONDS = 15
API_TIMEOUT_SECONDS = 60


# ============================================================
# SQL identifier safety
# ============================================================

def safe_identifier(name: str) -> str:
    """
    Keeps schema/table names safe because they are inserted into SQL strings.
    PostgreSQL folds unquoted identifiers to lowercase, so FIN_IND becomes fin_ind.
    """
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid SQL identifier: {name}")
    return name


# ============================================================
# Database connection
# ============================================================

def get_connection():
    print("Connecting to database...", flush=True)

    conn_args = {
        "dbname": DB_NAME,
        "user": DB_USER,
        "password": DB_PASS,
        "host": DB_HOST,
        "port": int(DB_PORT),
        "connect_timeout": DB_CONNECT_TIMEOUT_SECONDS,
    }

    if DB_SSLMODE:
        conn_args["sslmode"] = DB_SSLMODE

    conn = psycopg2.connect(**conn_args)
    print("Database connected.", flush=True)
    return conn


# ============================================================
# Type converters
# ============================================================

def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_bigint(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def to_timestamp(value):
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return None


# ============================================================
# Optional create target table and indexes
# Disabled by default for production.
# There is no TRUNCATE, DELETE, DROP, or table rebuild anywhere in this script.
# ============================================================

def create_target_table_and_indexes(cursor):
    schema = safe_identifier(TARGET_SCHEMA)
    table = safe_identifier(TARGET_TABLE)

    print(f"Creating/checking target schema {schema}...", flush=True)
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    print(f"Creating/checking target table {schema}.{table}...", flush=True)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            id BIGSERIAL PRIMARY KEY,

            symbol VARCHAR(30) NOT NULL,
            interval VARCHAR(10) NOT NULL,
            price_datetime TIMESTAMP NOT NULL,

            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume BIGINT,

            inserted_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """)

    if CREATE_UNIQUE_INDEX_IF_MISSING:
        print("Creating/checking unique index needed for ON CONFLICT...", flush=True)
        cursor.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_{table}_symbol_interval_datetime
            ON {schema}.{table} (symbol, interval, price_datetime);
        """)

    print("Creating/checking regular indexes...", flush=True)

    cursor.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{table}_symbol
        ON {schema}.{table} (symbol);
    """)

    cursor.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{table}_price_datetime
        ON {schema}.{table} (price_datetime);
    """)

    cursor.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{table}_symbol_interval_datetime
        ON {schema}.{table} (symbol, interval, price_datetime);
    """)


# ============================================================
# Get cutoff max datetime from FIN_IND.intraday_data
# ============================================================

def fetch_max_datetime_from_intraday_table(cursor):
    schema = safe_identifier(TARGET_SCHEMA)
    table = safe_identifier(TARGET_TABLE)

    print(f"Fetching MAX(price_datetime) from {schema}.{table} for interval={INTERVAL}...", flush=True)

    query = f"""
        SELECT MAX(price_datetime) AS max_price_datetime
        FROM {schema}.{table}
        
        ;
    """

    cursor.execute(query, (INTERVAL,))
    row = cursor.fetchone()

    max_datetime = row[0] if row and row[0] else None

    print(f"MAX(price_datetime) from {schema}.{table} for interval={INTERVAL}: {max_datetime}", flush=True)

    if max_datetime is None:
        raise SystemExit(
            f"ERROR: MAX(price_datetime) from {schema}.{table} is None for interval={INTERVAL}. "
            "Stopping so the script does not accidentally reload full intraday history."
        )

    return max_datetime


# ============================================================
# Fetch ALL stock symbols from FIN_IND.us_stock_master
# No OTC/ETF/test issue filters. All non-empty tickers are included.
# ============================================================

def fetch_all_stock_symbols_from_master(cursor):
    schema = safe_identifier(STOCK_MASTER_SCHEMA)
    table = safe_identifier(STOCK_MASTER_TABLE)

    print(f"Fetching ALL symbols from {schema}.{table} with no stock-type filters...", flush=True)

    params = []

    where_conditions = [
        "ticker IS NOT NULL",
        "TRIM(ticker) <> ''"
    ]

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

    print(f"Fetched {len(symbols):,} symbols from stock master.", flush=True)
    return symbols


# ============================================================
# Fetch Alpha Vantage intraday data for one symbol
# and insert only newer rows
# ============================================================

def fetch_and_store_intraday(conn, cursor, session, symbol, max_datetime_from_intraday_table):
    schema = safe_identifier(TARGET_SCHEMA)
    table = safe_identifier(TARGET_TABLE)

    url = (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_INTRADAY"
        f"&symbol={symbol}"
        f"&interval={INTERVAL}"
        f"&outputsize={OUTPUTSIZE}"
        f"&apikey={API_KEY}"
    )

    try:
        response = session.get(url, timeout=API_TIMEOUT_SECONDS)
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        print(f"{symbol}: API request failed: {exc}", flush=True)
        return 0, 0

    time_series_key = f"Time Series ({INTERVAL})"

    if time_series_key not in data:
        message = (
            data.get("Note")
            or data.get("Information")
            or data.get("Error Message")
            or str(data)[:500]
        )
        print(f"{symbol}: No intraday data returned. Message: {message}", flush=True)
        return 0, 0

    rows = []
    api_rows_seen = 0

    for timestamp_str, values in data.get(time_series_key, {}).items():
        price_datetime = to_timestamp(timestamp_str)
        if price_datetime is None:
            continue

        api_rows_seen += 1

        # IMPORTANT:
        # Alpha Vantage intraday does not accept a start datetime parameter.
        # We fetch compact data, then filter in Python.
        #
        # Example:
        # If FIN_IND.intraday_data max price_datetime is 2026-06-18 15:45:00,
        # this inserts only rows where price_datetime > 2026-06-18 15:45:00.
        if price_datetime <= max_datetime_from_intraday_table:
            continue

        rows.append((
            symbol,
            INTERVAL,
            price_datetime,
            to_float(values.get("1. open")),
            to_float(values.get("2. high")),
            to_float(values.get("3. low")),
            to_float(values.get("4. close")),
            to_bigint(values.get("5. volume")),
        ))

    if not rows:
        return 0, api_rows_seen

    insert_statement = f"""
        INSERT INTO {schema}.{table} (
            symbol,
            interval,
            price_datetime,
            open,
            high,
            low,
            close,
            volume
        )
        VALUES %s
        ON CONFLICT (symbol, interval, price_datetime)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            updated_at_utc = NOW();
    """

    try:
        execute_values(cursor, insert_statement, rows, page_size=1000)
        return len(rows), api_rows_seen

    except Exception as exc:
        conn.rollback()
        print(f"{symbol}: database upsert failed: {exc}", flush=True)
        return 0, api_rows_seen


# ============================================================
# Main process
# ============================================================

def main():
    started = perf_counter()
    print("Main started.", flush=True)

    conn = get_connection()
    cursor = conn.cursor()

    try:
        print("=" * 70, flush=True)
        print("Configuration:", flush=True)
        print(f"Stock master table: {STOCK_MASTER_SCHEMA}.{STOCK_MASTER_TABLE}", flush=True)
        print(f"Target/load table: {TARGET_SCHEMA}.{TARGET_TABLE}", flush=True)
        print(f"Max datetime source table: {TARGET_SCHEMA}.{TARGET_TABLE}", flush=True)
        print(f"Interval: {INTERVAL}", flush=True)
        print(f"Output size: {OUTPUTSIZE}", flush=True)
        print(f"Start after ticker: {START_AFTER_TICKER if START_AFTER_TICKER else 'None'}", flush=True)
        print(f"Max symbols: {MAX_STOCK_SYMBOLS if MAX_STOCK_SYMBOLS else 'No limit'}", flush=True)
        print(f"Batch size: {BATCH_SIZE}", flush=True)
        print(f"Sleep per API call: {INTERVAL_PER_API}", flush=True)
        print(f"Sleep per batch: {BATCH_INTERVAL}", flush=True)
        print(f"Create target if missing: {CREATE_TARGET_IF_MISSING}", flush=True)
        print(f"Create unique index if missing: {CREATE_UNIQUE_INDEX_IF_MISSING}", flush=True)
        print("No TRUNCATE. No DELETE. No DROP. Incremental upsert only.", flush=True)
        print("=" * 70, flush=True)

        if CREATE_TARGET_IF_MISSING:
            try:
                create_target_table_and_indexes(cursor)
                conn.commit()
                print("Target table/index check completed.", flush=True)
            except Exception as exc:
                conn.rollback()
                print("Target table/index creation failed.", flush=True)
                print(f"Error: {exc}", flush=True)
                print("If this is read-only, set CREATE_TARGET_IF_MISSING=False after table/index exists.", flush=True)
                raise
        else:
            print("Skipping CREATE TABLE / CREATE INDEX.", flush=True)

        # Step 1: Get the overall max intraday datetime from FIN_IND.intraday_data.
        max_datetime_from_intraday_table = fetch_max_datetime_from_intraday_table(cursor)

        # Step 2: Get all tickers from FIN_IND.us_stock_master.
        stock_symbols = fetch_all_stock_symbols_from_master(cursor)

        print("=" * 70, flush=True)
        print(f"Total stock master symbols to process: {len(stock_symbols):,}", flush=True)
        print(f"Using MAX price_datetime from {TARGET_SCHEMA}.{TARGET_TABLE}: {max_datetime_from_intraday_table}", flush=True)
        print(f"Will append/upsert only rows where price_datetime > {max_datetime_from_intraday_table}", flush=True)
        print("No stock-type filters are applied. All non-empty tickers are included.", flush=True)
        print("=" * 70, flush=True)

        session = requests.Session()

        total_upserted = 0
        total_api_rows_seen = 0
        total_no_new_data = 0

        for i in range(0, len(stock_symbols), BATCH_SIZE):
            batch_start = perf_counter()
            batch_symbols = stock_symbols[i:i + BATCH_SIZE]
            batch_number = (i // BATCH_SIZE) + 1

            print(f"\nStarting batch {batch_number} with {len(batch_symbols)} symbols", flush=True)

            for j, symbol in enumerate(batch_symbols, start=1):
                n, api_rows_seen = fetch_and_store_intraday(
                    conn=conn,
                    cursor=cursor,
                    session=session,
                    symbol=symbol,
                    max_datetime_from_intraday_table=max_datetime_from_intraday_table
                )

                total_upserted += n
                total_api_rows_seen += api_rows_seen

                if n == 0:
                    total_no_new_data += 1

                print(
                    f"{symbol}: batch {batch_number}.{j} "
                    f"max_price_datetime={max_datetime_from_intraday_table} "
                    f"api_rows_seen={api_rows_seen} "
                    f"new_rows_upserted={n}",
                    flush=True
                )

                if INTERVAL_PER_API > 0:
                    time.sleep(INTERVAL_PER_API)

            conn.commit()
            elapsed = perf_counter() - batch_start
            print(f"Batch {batch_number} committed. Elapsed: {elapsed:.2f}s", flush=True)

            if BATCH_INTERVAL > 0:
                time.sleep(BATCH_INTERVAL)

        total_elapsed = perf_counter() - started

        print("\n" + "=" * 70, flush=True)
        print("Finished incremental update into original intraday_data table.", flush=True)
        print(f"Total symbols processed: {len(stock_symbols):,}", flush=True)
        print(f"Interval: {INTERVAL}", flush=True)
        print(f"MAX price_datetime used: {max_datetime_from_intraday_table}", flush=True)
        print(f"Target table updated: {TARGET_SCHEMA}.{TARGET_TABLE}", flush=True)
        print(f"Total API rows seen: {total_api_rows_seen:,}", flush=True)
        print(f"Total new rows upserted: {total_upserted:,}", flush=True)
        print(f"Symbols with no new rows: {total_no_new_data:,}", flush=True)
        print(f"Total elapsed time: {total_elapsed:.2f}s", flush=True)
        print("=" * 70, flush=True)

    finally:
        cursor.close()
        conn.close()
        print("Database connection closed.", flush=True)


if __name__ == "__main__":
    main()
