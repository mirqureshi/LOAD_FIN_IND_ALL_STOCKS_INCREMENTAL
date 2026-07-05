import os
import re
import time
from pathlib import Path
from time import perf_counter

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

print("Incremental DAILY AROON from daily_adjusted_data started...", flush=True)

load_dotenv(Path(__file__).with_name(".env"))


def need(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing env var: {name}")
    return value


# ============================================================
# DB config
# ============================================================

DB_HOST = need("DB_HOST_MAIN")
DB_PORT = os.getenv("DB_PORT_MAIN", "5432")
DB_NAME = need("DB_NAME_MAIN")
DB_USER = need("DB_USER_MAIN")
DB_PASS = need("DB_PASS_MAIN")
DB_SSLMODE = os.getenv("DB_SSLMODE_MAIN", "require").strip()


# ============================================================
# Fixed config
# ============================================================

STOCK_MASTER_SCHEMA = "FIN_IND"
STOCK_MASTER_TABLE = "us_stock_master"

SOURCE_SCHEMA = "FIN_IND"
SOURCE_TABLE = "daily_adjusted_data"
SOURCE_DATE_COLUMN = "price_date"

TARGET_SCHEMA = "FIN_IND"
TARGET_TABLE = "aroon_data"

INTERVAL = "daily"
TIME_PERIOD = 14

START_AFTER_TICKER = ""
MAX_STOCK_SYMBOLS = 0

BATCH_SIZE = 100
BATCH_INTERVAL = 0

DB_CONNECT_TIMEOUT_SECONDS = 15

# False by default to avoid read-only DDL errors during incremental runs.
CREATE_TARGET_IF_MISSING = False


# ============================================================
# Helpers
# ============================================================

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


# ============================================================
# Optional target table/indexes
# ============================================================

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
    ensure_unique_index_for_upsert(cursor)
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_symbol ON {schema}.{table} (symbol);")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_date ON {schema}.{table} (date);")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_symbol_date ON {schema}.{table} (symbol, date);")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_interval_period_date ON {schema}.{table} (interval, time_period, date);")


def ensure_unique_index_for_upsert(cursor):
    schema = safe_identifier(TARGET_SCHEMA)
    table = safe_identifier(TARGET_TABLE)

    print("Checking duplicate AROON rows before creating unique index...", flush=True)
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
        print(f"Found {duplicate_groups:,} duplicate AROON key groups. Removing duplicates and keeping lowest id...", flush=True)
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
        print("No duplicate AROON key groups found.", flush=True)

    unique_index_name = f"ux_{table}_symbol_interval_period_date_live"
    cursor.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {unique_index_name}
        ON {schema}.{table} (symbol, interval, time_period, date);
    """)
    print(f"Required unique index is ready on {schema}.{table}: {unique_index_name}", flush=True)


# ============================================================
# Fetch max AROON date
# ============================================================

def fetch_max_aroon_date(cursor):
    schema = safe_identifier(TARGET_SCHEMA)
    table = safe_identifier(TARGET_TABLE)

    query = f"""
        SELECT MAX(date)
        FROM {schema}.{table}
        ;
    """

    cursor.execute(query, (INTERVAL, TIME_PERIOD))
    row = cursor.fetchone()
    max_date = row[0] if row and row[0] else None
    print(f"MAX AROON date from {schema}.{table} for {INTERVAL}/{TIME_PERIOD}: {max_date}", flush=True)

    if max_date is None:
        raise SystemExit(f"No existing {INTERVAL} AROON found. Run the historical DAILY AROON load first.")

    return max_date


# ============================================================
# Fetch ALL stock symbols
# ============================================================

def fetch_all_stock_symbols_from_master(cursor):
    schema = safe_identifier(STOCK_MASTER_SCHEMA)
    table = safe_identifier(STOCK_MASTER_TABLE)

    where_conditions = ["ticker IS NOT NULL", "TRIM(ticker) <> ''"]
    params = []

    if START_AFTER_TICKER:
        where_conditions.append("TRIM(ticker) > %s")
        params.append(START_AFTER_TICKER.strip())

    where_sql = " AND ".join(where_conditions)
    limit_sql = ""
    if MAX_STOCK_SYMBOLS and MAX_STOCK_SYMBOLS > 0:
        limit_sql = f"LIMIT {int(MAX_STOCK_SYMBOLS)}"

    query = f"""
        SELECT DISTINCT TRIM(ticker) AS ticker
        FROM {schema}.{table}
        WHERE {where_sql}
        ORDER BY TRIM(ticker)
        {limit_sql};
    """

    cursor.execute(query, params)
    symbols = [row[0] for row in cursor.fetchall() if row[0]]
    print(f"Fetched {len(symbols):,} symbols from {schema}.{table}.", flush=True)
    return symbols


# ============================================================
# Fetch only needed daily price rows
# ============================================================

def fetch_daily_price_batch(conn, symbols, max_aroon_date):
    if not symbols:
        return pd.DataFrame(columns=["symbol", "date", "high", "low"])

    source_schema = safe_identifier(SOURCE_SCHEMA)
    source_table = safe_identifier(SOURCE_TABLE)
    source_date_column = safe_identifier(SOURCE_DATE_COLUMN)

    # AROON needs a recent high/low window for new rows. 4x period gives buffer for market holidays/weekends.
    lookback_days = TIME_PERIOD * 4

    query = f"""
        SELECT
            symbol,
            {source_date_column}::date AS date,
            high::numeric AS high,
            low::numeric AS low
        FROM {source_schema}.{source_table}
        WHERE symbol = ANY(%s)
          AND {source_date_column}::date >= (%s::date - %s::int)
          AND {source_date_column} IS NOT NULL
          AND high IS NOT NULL
          AND low IS NOT NULL
        ORDER BY symbol, {source_date_column} ASC;
    """

    with conn.cursor() as cur:
        cur.execute(query, (symbols, max_aroon_date, lookback_days))
        rows = cur.fetchall()

    if not rows:
        return pd.DataFrame(columns=["symbol", "date", "high", "low"])

    return pd.DataFrame(rows, columns=["symbol", "date", "high", "low"])


# ============================================================
# DAILY AROON calculation
# ============================================================

def calculate_daily_aroon_for_symbol(df):
    df = df.copy()
    df = df.sort_values("date").reset_index(drop=True)

    if len(df) < TIME_PERIOD:
        return pd.DataFrame(columns=["symbol", "date", "aroon_up", "aroon_down"])

    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)

    rows = []
    n = TIME_PERIOD

    for i in range(n - 1, len(df)):
        window = df.iloc[i - n + 1:i + 1]
        high_values = window["high"].tolist()
        low_values = window["low"].tolist()

        highest_high = max(high_values)
        lowest_low = min(low_values)

        # Most recent occurrence if duplicate highs/lows exist.
        high_index_from_start = max(idx for idx, value in enumerate(high_values) if value == highest_high)
        low_index_from_start = max(idx for idx, value in enumerate(low_values) if value == lowest_low)

        periods_since_high = (n - 1) - high_index_from_start
        periods_since_low = (n - 1) - low_index_from_start

        aroon_up = ((n - periods_since_high) / n) * 100
        aroon_down = ((n - periods_since_low) / n) * 100

        rows.append((df.loc[i, "symbol"], df.loc[i, "date"], aroon_up, aroon_down))

    return pd.DataFrame(rows, columns=["symbol", "date", "aroon_up", "aroon_down"])


def build_daily_aroon_rows_for_batch(price_df, max_aroon_date):
    if price_df.empty:
        return []

    rows = []
    for symbol, symbol_prices in price_df.groupby("symbol"):
        aroon_df = calculate_daily_aroon_for_symbol(symbol_prices)
        for _, row in aroon_df.iterrows():
            if row["date"] <= max_aroon_date:
                continue
            rows.append((
                symbol,
                INTERVAL,
                TIME_PERIOD,
                row["date"],
                round(float(row["aroon_up"]), 4),
                round(float(row["aroon_down"]), 4),
            ))
    return rows


# ============================================================
# Upsert
# ============================================================

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


# ============================================================
# Main
# ============================================================

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

        max_aroon_date = fetch_max_aroon_date(cursor)
        symbols = fetch_all_stock_symbols_from_master(cursor)

        print("=" * 70, flush=True)
        print("Incremental DAILY AROON from daily_adjusted_data", flush=True)
        print(f"Source table: {SOURCE_SCHEMA}.{SOURCE_TABLE}", flush=True)
        print(f"Target table: {TARGET_SCHEMA}.{TARGET_TABLE}", flush=True)
        print(f"Interval: {INTERVAL}", flush=True)
        print(f"Time period: {TIME_PERIOD}", flush=True)
        print(f"MAX AROON date used: {max_aroon_date}", flush=True)
        print(f"Total symbols: {len(symbols):,}", flush=True)
        print("No Alpha Vantage calls. Daily calculation from daily_adjusted_data only.", flush=True)
        print("No weekly conversion. No TRUNCATE. No DROP.", flush=True)
        print("=" * 70, flush=True)

        total_upserted = 0

        for i in range(0, len(symbols), BATCH_SIZE):
            batch_start = perf_counter()
            batch = symbols[i:i + BATCH_SIZE]
            batch_number = (i // BATCH_SIZE) + 1

            price_df = fetch_daily_price_batch(conn, batch, max_aroon_date)
            rows = build_daily_aroon_rows_for_batch(price_df, max_aroon_date)
            n = upsert_aroon_rows(conn, cursor, rows)

            if n == -1:
                print(f"Batch {batch_number}: upsert failed. Reconnecting and retrying once...", flush=True)
                conn, cursor = reconnect_db(cursor, conn)
                n = upsert_aroon_rows(conn, cursor, rows)

            if n == -1:
                print(f"Batch {batch_number}: retry failed. Skipping this batch.", flush=True)
                n = 0
            else:
                conn.commit()

            total_upserted += n
            print(
                f"Batch {batch_number} committed. "
                f"symbols={len(batch)} price_rows={len(price_df):,} "
                f"new_aroon_rows_upserted={n:,} elapsed={perf_counter() - batch_start:.2f}s",
                flush=True
            )

            if BATCH_INTERVAL > 0:
                time.sleep(BATCH_INTERVAL)

        print("\n" + "=" * 70, flush=True)
        print("Finished incremental DAILY AROON load.", flush=True)
        print(f"Total symbols processed: {len(symbols):,}", flush=True)
        print(f"Total new AROON rows upserted: {total_upserted:,}", flush=True)
        print(f"Total elapsed time: {perf_counter() - started:.2f}s", flush=True)
        print("=" * 70, flush=True)

    finally:
        cursor.close()
        conn.close()
        print("Database connection closed.", flush=True)


if __name__ == "__main__":
    main()
