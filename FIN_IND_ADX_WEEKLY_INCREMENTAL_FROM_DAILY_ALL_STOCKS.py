import os
import re
import time
from pathlib import Path
from time import perf_counter

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

print("Incremental weekly ADX from daily_adjusted_data started...", flush=True)

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

STOCK_MASTER_SCHEMA = "FIN_IND"
STOCK_MASTER_TABLE = "us_stock_master"

SOURCE_SCHEMA = "FIN_IND"
SOURCE_TABLE = "daily_adjusted_data"
SOURCE_DATE_COLUMN = "price_date"

TARGET_SCHEMA = "FIN_IND"
TARGET_TABLE = "adx_data"

INTERVAL = "weekly"
TIME_PERIOD = 14

START_AFTER_TICKER = ""
MAX_STOCK_SYMBOLS = 0

BATCH_SIZE = 250
BATCH_INTERVAL = 0

DB_CONNECT_TIMEOUT_SECONDS = 15


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

    cursor.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_{table}_symbol_interval_period_date
        ON {schema}.{table} (symbol, interval, time_period, date);
    """)

    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_symbol ON {schema}.{table} (symbol);")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_date ON {schema}.{table} (date);")


def fetch_max_adx_date(cursor):
    schema = safe_identifier(TARGET_SCHEMA)
    table = safe_identifier(TARGET_TABLE)

    query = f"""
        SELECT MAX(date)
        FROM {schema}.{table}
        WHERE interval = %s
          AND time_period = %s;
    """

    cursor.execute(query, (INTERVAL, TIME_PERIOD))
    row = cursor.fetchone()
    max_date = row[0] if row and row[0] else None

    print(f"MAX ADX date from {schema}.{table} for {INTERVAL}/{TIME_PERIOD}: {max_date}", flush=True)

    if max_date is None:
        raise SystemExit(
            "No existing weekly ADX found. Run the historical Alpha Vantage weekly ADX load first."
        )

    return max_date


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


def fetch_price_batch(conn, symbols):
    if not symbols:
        return pd.DataFrame(columns=["symbol", "date", "high", "low", "close"])

    source_schema = safe_identifier(SOURCE_SCHEMA)
    source_table = safe_identifier(SOURCE_TABLE)
    source_date_column = safe_identifier(SOURCE_DATE_COLUMN)

    query = f"""
        SELECT
            UPPER(TRIM(symbol)) AS symbol,
            {source_date_column}::date AS date,
            high::numeric AS high,
            low::numeric AS low,
            close::numeric AS close
        FROM {source_schema}.{source_table}
        WHERE UPPER(TRIM(symbol)) = ANY(%s)
          AND {source_date_column} IS NOT NULL
          AND high IS NOT NULL
          AND low IS NOT NULL
          AND close IS NOT NULL
        ORDER BY UPPER(TRIM(symbol)), {source_date_column} ASC;
    """

    with conn.cursor() as cur:
        cur.execute(query, (symbols,))
        rows = cur.fetchall()

    return pd.DataFrame(rows, columns=["symbol", "date", "high", "low", "close"])


def daily_to_weekly_ohlc(df):
    if df.empty:
        return pd.DataFrame(columns=["symbol", "date", "high", "low", "close"])

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

    # Weekly bars ending Friday. Stored date is actual last trading date in that week.
    df["week_end"] = df["date"].dt.to_period("W-FRI").dt.end_time.dt.date

    weekly = (
        df.groupby(["symbol", "week_end"], as_index=False)
          .agg(
              date=("date", "max"),
              high=("high", "max"),
              low=("low", "min"),
              close=("close", "last")
          )
    )

    weekly["date"] = pd.to_datetime(weekly["date"]).dt.date
    return weekly.sort_values(["symbol", "date"]).reset_index(drop=True)


def calculate_weekly_adx_for_symbol(df):
    """
    Calculates weekly ADX using Wilder-style smoothing.

    Historical baseline comes from Alpha Vantage.
    Incremental rows are calculated from daily_adjusted_data.
    """
    df = df.copy()
    df = df.sort_values("date").reset_index(drop=True)

    min_rows_needed = (TIME_PERIOD * 2)
    if len(df) < min_rows_needed:
        return pd.DataFrame(columns=["symbol", "date", "adx"])

    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["close"] = df["close"].astype(float)

    prev_high = df["high"].shift(1)
    prev_low = df["low"].shift(1)
    prev_close = df["close"].shift(1)

    up_move = df["high"] - prev_high
    down_move = prev_low - df["low"]

    df["plus_dm"] = 0.0
    df["minus_dm"] = 0.0
    df.loc[(up_move > down_move) & (up_move > 0), "plus_dm"] = up_move
    df.loc[(down_move > up_move) & (down_move > 0), "minus_dm"] = down_move

    high_low = df["high"] - df["low"]
    high_prev_close = (df["high"] - prev_close).abs()
    low_prev_close = (df["low"] - prev_close).abs()

    df["tr"] = pd.concat([high_low, high_prev_close, low_prev_close], axis=1).max(axis=1)
    df.loc[0, "tr"] = None

    n = TIME_PERIOD
    tr = df["tr"].tolist()
    plus_dm = df["plus_dm"].tolist()
    minus_dm = df["minus_dm"].tolist()

    smoothed_tr = [None] * len(df)
    smoothed_plus_dm = [None] * len(df)
    smoothed_minus_dm = [None] * len(df)
    plus_di = [None] * len(df)
    minus_di = [None] * len(df)
    dx = [None] * len(df)
    adx = [None] * len(df)

    # Initial Wilder sums use rows 1..n.
    smoothed_tr[n] = sum(x for x in tr[1:n + 1] if x is not None)
    smoothed_plus_dm[n] = sum(plus_dm[1:n + 1])
    smoothed_minus_dm[n] = sum(minus_dm[1:n + 1])

    for i in range(n, len(df)):
        if i > n:
            if tr[i] is None:
                continue
            smoothed_tr[i] = smoothed_tr[i - 1] - (smoothed_tr[i - 1] / n) + tr[i]
            smoothed_plus_dm[i] = smoothed_plus_dm[i - 1] - (smoothed_plus_dm[i - 1] / n) + plus_dm[i]
            smoothed_minus_dm[i] = smoothed_minus_dm[i - 1] - (smoothed_minus_dm[i - 1] / n) + minus_dm[i]

        if not smoothed_tr[i]:
            continue

        plus_di[i] = 100 * smoothed_plus_dm[i] / smoothed_tr[i]
        minus_di[i] = 100 * smoothed_minus_dm[i] / smoothed_tr[i]

        denom = plus_di[i] + minus_di[i]
        if denom:
            dx[i] = 100 * abs(plus_di[i] - minus_di[i]) / denom

    first_adx_index = (2 * n) - 1
    first_dx_values = [x for x in dx[n:first_adx_index + 1] if x is not None]

    if len(first_dx_values) < n:
        return pd.DataFrame(columns=["symbol", "date", "adx"])

    adx[first_adx_index] = sum(first_dx_values) / n

    for i in range(first_adx_index + 1, len(df)):
        if dx[i] is None:
            continue
        adx[i] = ((adx[i - 1] * (n - 1)) + dx[i]) / n

    df["adx"] = adx
    return df.dropna(subset=["adx"])[["symbol", "date", "adx"]]


def build_weekly_adx_rows_for_batch(price_df, max_adx_date):
    if price_df.empty:
        return []

    weekly_df = daily_to_weekly_ohlc(price_df)
    if weekly_df.empty:
        return []

    rows = []
    for symbol, symbol_weekly in weekly_df.groupby("symbol"):
        adx_df = calculate_weekly_adx_for_symbol(symbol_weekly)

        for _, row in adx_df.iterrows():
            if row["date"] <= max_adx_date:
                continue

            rows.append((
                symbol,
                INTERVAL,
                TIME_PERIOD,
                row["date"],
                round(float(row["adx"]), 4),
            ))

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

        max_adx_date = fetch_max_adx_date(cursor)
        symbols = fetch_all_stock_symbols_from_master(cursor)

        print("=" * 70, flush=True)
        print("Incremental weekly ADX from daily_adjusted_data", flush=True)
        print(f"Source table: {SOURCE_SCHEMA}.{SOURCE_TABLE}", flush=True)
        print(f"Target table: {TARGET_SCHEMA}.{TARGET_TABLE}", flush=True)
        print(f"Interval: {INTERVAL}", flush=True)
        print(f"Time period: {TIME_PERIOD}", flush=True)
        print(f"MAX ADX date used: {max_adx_date}", flush=True)
        print(f"Total symbols: {len(symbols):,}", flush=True)
        print("All non-empty tickers are included.", flush=True)
        print("No Alpha Vantage calls. Incremental calculation from daily_adjusted_data only.", flush=True)
        print("=" * 70, flush=True)

        total_upserted = 0

        for i in range(0, len(symbols), BATCH_SIZE):
            batch_start = perf_counter()
            batch = symbols[i:i + BATCH_SIZE]
            batch_number = (i // BATCH_SIZE) + 1

            price_df = fetch_price_batch(conn, batch)
            rows = build_weekly_adx_rows_for_batch(price_df, max_adx_date)
            n = upsert_adx_rows(conn, cursor, rows)
            total_upserted += n

            conn.commit()
            elapsed = perf_counter() - batch_start
            print(
                f"Batch {batch_number} committed. "
                f"symbols={len(batch)} price_rows={len(price_df):,} "
                f"new_adx_rows_upserted={n:,} elapsed={elapsed:.2f}s",
                flush=True
            )

            if BATCH_INTERVAL > 0:
                time.sleep(BATCH_INTERVAL)

        print("\n" + "=" * 70, flush=True)
        print("Finished incremental weekly ADX load.", flush=True)
        print(f"Total symbols processed: {len(symbols):,}", flush=True)
        print(f"Total new ADX rows upserted: {total_upserted:,}", flush=True)
        print(f"Total elapsed time: {perf_counter() - started:.2f}s", flush=True)
        print("=" * 70, flush=True)

    finally:
        cursor.close()
        conn.close()
        print("Database connection closed.", flush=True)


if __name__ == "__main__":
    main()
