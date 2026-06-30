import os
import time
import re
import hashlib
from pathlib import Path
from time import perf_counter
from datetime import datetime, timedelta, timezone

print("News sentiment script started...", flush=True)

import requests
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values, Json

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

# Source table containing all tickers.
STOCK_MASTER_SCHEMA = "FIN_IND"
STOCK_MASTER_TABLE = "us_stock_master"

# Target news sentiment table.
TARGET_SCHEMA = "FIN_IND"
TARGET_TABLE = "news_sentiment_data"

# Alpha Vantage NEWS_SENTIMENT.
NEWS_LIMIT = 1000
NEWS_SORT = "LATEST"

# IMPORTANT:
# No stock-type filters. This pulls every non-empty ticker from us_stock_master.
START_AFTER_TICKER = ""

# 0 means no limit. Use 5 or 10 for testing only.
MAX_STOCK_SYMBOLS = 0

# Batch/rate settings.
BATCH_SIZE = 50
INTERVAL_PER_API = 1
BATCH_INTERVAL = 1

# Timeouts.
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
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_alpha_time(value):
    """
    Alpha Vantage NEWS_SENTIMENT time_published format is usually:
        20240618T153000
    """
    if not value:
        return None

    for fmt in ("%Y%m%dT%H%M%S", "%Y%m%dT%H%M"):
        try:
            return datetime.strptime(value, fmt)
        except (TypeError, ValueError):
            pass

    return None


def format_alpha_time(dt):
    """
    Alpha Vantage time_from/time_to format:
        YYYYMMDDTHHMM
    """
    return dt.strftime("%Y%m%dT%H%M")


def make_article_key(symbol, url, time_published, title):
    """
    Unique key is per symbol + article.
    This allows the same news article to be stored once for each ticker it is loaded under.
    """
    raw = f"{symbol}|{url or ''}|{time_published or ''}|{title or ''}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ============================================================
# Create target table and indexes
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

            article_key VARCHAR(64) NOT NULL,

            symbol VARCHAR(30) NOT NULL,
            time_published TIMESTAMP NOT NULL,

            title TEXT,
            url TEXT,
            source TEXT,
            source_domain TEXT,
            authors TEXT,
            summary TEXT,
            banner_image TEXT,
            category_within_source TEXT,
            source_type TEXT,

            overall_sentiment_score NUMERIC,
            overall_sentiment_label VARCHAR(100),

            ticker_relevance_score NUMERIC,
            ticker_sentiment_score NUMERIC,
            ticker_sentiment_label VARCHAR(100),

            topics JSONB,
            ticker_sentiment JSONB,
            raw_article JSONB,

            inserted_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """)

    print("Creating/checking unique index needed for ON CONFLICT...", flush=True)
    cursor.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_{table}_article_key
        ON {schema}.{table} (article_key);
    """)

    print("Creating/checking regular indexes...", flush=True)

    cursor.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{table}_symbol
        ON {schema}.{table} (symbol);
    """)

    cursor.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{table}_time_published
        ON {schema}.{table} (time_published);
    """)

    cursor.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{table}_symbol_time
        ON {schema}.{table} (symbol, time_published);
    """)


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
# NEWS_SENTIMENT API and parsing
# ============================================================

def call_news_sentiment_api(session, symbol, time_from=None, time_to=None):
    params = {
        "function": "NEWS_SENTIMENT",
        "tickers": symbol,
        "sort": NEWS_SORT,
        "limit": str(NEWS_LIMIT),
        "apikey": API_KEY,
    }

    if time_from:
        params["time_from"] = time_from

    if time_to:
        params["time_to"] = time_to

    try:
        response = session.get(
            "https://www.alphavantage.co/query",
            params=params,
            timeout=API_TIMEOUT_SECONDS
        )
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        print(f"{symbol}: API request failed: {exc}", flush=True)
        return None


def get_symbol_ticker_sentiment(article, symbol):
    target = symbol.upper().strip()

    for item in article.get("ticker_sentiment", []) or []:
        ticker = str(item.get("ticker", "")).upper().strip()
        if ticker == target:
            return item

    return None


def article_to_row(symbol, article):
    time_published = parse_alpha_time(article.get("time_published"))

    if time_published is None:
        return None

    url = article.get("url")
    title = article.get("title")

    ticker_item = get_symbol_ticker_sentiment(article, symbol)

    article_key = make_article_key(
        symbol=symbol,
        url=url,
        time_published=time_published.isoformat(),
        title=title,
    )

    authors = article.get("authors")
    if isinstance(authors, list):
        authors_text = ", ".join(str(a) for a in authors)
    else:
        authors_text = authors

    return (
        article_key,
        symbol,
        time_published,

        title,
        url,
        article.get("source"),
        article.get("source_domain"),
        authors_text,
        article.get("summary"),
        article.get("banner_image"),
        article.get("category_within_source"),
        article.get("source_type"),

        to_float(article.get("overall_sentiment_score")),
        article.get("overall_sentiment_label"),

        to_float(ticker_item.get("relevance_score")) if ticker_item else None,
        to_float(ticker_item.get("ticker_sentiment_score")) if ticker_item else None,
        ticker_item.get("ticker_sentiment_label") if ticker_item else None,

        Json(article.get("topics", [])),
        Json(article.get("ticker_sentiment", [])),
        Json(article),
    )


def upsert_news_rows(conn, cursor, rows):
    if not rows:
        return 0

    schema = safe_identifier(TARGET_SCHEMA)
    table = safe_identifier(TARGET_TABLE)

    insert_statement = f"""
        INSERT INTO {schema}.{table} (
            article_key,
            symbol,
            time_published,

            title,
            url,
            source,
            source_domain,
            authors,
            summary,
            banner_image,
            category_within_source,
            source_type,

            overall_sentiment_score,
            overall_sentiment_label,

            ticker_relevance_score,
            ticker_sentiment_score,
            ticker_sentiment_label,

            topics,
            ticker_sentiment,
            raw_article
        )
        VALUES %s
        ON CONFLICT (article_key)
        DO UPDATE SET
            title = EXCLUDED.title,
            url = EXCLUDED.url,
            source = EXCLUDED.source,
            source_domain = EXCLUDED.source_domain,
            authors = EXCLUDED.authors,
            summary = EXCLUDED.summary,
            banner_image = EXCLUDED.banner_image,
            category_within_source = EXCLUDED.category_within_source,
            source_type = EXCLUDED.source_type,

            overall_sentiment_score = EXCLUDED.overall_sentiment_score,
            overall_sentiment_label = EXCLUDED.overall_sentiment_label,

            ticker_relevance_score = EXCLUDED.ticker_relevance_score,
            ticker_sentiment_score = EXCLUDED.ticker_sentiment_score,
            ticker_sentiment_label = EXCLUDED.ticker_sentiment_label,

            topics = EXCLUDED.topics,
            ticker_sentiment = EXCLUDED.ticker_sentiment,
            raw_article = EXCLUDED.raw_article,
            updated_at_utc = NOW();
    """

    try:
        execute_values(cursor, insert_statement, rows, page_size=1000)
        return len(rows)

    except Exception as exc:
        conn.rollback()
        print(f"Database upsert failed: {exc}", flush=True)
        return 0


# ============================================================
# Incremental helpers
# ============================================================

def fetch_max_time_published(cursor):
    """
    Gets one overall max time_published from FIN_IND.news_sentiment_data.
    Same style as daily/intraday max-date logic.
    """
    schema = safe_identifier(TARGET_SCHEMA)
    table = safe_identifier(TARGET_TABLE)

    print(f"Fetching MAX(time_published) from {schema}.{table}...", flush=True)

    query = f"""
        SELECT MAX(time_published) AS max_time_published
        FROM {schema}.{table};
    """

    cursor.execute(query)
    row = cursor.fetchone()

    max_time = row[0] if row and row[0] else None

    print(f"MAX(time_published) from {schema}.{table}: {max_time}", flush=True)

    if max_time is None:
        raise SystemExit(
            f"ERROR: MAX(time_published) from {schema}.{table} is None. "
            "Run the historical news sentiment load first."
        )

    return max_time


def fetch_and_store_news_incremental(conn, cursor, session, symbol, max_time_published):
    """
    Incremental load:
    - Uses one global MAX(time_published) from target table.
    - Calls NEWS_SENTIMENT with time_from=max_time_published.
    - Filters in Python and inserts only rows where time_published > max_time_published.
    - No TRUNCATE. No DELETE. No DROP.
    """
    time_from = format_alpha_time(max_time_published)

    data = call_news_sentiment_api(
        session=session,
        symbol=symbol,
        time_from=time_from,
        time_to=None
    )

    if not data:
        return 0, 0

    if "feed" not in data:
        message = (
            data.get("Note")
            or data.get("Information")
            or data.get("Error Message")
            or str(data)[:500]
        )
        print(f"{symbol}: no feed returned. Message: {message}", flush=True)
        return 0, 0

    feed = data.get("feed", []) or []
    rows = []
    api_articles_seen = 0

    for article in feed:
        row = article_to_row(symbol, article)
        if row is None:
            continue

        api_articles_seen += 1
        article_time = row[2]

        if article_time <= max_time_published:
            continue

        rows.append(row)

    upserted = upsert_news_rows(conn, cursor, rows)
    return upserted, api_articles_seen


# ============================================================
# Main process
# ============================================================

def main():
    started = perf_counter()
    print("Incremental news sentiment load started.", flush=True)

    conn = get_connection()
    cursor = conn.cursor()

    try:
        create_target_table_and_indexes(cursor)
        conn.commit()

        max_time_published = fetch_max_time_published(cursor)
        stock_symbols = fetch_all_stock_symbols_from_master(cursor)

        print("=" * 70, flush=True)
        print("Incremental news sentiment configuration:", flush=True)
        print(f"Stock master table: {STOCK_MASTER_SCHEMA}.{STOCK_MASTER_TABLE}", flush=True)
        print(f"Target table: {TARGET_SCHEMA}.{TARGET_TABLE}", flush=True)
        print(f"MAX time_published used: {max_time_published}", flush=True)
        print(f"Alpha Vantage time_from used: {format_alpha_time(max_time_published)}", flush=True)
        print(f"News limit per API call: {NEWS_LIMIT}", flush=True)
        print(f"Total symbols: {len(stock_symbols):,}", flush=True)
        print("No stock-type filters are applied. All non-empty tickers are included.", flush=True)
        print("Will insert only articles where time_published > max_time_published.", flush=True)
        print("No TRUNCATE. No DELETE. No DROP. Incremental upsert only.", flush=True)
        print("=" * 70, flush=True)

        session = requests.Session()

        total_upserted = 0
        total_api_articles_seen = 0
        total_no_new_data = 0

        for i in range(0, len(stock_symbols), BATCH_SIZE):
            batch_start = perf_counter()
            batch_symbols = stock_symbols[i:i + BATCH_SIZE]
            batch_number = (i // BATCH_SIZE) + 1

            print(f"\nStarting batch {batch_number} with {len(batch_symbols)} symbols", flush=True)

            for j, symbol in enumerate(batch_symbols, start=1):
                n, api_seen = fetch_and_store_news_incremental(
                    conn=conn,
                    cursor=cursor,
                    session=session,
                    symbol=symbol,
                    max_time_published=max_time_published
                )

                total_upserted += n
                total_api_articles_seen += api_seen

                if n == 0:
                    total_no_new_data += 1

                print(
                    f"{symbol}: batch {batch_number}.{j} "
                    f"max_time_published={max_time_published} "
                    f"api_articles_seen={api_seen} "
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
        print("Finished incremental news sentiment load.", flush=True)
        print(f"Total symbols processed: {len(stock_symbols):,}", flush=True)
        print(f"MAX time_published used: {max_time_published}", flush=True)
        print(f"Total API articles seen: {total_api_articles_seen:,}", flush=True)
        print(f"Total rows upserted: {total_upserted:,}", flush=True)
        print(f"Symbols with no new rows: {total_no_new_data:,}", flush=True)
        print(f"Target table: {TARGET_SCHEMA}.{TARGET_TABLE}", flush=True)
        print(f"Total elapsed time: {total_elapsed:.2f}s", flush=True)
        print("=" * 70, flush=True)

    finally:
        cursor.close()
        conn.close()
        print("Database connection closed.", flush=True)


if __name__ == "__main__":
    main()
