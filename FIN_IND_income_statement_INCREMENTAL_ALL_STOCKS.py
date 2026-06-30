import os
import re
import time
from pathlib import Path
from time import perf_counter
from datetime import datetime

print("Income statement script started...", flush=True)

import requests
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

print("Imports completed.", flush=True)

ENV_PATH = Path(__file__).with_name(".env")
print(f"Loading .env from: {ENV_PATH}", flush=True)
load_dotenv(ENV_PATH)
print(".env loaded.", flush=True)


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


# ============================================================
# Fixed configuration in Python
# ============================================================

STOCK_MASTER_SCHEMA = "FIN_IND"
STOCK_MASTER_TABLE = "us_stock_master"

TARGET_SCHEMA = "dividend"
ANNUAL_TABLE = "income_statements_annual"
QUARTERLY_TABLE = "income_statements_quarterly"

# Include every non-empty ticker from us_stock_master.
START_AFTER_TICKER = ""
MAX_STOCK_SYMBOLS = 0

# Current 75 API/min safe settings.
# For 1200 API/min later, you can use INTERVAL_PER_API = 0.05 after testing.
BATCH_SIZE = 50
INTERVAL_PER_API = 1
BATCH_INTERVAL = 1

DB_CONNECT_TIMEOUT_SECONDS = 15
API_TIMEOUT_SECONDS = 60


def safe_identifier(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid SQL identifier: {name}")
    return name


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


def parse_numeric(value):
    if value is None:
        return None
    if isinstance(value, str) and value.strip() in ("", "None", "none", "null", "NULL"):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_date(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def create_target_tables(cursor):
    schema = safe_identifier(TARGET_SCHEMA)
    annual_table = safe_identifier(ANNUAL_TABLE)
    quarterly_table = safe_identifier(QUARTERLY_TABLE)

    print(f"Creating/checking schema {schema}...", flush=True)
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    create_table_template = """
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            id BIGSERIAL PRIMARY KEY,
            symbol VARCHAR(30) NOT NULL,
            fiscal_date_ending DATE NOT NULL,
            reported_currency VARCHAR(10),
            gross_profit NUMERIC,
            total_revenue NUMERIC,
            cost_of_revenue NUMERIC,
            costof_goods_and_services_sold NUMERIC,
            operating_income NUMERIC,
            selling_general_and_administrative NUMERIC,
            research_and_development NUMERIC,
            operating_expenses NUMERIC,
            investment_income_net NUMERIC,
            net_interest_income NUMERIC,
            interest_income NUMERIC,
            interest_expense NUMERIC,
            non_interest_income NUMERIC,
            other_non_operating_income NUMERIC,
            depreciation NUMERIC,
            depreciation_and_amortization NUMERIC,
            income_before_tax NUMERIC,
            income_tax_expense NUMERIC,
            interest_and_debt_expense NUMERIC,
            net_income_from_continuing_operations NUMERIC,
            comprehensive_income_net_of_tax NUMERIC,
            ebit NUMERIC,
            ebitda NUMERIC,
            net_income NUMERIC,
            inserted_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """

    print(f"Creating/checking table {schema}.{annual_table}...", flush=True)
    cursor.execute(create_table_template.format(schema=schema, table=annual_table))

    print(f"Creating/checking table {schema}.{quarterly_table}...", flush=True)
    cursor.execute(create_table_template.format(schema=schema, table=quarterly_table))

    for table in (annual_table, quarterly_table):
        cursor.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_{table}_symbol_fiscal_date
            ON {schema}.{table} (symbol, fiscal_date_ending);
        """)
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{table}_symbol
            ON {schema}.{table} (symbol);
        """)
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{table}_fiscal_date
            ON {schema}.{table} (fiscal_date_ending);
        """)


def fetch_all_stock_symbols_from_master(cursor):
    schema = safe_identifier(STOCK_MASTER_SCHEMA)
    table = safe_identifier(STOCK_MASTER_TABLE)

    print(f"Fetching ALL symbols from {schema}.{table} with no stock-type filters...", flush=True)

    where_conditions = [
        "ticker IS NOT NULL",
        "TRIM(ticker) <> ''"
    ]
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
    print(f"Fetched {len(symbols):,} symbols from stock master.", flush=True)
    return symbols


def row_from_report(symbol, report):
    fiscal_date = parse_date(report.get("fiscalDateEnding"))
    if fiscal_date is None:
        return None

    return (
        symbol,
        fiscal_date,
        report.get("reportedCurrency"),
        parse_numeric(report.get("grossProfit")),
        parse_numeric(report.get("totalRevenue")),
        parse_numeric(report.get("costOfRevenue")),
        parse_numeric(report.get("costofGoodsAndServicesSold")),
        parse_numeric(report.get("operatingIncome")),
        parse_numeric(report.get("sellingGeneralAndAdministrative")),
        parse_numeric(report.get("researchAndDevelopment")),
        parse_numeric(report.get("operatingExpenses")),
        parse_numeric(report.get("investmentIncomeNet")),
        parse_numeric(report.get("netInterestIncome")),
        parse_numeric(report.get("interestIncome")),
        parse_numeric(report.get("interestExpense")),
        parse_numeric(report.get("nonInterestIncome")),
        parse_numeric(report.get("otherNonOperatingIncome")),
        parse_numeric(report.get("depreciation")),
        parse_numeric(report.get("depreciationAndAmortization")),
        parse_numeric(report.get("incomeBeforeTax")),
        parse_numeric(report.get("incomeTaxExpense")),
        parse_numeric(report.get("interestAndDebtExpense")),
        parse_numeric(report.get("netIncomeFromContinuingOperations")),
        parse_numeric(report.get("comprehensiveIncomeNetOfTax")),
        parse_numeric(report.get("ebit")),
        parse_numeric(report.get("ebitda")),
        parse_numeric(report.get("netIncome")),
    )


def call_income_statement_api(session, symbol):
    url = (
        "https://www.alphavantage.co/query"
        f"?function=INCOME_STATEMENT"
        f"&symbol={symbol}"
        f"&apikey={API_KEY}"
    )

    try:
        response = session.get(url, timeout=API_TIMEOUT_SECONDS)
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        print(f"{symbol}: API request failed: {exc}", flush=True)
        return None

    if any(k in data for k in ("Note", "Information", "Error Message")):
        message = data.get("Note") or data.get("Information") or data.get("Error Message")
        print(f"{symbol}: API message: {message}", flush=True)
        return None

    return data


def parse_income_statement_rows(symbol, data):
    annual_rows = []
    quarterly_rows = []

    for report in data.get("annualReports", []) or []:
        row = row_from_report(symbol, report)
        if row:
            annual_rows.append(row)

    for report in data.get("quarterlyReports", []) or []:
        row = row_from_report(symbol, report)
        if row:
            quarterly_rows.append(row)

    return annual_rows, quarterly_rows


UPSERT_SQL_TEMPLATE = """
    INSERT INTO {schema}.{table} (
        symbol,
        fiscal_date_ending,
        reported_currency,
        gross_profit,
        total_revenue,
        cost_of_revenue,
        costof_goods_and_services_sold,
        operating_income,
        selling_general_and_administrative,
        research_and_development,
        operating_expenses,
        investment_income_net,
        net_interest_income,
        interest_income,
        interest_expense,
        non_interest_income,
        other_non_operating_income,
        depreciation,
        depreciation_and_amortization,
        income_before_tax,
        income_tax_expense,
        interest_and_debt_expense,
        net_income_from_continuing_operations,
        comprehensive_income_net_of_tax,
        ebit,
        ebitda,
        net_income
    )
    VALUES %s
    ON CONFLICT (symbol, fiscal_date_ending)
    DO UPDATE SET
        reported_currency = EXCLUDED.reported_currency,
        gross_profit = EXCLUDED.gross_profit,
        total_revenue = EXCLUDED.total_revenue,
        cost_of_revenue = EXCLUDED.cost_of_revenue,
        costof_goods_and_services_sold = EXCLUDED.costof_goods_and_services_sold,
        operating_income = EXCLUDED.operating_income,
        selling_general_and_administrative = EXCLUDED.selling_general_and_administrative,
        research_and_development = EXCLUDED.research_and_development,
        operating_expenses = EXCLUDED.operating_expenses,
        investment_income_net = EXCLUDED.investment_income_net,
        net_interest_income = EXCLUDED.net_interest_income,
        interest_income = EXCLUDED.interest_income,
        interest_expense = EXCLUDED.interest_expense,
        non_interest_income = EXCLUDED.non_interest_income,
        other_non_operating_income = EXCLUDED.other_non_operating_income,
        depreciation = EXCLUDED.depreciation,
        depreciation_and_amortization = EXCLUDED.depreciation_and_amortization,
        income_before_tax = EXCLUDED.income_before_tax,
        income_tax_expense = EXCLUDED.income_tax_expense,
        interest_and_debt_expense = EXCLUDED.interest_and_debt_expense,
        net_income_from_continuing_operations = EXCLUDED.net_income_from_continuing_operations,
        comprehensive_income_net_of_tax = EXCLUDED.comprehensive_income_net_of_tax,
        ebit = EXCLUDED.ebit,
        ebitda = EXCLUDED.ebitda,
        net_income = EXCLUDED.net_income,
        updated_at_utc = NOW();
"""


def upsert_rows(conn, cursor, table_name, rows):
    if not rows:
        return 0

    schema = safe_identifier(TARGET_SCHEMA)
    table = safe_identifier(table_name)
    sql = UPSERT_SQL_TEMPLATE.format(schema=schema, table=table)

    try:
        execute_values(cursor, sql, rows, page_size=1000)
        return len(rows)
    except Exception as exc:
        conn.rollback()
        print(f"Database upsert failed for {schema}.{table}: {exc}", flush=True)
        return 0


def fetch_max_fiscal_date(cursor, table_name):
    schema = safe_identifier(TARGET_SCHEMA)
    table = safe_identifier(table_name)

    print(f"Fetching MAX(fiscal_date_ending) from {schema}.{table}...", flush=True)

    query = f"""
        SELECT MAX(fiscal_date_ending)
        FROM {schema}.{table};
    """

    cursor.execute(query)
    row = cursor.fetchone()
    max_date = row[0] if row and row[0] else None

    print(f"MAX(fiscal_date_ending) from {schema}.{table}: {max_date}", flush=True)

    if max_date is None:
        raise SystemExit(
            f"ERROR: MAX(fiscal_date_ending) from {schema}.{table} is None. "
            "Run the historical income statement load first."
        )

    return max_date


def fetch_and_store_symbol_incremental(conn, cursor, session, symbol, max_annual_fiscal_date, max_quarterly_fiscal_date):
    data = call_income_statement_api(session, symbol)
    if not data:
        return 0, 0, 0, 0

    annual_rows, quarterly_rows = parse_income_statement_rows(symbol, data)

    annual_seen = len(annual_rows)
    quarterly_seen = len(quarterly_rows)

    new_annual_rows = [
        row for row in annual_rows
        if row[1] > max_annual_fiscal_date
    ]

    new_quarterly_rows = [
        row for row in quarterly_rows
        if row[1] > max_quarterly_fiscal_date
    ]

    annual_count = upsert_rows(conn, cursor, ANNUAL_TABLE, new_annual_rows)
    quarterly_count = upsert_rows(conn, cursor, QUARTERLY_TABLE, new_quarterly_rows)

    return annual_count, quarterly_count, annual_seen, quarterly_seen


def main():
    started = perf_counter()
    print("Incremental income statement load started.", flush=True)

    conn = get_connection()
    cursor = conn.cursor()

    try:
        create_target_tables(cursor)
        conn.commit()

        max_annual_fiscal_date = fetch_max_fiscal_date(cursor, ANNUAL_TABLE)
        max_quarterly_fiscal_date = fetch_max_fiscal_date(cursor, QUARTERLY_TABLE)
        stock_symbols = fetch_all_stock_symbols_from_master(cursor)

        print("=" * 70, flush=True)
        print("Incremental income statement configuration:", flush=True)
        print(f"Stock master table: {STOCK_MASTER_SCHEMA}.{STOCK_MASTER_TABLE}", flush=True)
        print(f"Annual target table: {TARGET_SCHEMA}.{ANNUAL_TABLE}", flush=True)
        print(f"Quarterly target table: {TARGET_SCHEMA}.{QUARTERLY_TABLE}", flush=True)
        print(f"MAX annual fiscal_date_ending used: {max_annual_fiscal_date}", flush=True)
        print(f"MAX quarterly fiscal_date_ending used: {max_quarterly_fiscal_date}", flush=True)
        print(f"Total symbols: {len(stock_symbols):,}", flush=True)
        print("All non-empty tickers are included.", flush=True)
        print("Will upsert only rows after each table's max fiscal date.", flush=True)
        print("No TRUNCATE. No DELETE. No DROP. Incremental upsert only.", flush=True)
        print("=" * 70, flush=True)

        session = requests.Session()

        total_annual = 0
        total_quarterly = 0
        total_no_new_data = 0
        total_annual_seen = 0
        total_quarterly_seen = 0

        for i in range(0, len(stock_symbols), BATCH_SIZE):
            batch_start = perf_counter()
            batch_symbols = stock_symbols[i:i + BATCH_SIZE]
            batch_number = (i // BATCH_SIZE) + 1

            print(f"\nStarting batch {batch_number} with {len(batch_symbols)} symbols", flush=True)

            for j, symbol in enumerate(batch_symbols, start=1):
                annual_count, quarterly_count, annual_seen, quarterly_seen = fetch_and_store_symbol_incremental(
                    conn=conn,
                    cursor=cursor,
                    session=session,
                    symbol=symbol,
                    max_annual_fiscal_date=max_annual_fiscal_date,
                    max_quarterly_fiscal_date=max_quarterly_fiscal_date
                )

                total_annual += annual_count
                total_quarterly += quarterly_count
                total_annual_seen += annual_seen
                total_quarterly_seen += quarterly_seen

                if annual_count == 0 and quarterly_count == 0:
                    total_no_new_data += 1

                print(
                    f"{symbol}: batch {batch_number}.{j} "
                    f"annual_seen={annual_seen} annual_new_upserted={annual_count} "
                    f"quarterly_seen={quarterly_seen} quarterly_new_upserted={quarterly_count}",
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
        print("Finished incremental income statement load.", flush=True)
        print(f"Total symbols processed: {len(stock_symbols):,}", flush=True)
        print(f"MAX annual fiscal date used: {max_annual_fiscal_date}", flush=True)
        print(f"MAX quarterly fiscal date used: {max_quarterly_fiscal_date}", flush=True)
        print(f"Total annual rows seen: {total_annual_seen:,}", flush=True)
        print(f"Total quarterly rows seen: {total_quarterly_seen:,}", flush=True)
        print(f"Total new annual rows upserted: {total_annual:,}", flush=True)
        print(f"Total new quarterly rows upserted: {total_quarterly:,}", flush=True)
        print(f"Symbols with no new rows: {total_no_new_data:,}", flush=True)
        print(f"Annual target table: {TARGET_SCHEMA}.{ANNUAL_TABLE}", flush=True)
        print(f"Quarterly target table: {TARGET_SCHEMA}.{QUARTERLY_TABLE}", flush=True)
        print(f"Total elapsed time: {total_elapsed:.2f}s", flush=True)
        print("=" * 70, flush=True)

    finally:
        cursor.close()
        conn.close()
        print("Database connection closed.", flush=True)


if __name__ == "__main__":
    main()
