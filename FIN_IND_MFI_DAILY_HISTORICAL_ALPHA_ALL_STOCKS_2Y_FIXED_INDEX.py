import os, re, time, requests, psycopg2
from pathlib import Path
from time import perf_counter
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv(Path(__file__).with_name('.env'))
def need(n):
    v=os.getenv(n)
    if not v: raise SystemExit(f'Missing env var: {n}')
    return v
DB_HOST=need('DB_HOST_MAIN'); DB_PORT=os.getenv('DB_PORT_MAIN','5432')
DB_NAME=need('DB_NAME_MAIN'); DB_USER=need('DB_USER_MAIN'); DB_PASS=need('DB_PASS_MAIN')
DB_SSLMODE=os.getenv('DB_SSLMODE_MAIN','require').strip(); API_KEY=need('API_KEY_MAIN')
STOCK_MASTER_SCHEMA='FIN_IND'; STOCK_MASTER_TABLE='us_stock_master'
TARGET_SCHEMA='FIN_IND'; TARGET_TABLE='mfi_data'
INTERVAL='daily'; HISTORY_YEARS=2; HISTORY_START_DATE=date.today()-timedelta(days=365*HISTORY_YEARS)
START_AFTER_TICKER=''; MAX_STOCK_SYMBOLS=0
BATCH_SIZE=1000; INTERVAL_PER_API=0.06; BATCH_INTERVAL=0
CREATE_TARGET_IF_MISSING=True; DB_CONNECT_TIMEOUT_SECONDS=15; API_TIMEOUT_SECONDS=60
TIME_PERIOD=14

def safe_identifier(n):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', n): raise ValueError(f'Invalid SQL identifier: {n}')
    return n

def conn_db():
    args=dict(dbname=DB_NAME,user=DB_USER,password=DB_PASS,host=DB_HOST,port=int(DB_PORT),connect_timeout=DB_CONNECT_TIMEOUT_SECONDS)
    if DB_SSLMODE: args['sslmode']=DB_SSLMODE
    return psycopg2.connect(**args)

def safe_rollback(conn):
    try:
        if conn and conn.closed==0: conn.rollback()
    except Exception: pass

def reconnect(cur=None, conn=None):
    try:
        if cur: cur.close()
    except Exception: pass
    try:
        if conn and conn.closed==0: conn.close()
    except Exception: pass
    conn=conn_db(); return conn, conn.cursor()

def parse_date(s):
    try: return datetime.strptime(s,'%Y-%m-%d').date()
    except Exception: return None

def parse_float(v):
    try: return None if v is None or v=='' else float(v)
    except Exception: return None

def setup(cur):
    s=safe_identifier(TARGET_SCHEMA); t=safe_identifier(TARGET_TABLE)
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS {s};')
    cur.execute(f"CREATE TABLE IF NOT EXISTS {s}.{t} (id BIGSERIAL PRIMARY KEY, symbol VARCHAR(30) NOT NULL, interval VARCHAR(20) NOT NULL, time_period INT NOT NULL, date DATE NOT NULL, mfi NUMERIC NOT NULL, inserted_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW());")
    cur.execute(f"SELECT COUNT(*) FROM (SELECT symbol, interval, time_period, date FROM {s}.{t} GROUP BY symbol, interval, time_period, date HAVING COUNT(*)>1) d;")
    if cur.fetchone()[0]:
        cur.execute(f"DELETE FROM {s}.{t} a USING {s}.{t} b WHERE a.id>b.id AND a.symbol=b.symbol AND a.interval=b.interval AND a.time_period=b.time_period AND a.date=b.date;")
    cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS ux_mfi_data_symbol_interval_period_date_live ON {s}.{t} (symbol, interval, time_period, date);")
    for col in ['symbol','date']:
        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{t}_{col} ON {s}.{t} ({col});')

def get_symbols(cur):
    s=safe_identifier(STOCK_MASTER_SCHEMA); t=safe_identifier(STOCK_MASTER_TABLE)
    where=['ticker IS NOT NULL', "TRIM(ticker) <> ''"]; params=[]
    if START_AFTER_TICKER:
        where.append('UPPER(TRIM(ticker)) > %s'); params.append(START_AFTER_TICKER.upper().strip())
    limit=f'LIMIT {int(MAX_STOCK_SYMBOLS)}' if MAX_STOCK_SYMBOLS and MAX_STOCK_SYMBOLS>0 else ''
    cur.execute(f"SELECT DISTINCT UPPER(TRIM(ticker)) AS ticker FROM {s}.{t} WHERE {' AND '.join(where)} ORDER BY ticker {limit};", params)
    return [r[0] for r in cur.fetchall() if r[0]]

def fetch_indicator(session, symbol):
    url=("https://www.alphavantage.co/query" + f"?function=MFI&symbol={symbol}&interval={INTERVAL}&time_period={TIME_PERIOD}&apikey={API_KEY}")
    try:
        r=session.get(url,timeout=API_TIMEOUT_SECONDS); r.raise_for_status(); data=r.json()
    except Exception as e:
        print(f'{symbol}: API request failed: {e}', flush=True); return []
    ta=data.get('Technical Analysis: MFI', {{}})
    if not ta:
        msg=data.get('Note') or data.get('Information') or data.get('Error Message') or str(data)[:300]
        print(f'{symbol}: no MFI data. Message: {msg}', flush=True); return []
    rows=[]
    for ds,row in ta.items():
        d=parse_date(ds)
        if d is None or d < HISTORY_START_DATE: continue
        val=parse_float(item.get("MFI"))
        if val is None: continue
        rows.append((symbol, INTERVAL, TIME_PERIOD, d, round(val,4)))
    return rows

def upsert(conn,cur,rows):
    if not rows: return 0
    s=safe_identifier(TARGET_SCHEMA); t=safe_identifier(TARGET_TABLE)
    sql=f"INSERT INTO {s}.{t} (symbol, interval, time_period, date, mfi) VALUES %s ON CONFLICT (symbol, interval, time_period, date) DO UPDATE SET mfi=EXCLUDED.mfi, updated_at_utc=NOW();"
    try:
        execute_values(cur,sql,rows,page_size=1000); return len(rows)
    except Exception as e:
        safe_rollback(conn); print(f'Database upsert failed: {e}', flush=True); return -1

def main():
    started=perf_counter(); conn=conn_db(); cur=conn.cursor()
    try:
        if CREATE_TARGET_IF_MISSING:
            setup(cur); conn.commit()
        symbols=get_symbols(cur)
        print(f'Historical DAILY MFI Alpha Vantage 2-year load. Start date={{HISTORY_START_DATE}} symbols={{len(symbols):,}}', flush=True)
        session=requests.Session(); total=0
        for i in range(0,len(symbols),BATCH_SIZE):
            batch=symbols[i:i+BATCH_SIZE]; bn=i//BATCH_SIZE+1; t0=perf_counter()
            for j,sym in enumerate(batch,1):
                n=upsert(conn,cur,fetch_indicator(session,sym))
                if n==-1:
                    conn,cur=reconnect(cur,conn); n=upsert(conn,cur,fetch_indicator(session,sym))
                if n==-1: n=0
                total+=n; print(f'{sym}: batch {bn}.{j} mfi_rows_upserted={{n}}', flush=True)
                if INTERVAL_PER_API>0: time.sleep(INTERVAL_PER_API)
            conn.commit(); print(f'Batch {bn} committed. elapsed={{perf_counter()-t0:.2f}}s', flush=True)
            if BATCH_INTERVAL>0: time.sleep(BATCH_INTERVAL)
        print(f'Finished MFI historical load. Total rows={{total:,}} elapsed={{perf_counter()-started:.2f}}s', flush=True)
    finally:
        cur.close(); conn.close()
if __name__=='__main__': main()
