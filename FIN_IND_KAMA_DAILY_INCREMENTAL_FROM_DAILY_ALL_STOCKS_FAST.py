import os, re, time, psycopg2, pandas as pd
from pathlib import Path
from time import perf_counter
from dotenv import load_dotenv
from psycopg2.extras import execute_values
load_dotenv(Path(__file__).with_name('.env'))
def need(n):
    v=os.getenv(n)
    if not v: raise SystemExit(f'Missing env var: {n}')
    return v
DB_HOST=need('DB_HOST_MAIN'); DB_PORT=os.getenv('DB_PORT_MAIN','5432')
DB_NAME=need('DB_NAME_MAIN'); DB_USER=need('DB_USER_MAIN'); DB_PASS=need('DB_PASS_MAIN')
DB_SSLMODE=os.getenv('DB_SSLMODE_MAIN','require').strip()
STOCK_MASTER_SCHEMA='FIN_IND'; STOCK_MASTER_TABLE='us_stock_master'
SOURCE_SCHEMA='FIN_IND'; SOURCE_TABLE='daily_adjusted_data'; PREFERRED_SOURCE_DATE_COLUMN='price_date'; FALLBACK_SOURCE_DATE_COLUMN='date'
TARGET_SCHEMA='FIN_IND'; TARGET_TABLE='kama_data'
INTERVAL='daily'; START_AFTER_TICKER=''; MAX_STOCK_SYMBOLS=0; BATCH_SIZE=100; BATCH_INTERVAL=0; CREATE_TARGET_IF_MISSING=False
SOURCE_PRICE_COLUMN="adjusted_close"
TIME_PERIOD=14
FAST_PERIOD=2
SLOW_PERIOD=30
SERIES_TYPE=SOURCE_PRICE_COLUMN
LOOKBACK_DAYS=TIME_PERIOD*10
def safe_identifier(n):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', n): raise ValueError(f'Invalid SQL identifier: {n}')
    return n
def conn_db():
    args=dict(dbname=DB_NAME,user=DB_USER,password=DB_PASS,host=DB_HOST,port=int(DB_PORT),connect_timeout=15)
    if DB_SSLMODE: args['sslmode']=DB_SSLMODE
    return psycopg2.connect(**args)
def safe_rollback(conn):
    try:
        if conn and conn.closed==0: conn.rollback()
    except Exception: pass
def reconnect(cur=None,conn=None):
    try:
        if cur: cur.close()
    except Exception: pass
    try:
        if conn and conn.closed==0: conn.close()
    except Exception: pass
    conn=conn_db(); return conn,conn.cursor()
def column_exists(cur,s,t,c):
    cur.execute('SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name=%s);',(s.lower(),t.lower(),c.lower()))
    return bool(cur.fetchone()[0])
def resolve_date_col(cur):
    if column_exists(cur,SOURCE_SCHEMA,SOURCE_TABLE,PREFERRED_SOURCE_DATE_COLUMN): return PREFERRED_SOURCE_DATE_COLUMN
    if column_exists(cur,SOURCE_SCHEMA,SOURCE_TABLE,FALLBACK_SOURCE_DATE_COLUMN): return FALLBACK_SOURCE_DATE_COLUMN
    raise SystemExit(f'No date column found in {SOURCE_SCHEMA}.{SOURCE_TABLE}')
def setup(cur):
    s=safe_identifier(TARGET_SCHEMA); t=safe_identifier(TARGET_TABLE)
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS {s};')
    cur.execute(f"CREATE TABLE IF NOT EXISTS {s}.{t} (id BIGSERIAL PRIMARY KEY, symbol VARCHAR(30) NOT NULL, interval VARCHAR(20) NOT NULL, time_period INT NOT NULL, fast_period INT NOT NULL, slow_period INT NOT NULL, series_type VARCHAR(30) NOT NULL, date DATE NOT NULL, kama NUMERIC NOT NULL, inserted_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW());")
    cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS ux_kama_data_symbol_interval_periods_series_date_live ON {s}.{t} (symbol, interval, time_period, fast_period, slow_period, series_type, date);")
def get_symbols(cur):
    s=safe_identifier(STOCK_MASTER_SCHEMA); t=safe_identifier(STOCK_MASTER_TABLE)
    where=['ticker IS NOT NULL', "TRIM(ticker) <> ''"]; params=[]
    if START_AFTER_TICKER:
        where.append('TRIM(ticker)>%s'); params.append(START_AFTER_TICKER.strip())
    limit=f'LIMIT {int(MAX_STOCK_SYMBOLS)}' if MAX_STOCK_SYMBOLS and MAX_STOCK_SYMBOLS>0 else ''
    cur.execute(f"SELECT DISTINCT TRIM(ticker) AS ticker FROM {s}.{t}  ORDER BY TRIM(ticker) {limit};",params)
    return [r[0] for r in cur.fetchall() if r[0]]
def max_date(cur):
    s=safe_identifier(TARGET_SCHEMA); t=safe_identifier(TARGET_TABLE)
    cur.execute(f"SELECT MAX(date) FROM {s}.{t} ;",(INTERVAL,TIME_PERIOD,FAST_PERIOD,SLOW_PERIOD,SERIES_TYPE))
    m=cur.fetchone()[0]
    if m is None: raise SystemExit('No existing KAMA data found. Run historical load first.')
    return m
def fetch_source(conn,symbols,m,date_col):
    if not symbols: return pd.DataFrame(columns=['symbol', 'date', 'price'])
    ss=safe_identifier(SOURCE_SCHEMA); st=safe_identifier(SOURCE_TABLE); dc=safe_identifier(date_col)
    q=f"SELECT symbol, {dc}::date AS date, adjusted_close::numeric AS price FROM {ss}.{st} WHERE symbol=ANY(%s) AND {dc}::date >= (%s::date - %s::int) AND {dc} IS NOT NULL AND adjusted_close IS NOT NULL ORDER BY symbol, {dc} ASC;"
    with conn.cursor() as c:
        c.execute(q,(symbols,m,LOOKBACK_DAYS)); rows=c.fetchall()
    return pd.DataFrame(rows,columns=['symbol', 'date', 'price'])
def calc_one(df):
    df=df.copy().sort_values('date').reset_index(drop=True); price=df['price'].astype(float); kama=[None]*len(df)
    if len(df)<=TIME_PERIOD: return pd.DataFrame(columns=['symbol','date','kama'])
    fast_sc=2/(FAST_PERIOD+1); slow_sc=2/(SLOW_PERIOD+1); kama[TIME_PERIOD-1]=price.iloc[:TIME_PERIOD].mean()
    for i in range(TIME_PERIOD,len(df)):
        change=abs(price.iloc[i]-price.iloc[i-TIME_PERIOD]); volatility=price.diff().abs().iloc[i-TIME_PERIOD+1:i+1].sum()
        er=0 if volatility==0 else change/volatility; sc=(er*(fast_sc-slow_sc)+slow_sc)**2
        kama[i]=kama[i-1]+sc*(price.iloc[i]-kama[i-1])
    df['kama']=kama
    return df.dropna(subset=['kama'])[['symbol','date','kama']]
def build_rows(source_df,m):
    rows=[]
    for symbol,g in source_df.groupby('symbol'):
        for _,r in calc_one(g).iterrows():
            if r['date']<=m: continue
            rows.append((symbol,INTERVAL,TIME_PERIOD,FAST_PERIOD,SLOW_PERIOD,SERIES_TYPE,r['date'],round(float(r['kama']),4)))
    return rows
def upsert(conn,cur,rows):
    if not rows: return 0
    s=safe_identifier(TARGET_SCHEMA); t=safe_identifier(TARGET_TABLE)
    sql=f"INSERT INTO {s}.{t} (symbol, interval, time_period, fast_period, slow_period, series_type, date, kama) VALUES %s ON CONFLICT (symbol, interval, time_period, fast_period, slow_period, series_type, date) DO UPDATE SET kama=EXCLUDED.kama, updated_at_utc=NOW();"
    try:
        execute_values(cur,sql,rows,page_size=1000); return len(rows)
    except Exception as e:
        safe_rollback(conn); print(f'Database upsert failed: {e}',flush=True); return -1
def main():
    started=perf_counter(); conn=conn_db(); cur=conn.cursor()
    try:
        if CREATE_TARGET_IF_MISSING: setup(cur); conn.commit()
        date_col=resolve_date_col(cur); m=max_date(cur); symbols=get_symbols(cur)
        print(f'Incremental DAILY KAMA. max_date={{m}} symbols={{len(symbols):,}}',flush=True)
        total=0
        for i in range(0,len(symbols),BATCH_SIZE):
            batch=symbols[i:i+BATCH_SIZE]; t0=perf_counter(); df=fetch_source(conn,batch,m,date_col); rows=build_rows(df,m); n=upsert(conn,cur,rows)
            if n==-1:
                conn,cur=reconnect(cur,conn); n=upsert(conn,cur,rows)
            if n==-1: n=0
            else: conn.commit()
            total+=n; print(f'Batch {{i//BATCH_SIZE+1}} committed. symbols={{len(batch)}} source_rows={{len(df):,}} new_rows={{n:,}} elapsed={{perf_counter()-t0:.2f}}s',flush=True)
            if BATCH_INTERVAL>0: time.sleep(BATCH_INTERVAL)
        print(f'Finished KAMA incremental load. total_new_rows={{total:,}} elapsed={{perf_counter()-started:.2f}}s',flush=True)
    finally:
        cur.close(); conn.close()
if __name__=='__main__': main()
