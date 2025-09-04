import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
import time
import IP2Location
import ipaddress


# Shared engines with keepalive/recycle
SRC_DB_CONFIG = {
    'host': '138.68.88.175',
    'port': 5432,
    'database': 'csd_bi',
    'user': 'datalens_utl',
    'password': 'mQnXQaHP6zkOaFdTLRVLx40gT4'
}
DWH_DB_CONFIG = {
    'host': 'primarydwhcsd.aerxd.tech',
    'port': 6432,
    'database': 'postgres',
    'user': 'ste',
    'password': 'ILzAYQ72aEe9'
}

def _make_engine(db, include_options: bool = True):
    conn = f"postgresql+psycopg2://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"
    connect_args = {
        "connect_timeout": 10,
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    }
    if include_options:
        connect_args["options"] = "-c statement_timeout=600000"
    return create_engine(
        conn,
        pool_pre_ping=True,
        pool_recycle=180,
        pool_size=5,
        max_overflow=2,
        connect_args=connect_args,
    )

ENGINE_SRC = _make_engine(SRC_DB_CONFIG, include_options=True)
ENGINE_DWH = _make_engine(DWH_DB_CONFIG, include_options=False)


def read_sql_retry(sql, params=None, retries=3):
    for attempt in range(retries):
        try:
            with ENGINE_SRC.connect() as conn:
                return pd.read_sql(text(sql), conn, params=params)
        except OperationalError as e:
            if attempt == retries - 1:
                raise
            time.sleep(2 * (attempt + 1))


def update_db():
    start_general = (datetime.now() - timedelta(days=30)).date()
    start_last_week = (datetime.now() - timedelta(days=7)).date()
    sql = """
        SELECT i.payer_id
        FROM orders.invoice i
        WHERE (i.created_at >= DATE :start_general
        AND i.status_id = 2) or i.created_at >= DATE :start_last_week
        GROUP BY i.payer_id
        HAVING COUNT(*) >= 1
    """
    data_top_payers = read_sql_retry(sql, params={"start_general": start_general, "start_last_week": start_last_week})
    print("Получили лучших пользователей")
    with ENGINE_DWH.begin() as conn:
        conn.execute(text("TRUNCATE TABLE cascade.top_payers"))
    with ENGINE_DWH.begin() as conn:
        data_top_payers.to_sql(
            schema='cascade',
            name='top_payers',
            if_exists='append',
            con=conn,
            index=False
        )


update_db()





