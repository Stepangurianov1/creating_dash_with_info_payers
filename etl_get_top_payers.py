import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy import text
import IP2Location
import ipaddress



def get_engine():
    db_config = {
        'host': '138.68.88.175',
        'port': 5432,
        'database': 'csd_bi',
        'user': 'datalens_utl',
        'password': 'mQnXQaHP6zkOaFdTLRVLx40gT4'
    }

    connection_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    return create_engine(connection_string, pool_pre_ping=True)

def get_engine_dwh():
    db_config = {
        'host': 'primarydwhcsd.aerxd.tech',
        'port': 6432,
        'database': 'postgres',
        'user': 'ste',
        'password': 'ILzAYQ72aEe9'
    }
    connection_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    return create_engine(connection_string, pool_pre_ping=True)

def update_db():
    start = (datetime.now() - timedelta(days=30)).date()

    sql = """
    SELECT i.payer_id
    FROM orders.invoice i
    WHERE i.created_at >= DATE :start_date
      AND i.status_id = 2
    GROUP BY i.payer_id
    HAVING COUNT(*) > 1
    """
    with get_engine().connect() as conn:
        data_top_payers = pd.read_sql(text(sql), conn, params={"start_date": start})
    print("Получили лучших пользователей")
    data_top_payers.to_sql(
        schema='cascade',
        name='top_payers',
        if_exists='replace',
        con=get_engine_dwh(),
        index=False
    )

update_db()





