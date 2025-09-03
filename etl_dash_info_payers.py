import pandas as pd
from datetime import datetime, timedelta
import warnings
from sqlalchemy import create_engine
import time
import calendar
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
warnings.filterwarnings('ignore')

SRC_DB_CONFIG = {
    'host': '138.68.88.175',
    'port': 5432,
    'database': 'csd_bi',
    'user': 'datalens_utl',
    'password': 'mQnXQaHP6zkOaFdTLRVLx40gT4'
}
TICKETS_DB_CONFIG = {
    'host': '138.68.88.175',
    'port': 5416,
    'database': 'ticket_replica',
    'user': 'klaksik77',
    'password': '6g3u0k13GhPhC2fvvPO'
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
ENGINE_TICKETS = _make_engine(TICKETS_DB_CONFIG, include_options=True)
ENGINE_DWH = _make_engine(DWH_DB_CONFIG, include_options=False)


def read_sql_retry_engine(engine, sql, params=None, retries: int = 3):
    for attempt in range(retries):
        try:
            with engine.connect() as conn:
                query = text(sql) if isinstance(sql, str) else sql
                return pd.read_sql(query, conn, params=params)
        except OperationalError as e:
            if attempt == retries - 1:
                raise
            time.sleep(2 * (attempt + 1))


def log_sql_failure(sql: str, err: Exception, chunk_idx: int, window_start: str, window_end: str, limit: int = 10000):
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    fname = f"failed_sql_{ts}_chunk{chunk_idx}_{window_start[:10]}_{window_end[:10]}.log"
    try:
        with open(fname, 'w') as f:
            f.write("ERROR:\n")
            f.write(str(err))
            f.write("\n\nSQL (first N chars):\n")
            f.write(sql[:limit])
        print(f"Saved failing SQL to: {fname}")
    except Exception:
        pass


def safe_parse_date(date_str):
    """Пытается создать дату, если день превышает допустимое — сдвигает на последний день месяца."""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        try:
            year, month, _ = map(int, date_str.split('-'))
            last_day = calendar.monthrange(year, month)[1]
            return datetime(year, month, last_day)
        except Exception as e:
            print(f"Ошибка при корректировке даты: {e}")
            raise

def get_query_tickets(start_date: str, end_date: str):
    return f"""
        WITH tickets_by_order AS (
        SELECT DISTINCT ON ((ticket_info->>'orderID')::bigint)
                (ticket_info->>'orderID')::bigint AS order_id,
                (ticket_info->>'appealDeclineReasonID') IS NOT NULL AS has_decline_reason,
                created_at
        FROM public.ticket
        WHERE ticket_info->>'orderID' IS NOT NULL
            AND ticket_info->>'orderID' <> '0'
        ORDER BY (ticket_info->>'orderID')::bigint, created_at DESC
        )
        SELECT * FROM tickets_by_order
        where created_at >= '{start_date}' and created_at <= '{end_date}'
    """

def create_query(start_date: str, end_date: str, payer_values: str):
    return f"""
            WITH payin AS (
                SELECT
                    oi.payer_id::text                       AS payer_id,
                    cl.system_name                          AS currency,
                    c.name                                  AS client_name,
                    oi.id                                   AS order_id,
                    oi.created_at                           AS created_at,
                    (oi.engine_id IS NOT NULL)::int         AS banking_details_issued,
                    (oi.status_id = 2)                      AS is_success,   
                    'payin'                                 AS order_type,
                    oi.amount                               AS amount
                FROM orders.invoice oi
                JOIN lists.currency_list cl ON cl.id = oi.currency_id
                JOIN clients.client c       ON c.id = oi.client_id
                JOIN unnest(ARRAY{payer_values}::text[]) AS tp(payer_id) ON tp.payer_id = oi.payer_id::text
                WHERE oi.created_at >= '{start_date}' and oi.created_at <= '{end_date}'
            ),
            payout AS (
                SELECT
                    (w.extra->'payerInfo'->>'userID')::text AS payer_id,
                    cl.system_name                          AS currency,
                    c.name                                  AS client_name,
                    w.id                                    AS order_id,
                    w.created_at                            AS created_at,
                    (w.engine_ids IS NOT NULL)::int         AS banking_details_issued,
                    (w.status_id = 4)                       AS is_success,    
                    'payout'                                AS order_type,
                    w.amount                                AS amount
                FROM orders.withdraw w
                JOIN lists.currency_list cl ON cl.id = w.currency_id
                JOIN clients.client c       ON c.id = w.client_id
                JOIN unnest(ARRAY{payer_values}::text[]) AS tp(payer_id) ON tp.payer_id = (w.extra->'payerInfo'->>'userID')::text
                WHERE (w.extra->'payerInfo'->>'userID') IS NOT NULL
                AND w.created_at >= '{start_date}' and w.created_at <= '{end_date}'
            ),
            u AS (
                SELECT * FROM payin
                UNION ALL
                SELECT * FROM payout
            )
            SELECT
                payer_id,
                currency,
                client_name,
                order_type,
                COUNT(order_id)                                    AS total_orders,
                SUM((is_success)::int)                             AS success_orders,
                SUM(CASE WHEN is_success THEN amount ELSE 0 END)   AS amount_success,
                NULLIF(SUM(banking_details_issued), 0)             AS banking_details_issued_count,
                MAX(created_at)                                    AS last_order_date,
                MIN(created_at)                                    AS first_order_date
            FROM u
            GROUP BY payer_id, currency, client_name, order_type
            ORDER BY success_orders DESC;
            """

def get_payers_by_tickets(orders_ids):
    return f"""
        select
            oi.id as order_id,
            oi.payer_id::text as payer_id,
            cl.system_name AS currency,
            c.name AS client_name,
            'payin' AS order_type
        from orders.invoice oi
        JOIN lists.currency_list cl ON cl.id = oi.currency_id
        JOIN clients.client c ON c.id = oi.client_id
        where oi.id in {tuple(orders_ids)}
        union all
        select
            w.id as order_id,
            (w.extra->'payerInfo'->>'userID')::text as payer_id,
            cl.system_name AS currency,
            c.name AS client_name,
            'payout' AS order_type
        from orders.withdraw w
        JOIN lists.currency_list cl ON cl.id = w.currency_id
        JOIN clients.client c ON c.id = w.client_id
        where w.id in {tuple(orders_ids)}
    """

def get_data_by_days(start_date: str, end_date: str):
    top_payers = read_sql_retry_engine(ENGINE_DWH, text("select payer_id from cascade.top_payers"))
    # try:
    start = safe_parse_date(start_date)
    end = safe_parse_date(end_date)
    # except Exception as e:
    #     print(e)
    #     return pd.DataFrame()
    if start > end:
        return pd.DataFrame()

    final_df = pd.DataFrame()
    current_date = start
    day_counter = 1
    while current_date <= end:
        window_start_dt = current_date
        window_end_dt = min(current_date + timedelta(days=6), end)
        window_start = window_start_dt.strftime('%Y-%m-%d 00:00:00')
        window_end = window_end_dt.strftime('%Y-%m-%d 23:59:59')

        chunk_size = 30_000
        try:
            print(f"Дата с {window_start} по {window_end}")
            payer_ids = top_payers['payer_id'].astype(str).tolist()
            chunked_frames = []
            start_time_general = time.time()
            for i in range(0, len(payer_ids), chunk_size):
                chunk = payer_ids[i:i+chunk_size]
                print(f"payer_values: {len(chunk)}")
                payer_values = '[' + ','.join([f"'{pid}'" for pid in chunk]) + ']'
                query = create_query(window_start, window_end, payer_values)
                start_time_payers = time.time()
                time.sleep(0.5)
                # try:
                part = read_sql_retry_engine(ENGINE_SRC, query)
                # except Exception as e:
                #     log_sql_failure(query, e, chunk_idx=i//chunk_size+1, window_start=window_start, window_end=window_end, limit=10000)
                #     raise
                end_time_payers = time.time()
                print(f"Выгузили payers chunk {i//chunk_size+1} ({len(chunk)} ids) - {end_time_payers - start_time_payers}")
                print(f"part: {len(part)}")
                if not part.empty:
                    chunked_frames.append(part)
            if chunked_frames:
                daily_data = pd.concat(chunked_frames, ignore_index=True)
                print(f"daily_data: {len(daily_data)}")
                daily_data = (
                    daily_data
                    .groupby(['payer_id', 'currency', 'client_name', 'order_type'], as_index=False)
                    .agg(
                        total_orders=('total_orders', 'sum'),
                        success_orders=('success_orders', 'sum'),
                        amount_success=('amount_success', 'sum'),
                        banking_details_issued_count=('banking_details_issued_count', 'sum'),
                        last_order_date=('last_order_date', 'max'),
                        first_order_date=('first_order_date', 'min'),
                    )
                )
            else:
                daily_data = pd.DataFrame()
            print(f"Итого daily_data по окну: {len(daily_data)}")

            query_tickets = get_query_tickets(window_start, window_end)
            start_time_tickets = time.time()
            tickets_data = read_sql_retry_engine(ENGINE_TICKETS, text(query_tickets))
            end_time_tickets = time.time()
            print(f"Выгузили tickets - {end_time_tickets - start_time_tickets}")

            query_get_payers_by_tickets = get_payers_by_tickets(tickets_data['order_id'].tolist())
            start_time_payers_by_tickets = time.time()
            payers_by_tickets = read_sql_retry_engine(ENGINE_SRC, text(query_get_payers_by_tickets))
            end_time_payers_by_tickets = time.time()
            print(f"Выгузили payers_by_tickets - {end_time_payers_by_tickets - start_time_payers_by_tickets}")

            tickets_data['order_id'] = tickets_data['order_id'].astype(str)
            payers_by_tickets['order_id'] = payers_by_tickets['order_id'].astype(str)

            tickets_data = tickets_data.merge(
                payers_by_tickets, on='order_id', how='inner'
            )
            tickets_data = (
                tickets_data.groupby(['payer_id', 'currency', 'order_type', 'client_name'])
                .agg(tickets_count=('order_id', 'count'),
                        count_rejected_tickets=('has_decline_reason', 'sum'))
                .reset_index()
            )

            print(f"Размер tickets_data - {len(tickets_data)}")
            daily_data = daily_data.merge(
                tickets_data,
                on=['payer_id', 'currency', 'client_name', 'order_type'],
                how='left'
            )
            print(f"Размер daily_data - {len(daily_data)}")
            end_time = time.time()
            print("Выгузили - ", end_time - start_time_general)
            if not daily_data.empty:
                final_df = pd.concat([final_df, daily_data], ignore_index=True)
                print(f"Размер final_df - {len(final_df)}")
                print(f"Получено {len(daily_data)} записей")
            else:
                print(f"Нет данных")
            current_date += timedelta(days=7)
            day_counter += 1
            time.sleep(0.5)
        except Exception as e:
            print(f"Ошибка при получении данных за {current_date.strftime('%Y-%m-%d')}: {e}")
            break
    return final_df

def update_db():
    start_date = datetime(datetime.now().year, 1, 1).strftime('%Y-%m-%d')
    # start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')
    data = get_data_by_days(start_date, end_date)
    metrics_success_banking = (
    data.groupby(['payer_id', 'currency', 'client_name', 'order_type'])
        .agg(
            total_orders=('total_orders', 'sum'),
            success_orders=('success_orders', 'sum'),
            amount_success=('amount_success', 'sum'),
            banking_details_issued_count=('banking_details_issued_count', 'sum'),
            last_order_date=('last_order_date', 'max'),
            first_order_date=('first_order_date', 'min'),
            tickets_count=('tickets_count', 'sum'),
            count_rejected_tickets=('count_rejected_tickets', 'sum')
        )
        .reset_index()
    )
    metrics_success_banking['conversion_payment'] = (
        metrics_success_banking['success_orders'] / metrics_success_banking['banking_details_issued_count']
    )
    metrics_success_banking['conversion_issued'] = (
        metrics_success_banking['banking_details_issued_count'] / metrics_success_banking['total_orders']
    )
    with ENGINE_DWH.begin() as conn:
        conn.execute(text("TRUNCATE TABLE cascade.info_about_payers"))
    metrics_success_banking = metrics_success_banking[metrics_success_banking['success_orders'] > 0]
    metrics_success_banking.to_sql(
        schema='cascade',
        name='info_about_payers',
        if_exists='append',
        con=ENGINE_DWH,
        index=False
    )

start = time.time()
update_db()
end = time.time()
print(f"Время выполнения: {end - start} секунд")