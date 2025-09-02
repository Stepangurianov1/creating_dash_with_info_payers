import pandas as pd
from datetime import datetime, timedelta
import warnings
from sqlalchemy import create_engine
import time
import calendar
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
warnings.filterwarnings('ignore')


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

def get_engine_tickets():
    db_config = {
        'host': '138.68.88.175',
        'port': 5416,
        'database': 'ticket_replica',
        'user': 'klaksik77',
        'password': '6g3u0k13GhPhC2fvvPO'
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

def create_query(start_date: str, end_date: str):
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
    top_payers = pd.read_sql(text("select * from cascade.top_payers"), get_engine_dwh())
    try:
        start = safe_parse_date(start_date)
        end = safe_parse_date(end_date)
    except Exception as e:
        print(e)
        return pd.DataFrame()
    if start > end:
        return pd.DataFrame()

    final_df = pd.DataFrame()
    current_date = start
    day_counter = 1
    while current_date <= end:
        window_start_dt = current_date
        window_end_dt = min(current_date + timedelta(days=1), end)
        window_start = window_start_dt.strftime('%Y-%m-%d 00:00:00')
        window_end = window_end_dt.strftime('%Y-%m-%d 23:59:59')
        print(f"Дата с {window_start} по {window_end}")
        
        attempts = 0
        max_retries = 2
        backoff_sec = 2
        while True:
            try:
                query = create_query(window_start, window_end)
                start_time = time.time()
                daily_data = pd.read_sql(text(query), get_engine())

                query_tickets = get_query_tickets(window_start, window_end)
                tickets_data = pd.read_sql(text(query_tickets), get_engine_tickets())

                query_get_payers_by_tickets = get_payers_by_tickets(tickets_data['order_id'].tolist())
                payers_by_tickets = pd.read_sql(text(query_get_payers_by_tickets), get_engine())

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
                print("Выгузили - ", end_time - start_time)
                daily_data = daily_data.merge(top_payers, on='payer_id')
                if not daily_data.empty:
                    final_df = pd.concat([final_df, daily_data], ignore_index=True)
                    print(f"Размер final_df - {len(final_df)}")
                    print(f"Получено {len(daily_data)} записей")
                else:
                    print(f"Нет данных")
                break
            except (OperationalError,) as e:
                attempts += 1
                if attempts > max_retries:
                    print(f"Период {window_start}..{window_end}: исчерпаны попытки из-за подключения: {e}")
                    break
                sleep_for = backoff_sec * attempts
                print(f"Ошибка подключения, ретрай {attempts}/{max_retries} через {sleep_for}s: {e}")
                time.sleep(sleep_for)
            except Exception as e:
                print(f"Ошибка при получении данных за {current_date.strftime('%Y-%m-%d')}: {e}")
                break

        current_date += timedelta(days=2)
        day_counter += 1
        time.sleep(0.5)
    return final_df

def update_db():
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
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
    with get_engine_dwh().begin() as conn:
        conn.execute(text("TRUNCATE TABLE cascade.info_about_payers"))
    metrics_success_banking.to_sql(
        schema='cascade',
        name='info_about_payers',
        if_exists='append',
        con=get_engine_dwh(),
        index=False
    )

start = time.time()
update_db()
end = time.time()
print(f"Время выполнения: {end - start} секунд")