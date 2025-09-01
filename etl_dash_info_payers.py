import pandas as pd
from datetime import datetime, timedelta
import warnings
from sqlalchemy import create_engine
import time
import numpy as np
import calendar
from sqlalchemy import text
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

def create_query_invoice(start_date: str, end_date: str):
    return f"""
            select 
                oi.id as order_id,
                oi.status_id,
                oi.payer_id,
                cl.system_name as currency,
                oi.amount, 
                case when oi.engine_id is null then 0 else 1 end as banking_details_issued,
                oi.created_at,
                c.name as client_name
            from orders.invoice oi
            join lists.currency_list cl on cl.id = oi.currency_id
            join clients.client c on c.id = oi.client_id
            where oi.created_at >= '{start_date}' and oi.created_at <= '{end_date}'
            """

def create_query_withdraw(start_date: str, end_date: str):
    return f"""select 
                    w.id as order_id,
                    w.status_id,
                    w.extra ->'payerInfo'->>'userID' as payer_id,
                    w.amount, 
                    case when w.engine_ids is null then 0 else 1 end as banking_details_issued,
                    w.created_at,
                    cl.system_name as currency,
                    c.name as client_name
                from orders.withdraw w 
                join lists.currency_list cl on cl.id = w.currency_id
                join clients.client c on c.id = w.client_id
                where w.extra ->'payerInfo'->>'userID' is not null
                and w.created_at >= '{start_date}' and w.created_at <= '{end_date}'
                """



def get_data_by_days(start_date: str, end_date: str, is_invoice: bool = True):
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
        day_start = current_date.strftime('%Y-%m-%d 00:00:00')
        day_end = current_date.strftime('%Y-%m-%d 23:59:59')
        print(f"День {day_counter}: {current_date.strftime('%Y-%m-%d')}")
        try:
            if is_invoice:
                query = create_query_invoice(day_start, day_end)
            else:
                query = create_query_withdraw(day_start, day_end)
            daily_data = pd.read_sql(text(query), get_engine())
            daily_data = daily_data.merge(top_payers, on='payer_id')
            if not daily_data.empty:
                daily_data['processing_date'] = current_date.strftime('%Y-%m-%d')
                final_df = pd.concat([final_df, daily_data], ignore_index=True)
                final_df['order_type'] = 'payin' if is_invoice else 'payout'
                print(f"Размер final_df - {len(final_df)}")
                print(f"Получено {len(daily_data)} записей")
            else:
                print(f"Нет данных")
        except Exception as e:
            print(f"Ошибка при получении данных за {current_date.strftime('%Y-%m-%d')}: {e}")

        current_date += timedelta(days=1)
        day_counter += 1
        time.sleep(0.5)
    if is_invoice:
        final_df['status_id']  = np.where(final_df['status_id'] == 2, 'success', 'other')
    else:
        final_df['status_id']  = np.where(final_df['status_id'] == 4, 'success', 'other')
    return final_df

def update_db():
    start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')
    invoice_data = get_data_by_days(start_date, end_date, is_invoice=True)
    withdraw_data = get_data_by_days(start_date, end_date, is_invoice=False)
    data = pd.concat([invoice_data, withdraw_data], ignore_index=True)

    metrics_success_banking = (
        data[data['banking_details_issued'] == 1]
        .assign(amount_success=lambda d: d['amount'].where(d['status_id'] == 'success', 0))
        .groupby(['payer_id', 'currency', 'client_name', 'order_type'])
        .agg(
            total_orders=('order_id', 'count'),
            success_orders=('status_id', lambda s: (s == 'success').sum()),
            amount_success=('amount_success', 'sum'), 
            last_order_date=('created_at', 'max')
        )
        .assign(conversion_payment=lambda x: (x['success_orders']/x['total_orders']).round(2))
        .reset_index()
        .merge(data
                    .groupby(['payer_id', 'currency', 'client_name', 'order_type'])
                    .agg(conversion_issued=('banking_details_issued', 'mean'))
                    .reset_index(), on=['payer_id', 'currency', 'client_name', 'order_type'], how='inner'
                )
        .sort_values('success_orders', ascending=False)
    )
    metrics_success_banking.to_sql(
        schema='cascade',
        name='info_about_payers',
        if_exists='replace',
        con=get_engine_dwh(),
        index=False
    )


update_db()