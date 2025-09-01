import pandas as pd
from datetime import datetime, timedelta
import warnings
from sqlalchemy import create_engine
import time
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
            year, month, day = map(int, date_str.split('-'))
            last_day = calendar.monthrange(year, month)[1]
            return datetime(year, month, last_day)
        except Exception as e:
            print(f"Ошибка при корректировке даты: {e}")
            raise


def get_invoice_data_by_days(start_date: str, end_date: str):
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
            query = f"""
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
            where oi.created_at >= '{day_start}' and oi.created_at <= '{day_end}'
            """
            daily_data = pd.read_sql(text(query), get_engine())
            daily_data = daily_data.merge(top_payers, on='payer_id')
            if not daily_data.empty:
                daily_data['processing_date'] = current_date.strftime('%Y-%m-%d')
                final_df = pd.concat([final_df, daily_data], ignore_index=True)
                print(f"Размер final_df - {len(final_df)}")
                print(f"Получено {len(daily_data)} записей")
            else:
                print(f"Нет данных")
        except Exception as e:
            print(f"Ошибка при получении данных за {current_date.strftime('%Y-%m-%d')}: {e}")

        current_date += timedelta(days=1)
        day_counter += 1
        time.sleep(0.5)
    return final_df

def update_db():
    start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')
    invoice_data = get_invoice_data_by_days(start_date, end_date)
    # invoice_data.to_csv('invoice_data.csv', index=False)
    metrics_success_banking = (
        invoice_data[invoice_data['banking_details_issued'] == 1]
        .assign(amount_success=lambda d: d['amount'].where(d['status_id'] == 2, 0))
        .groupby(['payer_id', 'currency', 'client_name'])
        .agg(
            total_orders=('order_id', 'count'),
            success_orders=('status_id', lambda s: (s == 2).sum()),
            amount_success=('amount_success', 'sum'), 
            last_order_date=('created_at', 'max')
        )
        .assign(conversion_payment=lambda x: (x['success_orders']/x['total_orders']).round(2))
        .reset_index()
        .merge(invoice_data
                    .groupby(['payer_id', 'currency', 'client_name'])
                    .agg(conversion_issued=('banking_details_issued', 'mean'))
                    .reset_index(), on=['payer_id', 'currency', 'client_name'], how='inner'
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