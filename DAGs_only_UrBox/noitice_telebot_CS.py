from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.operators.clickhouse_dbapi import ClickHouseDbApiHook
from datetime import datetime, timedelta
from clickhouse_driver import Client
import requests


TELEGRAM_TOKEN = '8021130212:AAHx7u70ptnb4VpZjDrsMu5i-yYY-dlBzyA'
TELEGRAM_CHAT_ID = '-4841407782'

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "owner": "Data-warehouse"
}

# ==== Hàm gửi telegram ====
def send_telegram_message(message: str):
    url = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage'
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'HTML'
    }
    response = requests.post(url, data=payload)
    response.raise_for_status()

# ==== Hàm xử lý chính ====
def get_data_and_send():
    current_date = datetime.now()
    previous_date = current_date - timedelta(days=1)
    pivot_data = f"""SELECT u.email as email_agent,
       			count(*) as so_luot_hoi_bot 
       FROM chatbot.message m 
       left join chatbot.conversations c on m.conversation_id = c.id 
       left join chatbot.users u on c.user_id = u.id 
       where 1=1
       and match(LOWER(m.content) , 'lg|urcard|urgift')
       AND m.role = 'user'
       and toDate(m.created_at) between '{current_date.date()}' and '{previous_date.date()}'
       group by u.email 
       order by count(*) desc
       """

    count_ask = f"""count(*) as so_luot_hoi_bot 
       FROM chatbot.message m 
       left join chatbot.conversations c on m.conversation_id = c.id 
       left join chatbot.users u on c.user_id = u.id 
       where 1=1
       and match(LOWER(m.content) , 'lg|urcard|urgift')
       AND m.role = 'user'
       and toDate(m.created_at) between '{current_date.date()}' and '{previous_date.date()}'
       """

    list_agent_ask_bot = ClickHouseDbApiHook(
        clickhouse_conn_id="clickhouse_conn_da", schema="chatbot"
    ).get_pandas_df(sql=pivot_data)

    number_ask_bot = ClickHouseDbApiHook(
        clickhouse_conn_id="clickhouse_conn_da", schema="chatbot"
    ).get_pandas_df(sql=count_ask)

    html_table = list_agent_ask_bot.to_html(index=False)
    message = f"""<b>Dear team CSKH,</b>
                \nData gửi thông tin số lượng check bot hôm qua ngày {previous_date}: <b>{number_ask_bot}</b>
                \n<b>{html_table} </b>"""
    send_telegram_message(message)

# ==== DAG ====
with DAG(
    dag_id='clickhouse_to_telegram_report',
    default_args=default_args,
    start_date=datetime(2025, 5, 15),
    schedule_interval='30 10 * * *',  # Mỗi ngày lúc 10h30 sáng
    catchup=False,
    tags=['report', 'telegram', 'clickhouse']
) as dag:
    task_send_report = PythonOperator(
        task_id='send_daily_user_report',
        python_callable=get_data_and_send
    )
