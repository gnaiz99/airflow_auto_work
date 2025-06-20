from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.operators.clickhouse_dbapi import ClickHouseDbApiHook
from datetime import datetime, timedelta
import requests
from airflow.providers.telegram.hooks.telegram import TelegramHook
from sqlalchemy.sql.functions import current_date

# TELEGRAM_TOKEN = '8021130212:AAHx7u70ptnb4VpZjDrsMu5i-yYY-dlBzyA' # tuyệt đối ko ghi thông tin credential lên code
TELEGRAM_CHAT_ID = '-1002592359099'
# -1004841407782
# Vào trang dưới đây để check chat_id
# https://api.telegram.org/bot<tele_token>/getUpdates
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "owner": "Data-warehouse"
}

# ==== Hàm gửi telegram ====
def send_telegram_message(message: str):
    telegram_conn_id = "telegram_conn_id"
    telegram_hook = TelegramHook(
            telegram_conn_id = telegram_conn_id
    )
    telegram_hook.send_message(api_params={ "chat_id": TELEGRAM_CHAT_ID,
                                            "text": message,
                                           "parse_mode": "HTML"})



# ==== Hàm xử lý chính ====
def get_data_and_send(**kwargs):

    # current_date = datetime.now() # sai
    # previous_date = current_date - timedelta(days=1) # sai
    # cái này sai trong trường hợp nếu task ông chạy lỗi mà phải chạy lại thì chỗ này sẽ bị thay đổi
    # cần phải cố định thời điểm này = context['data_interval_end'] và context['data_interval_start']
    # previous_date = kwargs['data_interval_start']
    current_date = kwargs['data_interval_start']
    previous_date = kwargs['data_interval_end']

    pivot_data = f"""SELECT u.email as email_agent,
       			count(*) as so_luot_hoi_bot 
       FROM chatbot.message m 
       left join chatbot.conversations c on m.conversation_id = c.id 
       left join chatbot.users u on c.user_id = u.id 
       where 1=1
       and match(LOWER(m.content) , 'lg|urcard|urgift')
       AND m.role = 'user'
       and toDate(m.created_at) between '{current_date.date()}'  and  '{previous_date.date()}'
       group by u.email 
       order by count(*) desc
       """

    count_ask = f"""select count(*) as so_luot_hoi_bot 
       FROM chatbot.message m 
       left join chatbot.conversations c on m.conversation_id = c.id 
       left join chatbot.users u on c.user_id = u.id 
       where 1=1
       and match(LOWER(m.content) , 'lg|urcard|urgift')
       AND m.role = 'user'
       and toDate(m.created_at) between '{current_date.date()}'  and  '{previous_date.date()}'
       """

    # Lưu ý: Object taọ lại 2 lần là phí tài nguyên
    clickhouse_hook = ClickHouseDbApiHook(
        clickhouse_conn_id="clickhouse_conn_da", schema="chatbot"
    )
    list_agent_ask_bot = clickhouse_hook.get_pandas_df(sql=pivot_data)
    number_ask_bot = clickhouse_hook.get_pandas_df(sql=count_ask)
    number = number_ask_bot.iloc[0]['so_luot_hoi_bot']

    # html_table = list_agent_ask_bot.to_html(index=False)
    table_text = list_agent_ask_bot.to_string(index=False)

    message = f"""<b>Dear team CSKH,</b>
                \nData gửi thông tin số lượng check bot ngày {previous_date.date()}: <b>{number}</b>
                \n<pre>{table_text}</pre>"""
    send_telegram_message(message)

# ==== DAG ====
with DAG(
    dag_id='clickhouse_to_telegram_report',
    default_args=default_args,
    start_date=datetime(2025, 5, 15),
    schedule_interval='30 10 * * *',  # Mỗi ngày lúc 10h30 sáng
    catchup=True,
    tags=['report', 'telegram', 'clickhouse']
) as dag:
    task_send_report = PythonOperator(
        task_id='send_daily_user_report',
        python_callable=get_data_and_send
    )
    # thiếu gọi task 

    task_send_report
