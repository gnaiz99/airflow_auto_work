from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'gzang',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_with_catchup_and_backfill',
    default_args=default_args,
    start_date=datetime(2025,2,15),
    schedule_interval='@daily',
    catchup=False
) as dag:

    task1 = BashOperator(
        task_id='task1',
        bash_command='echo This is a simple bash command!'
    )

    task1

"""
Ý tưởng của catchup and backfill
catchup:
default catchup=True -> Tự động lênh lịch chạy từ ngày bắt đầu đến ngày hiện tại (hoặc ngày kết thúc DAG)

backfill:
catchup=False
Vẫn chạy được trong quá khứ bằng terminal
cmd: docker ps -> để xem id container, lấy id webserver (99e935334142)
cmd: docker exec -it 99e935334142 bash -> tương tác
cmd: airflow dags backfill -s 2025-02-01 -e 2025-02-23 dag_with_catchup_and_backfill
                            start_date      end_date            dag_id
"""