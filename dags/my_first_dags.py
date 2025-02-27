from airflow import DAG
from datetime import datetime, timedelta

from airflow.example_dags.example_latest_only_with_trigger import task2
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'gzang',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

def great(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"airflow chào {last_name} {first_name}, i am {age} days old!!!")

def get_name(ti):
    ti.xcom_push(key='first_name', value='Giang')
    ti.xcom_push(key='last_name', value='Trường')

def get_age(ti):
    ti.xcom_push(key='age', value=2)

with DAG(
    dag_id='my_first_dag_1',
    default_args=default_args,
    description='This is my firt dag i write in this computer',
    start_date=datetime(2025, 2,1),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo hello world! the first task is defined!!!'
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command='echo hey, this is task 2, and will be run after task 1.'
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command='echo finally, it will be done!!!'
    )

    task4 = PythonOperator(
        task_id='great_func',
        python_callable=great
        #Phương pháp truyền biến cho func <truyền trực tiếp value> dùng op_kwargs
        # op_kwargs={'name':'Giang','age':1}
    )
    #Có thể truyền biến từ work này sang work khác bằng xcom
    #Nhưng dung lượng giới hạn là 48kb nên không thể share dữ liệu lớn như pandas, df -> sập

    task5 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task6 = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )
    #method 1
    # task1>>task2
    # task1>>task3
    #method 2
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)
    #method 3
    task1 >> task2 >> task3 >> [task5, task6] >> task4