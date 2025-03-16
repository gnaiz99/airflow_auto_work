from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'gzang',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

# Cách viết khác để giảm thiểu số lượng dòng code <decorator>
@dag(dag_id='dag_with_taskflow_api',
     default_args=default_args,
     start_date=datetime(2025,2,1),
     schedule_interval='@daily')
def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {'first_name':'AirFlow',
                'last_name':'api'}

    @task
    def get_age():
        return '26'

    @task
    def great(first_name,last_name,age):
        print(f"Hello, i'm {first_name}_{last_name} and i'm {age} days old!!!")

    name_dict = get_name()
    age = get_age()
    great(first_name=name_dict['first_name'],
          last_name=name_dict['last_name'],
          age=age)

dags = hello_world_etl()