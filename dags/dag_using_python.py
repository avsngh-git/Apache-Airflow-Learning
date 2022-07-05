from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'avinash',
    'retries': 5,
    'retry_delay' : timedelta(minutes=2)
}

def greet():
    print('Hello World!')
#create DAGS
with DAG(
    dag_id = 'my_first_dag',
    description = 'this is our first dag that we will write',
    start_date = datetime(2022, 7, 29, 2),
    schedule_interval='@daily') as dag:
        task1 = PythonOperator(
            task_id = 'greet',
            python_callable=greet)
        task1

