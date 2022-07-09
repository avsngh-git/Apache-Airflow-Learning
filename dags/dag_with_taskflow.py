from airflow.decorators import dag, task
from datetime import datetime,timedelta




default_args = {
    'owner' : 'avinash',
    'retries': 5,
    'retry_delay' : timedelta(minutes=2)
}

@dag(dag_id='dag_with_taskflow_v01', 
    default_args=default_args,
    start_date = datetime(2022,7,8),
    schedule_interval='@daily')

def hello_world_etl():


    @task()
    def get_name():
        return 'Avinash'

    @task()
    def get_age():
        return 19

    @task()
    def greet():
        print(f'Hello World! My name is {name}'
            f'and i am {age} years old!')  

    name = get_name() 
    age =(get_age)
    greet(name=name, age=age)

greet_dag = hello_world_etl()