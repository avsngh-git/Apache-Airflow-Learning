from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'avinash',
    'retries': 5,
    'retry_delay' : timedelta(minutes=2)

}
#create DAGS
with DAG(
    dag_id = 'my_first_dag',
    description = 'this is our first dag that we will write',
    start_date = datetime(2022, 7, 29, 2),
    schedule_interval='@daily') as dag:
        task1 = BashOperator(
            task_id = 'first_task',
            bash_command='echo hello world, this is the first task')

        task2 = BashOperator(
            task_id = 'second_task',
            bash_command= 'echo hello this is 2nd task, it will run after task1'
        )
        task3 = BashOperator(
            task_id = 'third_task',
            bash_command= 'third task, after task 1  but at same time as task 2'
        )
#task1.set_downstream(task2)
#task1.set_downstream(task3)
#use one line relationship
task1>>[task2, task3]