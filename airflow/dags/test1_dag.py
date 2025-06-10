from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 9),
    'retries': 0,
}

with DAG(
    dag_id='hello_world_dag',
    default_args=default_args,
    description='A simple Hello World DAG',
    schedule_interval='*/5 * * * *',  # Trigger manually
    catchup=False,
    tags=['test']
) as dag:

    hello_task = BashOperator(
        task_id='print_hello',
        bash_command='echo "✅ Hello World from Airflow!"'
    )

    hello_task