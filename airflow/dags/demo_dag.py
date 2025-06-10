from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='run_kafka_producer2',
    start_date=datetime(2025, 6, 9),
    schedule_interval=None,
    catchup=False,
) as dag:
    print_hello = BashOperator(
        task_id='print_hello_world',
        bash_command='echo "Hello World"'
    )
    
    