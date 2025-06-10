from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import socket

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(seconds=10)
}

def check_kafka_connection(**kwargs):
    try:
        # Try to connect to Kafka broker inside Docker
        s = socket.create_connection(("kafka", 29092), timeout=5)
        s.close()
        return "kafka_available"  # Changed to match the immediate next task
    except Exception as e:
        print(f"[✖] Kafka connection failed: {e}")
        return "kafka_connection_failed"

with DAG(
    dag_id='kafka_pipeline_branching_dag',
    default_args=default_args,
    description='Kafka Producer and Consumer pipeline with Kafka connection check',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2025, 6, 10),
    catchup=False,
    tags=["kafka", "branching"]
) as dag:

    check_kafka = BranchPythonOperator(
        task_id='check_kafka_connection',
        python_callable=check_kafka_connection,
        provide_context=True
    )
    kafka_available = DummyOperator(
        task_id='kafka_available'
    )
  
    run_producer = BashOperator(
        task_id='run_producer',
        bash_command='timeout 300 python /opt/airflow/kafka_client/producer.py > /opt/airflow/logs/producer_output.log 2>&1'
    )

    run_consumer = BashOperator(
        task_id='run_consumer',
        bash_command='timeout 300 python /opt/airflow/kafka_client/consumer.py > /opt/airflow/logs/consumer_output.log 2>&1'
    )

    kafka_connection_failed = BashOperator(
        task_id='kafka_connection_failed',
        bash_command='echo "Kafka is not available. Skipping pipeline." && exit 1'
    )

    # DAG flow
    check_kafka >> [kafka_available, kafka_connection_failed]
    kafka_available >> [run_producer, run_consumer]