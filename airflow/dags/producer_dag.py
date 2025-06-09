# airflow/dags/master_pipeline_with_es_guard.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import time
import requests
import json
from kafka import KafkaAdminClient
from kafka.errors import NoBrokersAvailable

default_args = {
    'owner': 'panshul',
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

ES_COUNTER_FILE = '/opt/airflow/logs/es_failure_counter.json'
MAX_FAILURES = 2

def check_kafka_connection():
    broker = os.getenv('KAFKA_BROKER', 'kafka:29092')
    for i in range(10):
        try:
            print(f"Attempt {i+1}: Connecting to Kafka at {broker}")
            admin = KafkaAdminClient(bootstrap_servers=[broker], api_version=(2, 0, 2))
            admin.list_topics()
            print("✅ Kafka is UP!")
            return
        except NoBrokersAvailable:
            print(f"❌ Kafka unavailable. Retrying...")
            time.sleep(5)
    raise Exception("❌ Kafka connection failed after retries.")

def check_elasticsearch_soft():
    url = os.getenv("ES_HOST", "http://localhost:9200")

    def read_failures():
        if os.path.exists(ES_COUNTER_FILE):
            with open(ES_COUNTER_FILE) as f:
                return json.load(f).get("failures", 0)
        return 0

    def write_failures(count):
        os.makedirs(os.path.dirname(ES_COUNTER_FILE), exist_ok=True)
        with open(ES_COUNTER_FILE, "w") as f:
            json.dump({"failures": count}, f)

    try:
        response = requests.head(url, timeout=3)
        if response.status_code == 200:
            print("✅ Elasticsearch is reachable.")
            write_failures(0)
        else:
            print(f"⚠️ Elasticsearch error {response.status_code}")
            write_failures(read_failures() + 1)
    except Exception as e:
        print(f"❌ ES request failed: {e}")
        write_failures(read_failures() + 1)

def fail_if_es_down_too_long():
    if not os.path.exists(ES_COUNTER_FILE):
        return
    with open(ES_COUNTER_FILE) as f:
        failures = json.load(f).get("failures", 0)
    print(f"📊 ES failure count: {failures}")
    if failures >= MAX_FAILURES:
        raise Exception("❌ ES has been down too long.")

def summarize_dlq():
    files = [
        '/opt/airflow/logs/failed_records.json',
        '/opt/airflow/logs/unprocessed_records.json',
    ]
    producer_dlq_dir = '/opt/airflow/logs/producer_dlq'

    def count_lines(path):
        try:
            with open(path) as f:
                return sum(1 for _ in f)
        except:
            return 0

    total = 0
    for f in files:
        total += count_lines(f)
    if os.path.exists(producer_dlq_dir):
        for f in os.listdir(producer_dlq_dir):
            total += count_lines(os.path.join(producer_dlq_dir, f))

    print(f"📊 Total DLQ failures: {total}")

with DAG(
    dag_id='master_pipeline_with_es_guard',
    default_args=default_args,
    start_date=datetime(2025, 6, 6),
    schedule_interval='*/30 * * * *',
    catchup=False,
    description="Full pipeline with Kafka-ES health and DLQ",
    tags=['kafka', 'es', 'dlq']
) as dag:

    kafka_check = PythonOperator(
        task_id='check_kafka',
        python_callable=check_kafka_connection
    )

    run_producer = BashOperator(
        task_id='run_producer',
        bash_command='python /opt/airflow/kafka_client/producer_one_shot.py'
    )

    run_consumer = BashOperator(
        task_id='run_consumer',
        bash_command='python /opt/airflow/kafka_client/consumer_airflow.py',
        env={'KAFKA_BROKER': '{{ var.value.get("kafka_broker", "kafka:29092") }}'}
    )

    check_es = PythonOperator(
        task_id='check_elasticsearch_soft',
        python_callable=check_elasticsearch_soft
    )

    es_guard = PythonOperator(
        task_id='fail_if_es_down_too_long',
        python_callable=fail_if_es_down_too_long
    )

    dlq_summary = PythonOperator(
        task_id='summarize_dlq',
        python_callable=summarize_dlq
    )

    # ✅ DAG Dependencies
    kafka_check >> run_producer
    kafka_check >> run_consumer
    kafka_check >> check_es >> es_guard >> dlq_summary