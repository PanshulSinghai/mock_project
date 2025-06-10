import os
import json
import time
import shutil
import pandas as pd
from kafka import KafkaProducer
from dotenv import load_dotenv
from kafka.errors import KafkaError
from time import sleep

# Load .env variables
load_dotenv()

# Constants and paths
# SPLIT_FILES_DIR = os.path.join(os.path.dirname(__file__), '../data/split_csv_files')
# PROCESSED_DIR = os.path.join(os.path.dirname(__file__), '../data/processed')


# Hardcoded paths inside the Airflow container (Docker)
SPLIT_FILES_DIR = "/opt/airflow/data/split_csv_files"
PROCESSED_DIR = "/opt/airflow/data/processed"
DLQ_FILE = "/opt/airflow/loggerFiles/failed_records.json"
KAFKA_BROKER = "kafka:29092"







KAFKA_TOPIC = 'covid-data'
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# DLQ_FILE = os.getenv("DLQ_FILE", os.path.join(os.path.dirname(__file__), '../logs/failed_records.json'))

# KAFKA_BROKER = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
if "BOOTSTRAP_SERVERS" in os.environ:
    print(f"[ENV] KAFKA_BROKER loaded from environment: {KAFKA_BROKER}")
else:
    print(f"[DEFAULT] KAFKA_BROKER fallback used: {KAFKA_BROKER}")

def save_to_dlq(record, reason):
    """Append a single failed record to DLQ JSON file."""
    try:
        os.makedirs(os.path.dirname(DLQ_FILE), exist_ok=True)
        with open(DLQ_FILE, 'a') as f:
            f.write(json.dumps({
                'record': record,
                'reason': str(reason),
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            }) + '\n')
        print(f"[DLQ] Record written to {DLQ_FILE}")
    except Exception as e:
        print(f"[DLQ ERROR] Failed to write to DLQ: {e}")

def send_file_to_kafka(file_path):
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"[✖] Failed to read file {file_path}: {e}")
        return

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"[✖] Kafka producer initialization failed: {e}")
        # Save every record to DLQ because Kafka is entirely down
        for _, row in df.iterrows():
            save_to_dlq(row.to_dict(), f"KafkaProducer init failed: {str(e)}")
        return

    for _, row in df.iterrows():
        message = row.to_dict()
        retries = 0
        while retries < MAX_RETRIES:
            try:
                producer.send(KAFKA_TOPIC, value=message, partition=0).get(timeout=10)
                break
            except KafkaError as e:
                print(f"[Retry {retries+1}] Kafka send failed for record: {e}")
                retries += 1
                sleep(RETRY_DELAY)
        else:
            print(f"[DLQ] Record failed after {MAX_RETRIES} retries.")
            save_to_dlq(message, "Kafka send failed after retries")

    try:
        producer.flush()
    except Exception as e:
        print(f"[✖] Kafka flush failed: {e}")

    try:
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        shutil.move(file_path, os.path.join(PROCESSED_DIR, os.path.basename(file_path)))
        print(f"[✔] Moved processed file: {file_path}")
    except Exception as e:
        print(f"[✖] Could not move file to processed: {e}")
        save_to_dlq({"file_path": file_path}, f"File move failed: {str(e)}")

def run_producer():
    print("[🚀] Starting Kafka Producer - sending 1 file every 30 seconds")

    while True:
        files = sorted([
            f for f in os.listdir(SPLIT_FILES_DIR)
            if f.endswith('.csv')
        ])

        if not files:
            print("[⏳] No files left to send. Waiting...")
            time.sleep(30)
            continue

        file_to_send = files[0]
        file_path = os.path.join(SPLIT_FILES_DIR, file_to_send)
        print(f"[📤] Processing file: {file_path}")
        send_file_to_kafka(file_path)
        
        time.sleep(5)

if __name__ == "__main__":
    run_producer()