import os
import json
import time
import shutil
import pandas as pd
from kafka import KafkaProducer
from dotenv import load_dotenv
from kafka.errors import KafkaError
from time import sleep

load_dotenv()

SPLIT_FILES_DIR = os.path.join(os.path.dirname(__file__), '../data/split_csv_files')
PROCESSED_DIR = os.path.join(os.path.dirname(__file__), '../data/processed')

KAFKA_TOPIC = 'covid-data'
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

def save_to_dlq(record, reason):
    dlq_dir = os.path.join(os.path.dirname(__file__), '../logs/producer_dlq')
    os.makedirs(dlq_dir, exist_ok=True)

    dlq_file = os.path.join(dlq_dir, f"failed_{int(time.time())}.json")
    with open(dlq_file, 'a') as f:
        f.write(json.dumps({
            'record': record,
            'reason': str(reason),
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }) + '\n')

def send_file_to_kafka(file_path):
    try:
        df = pd.read_csv(file_path)

        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        for _, row in df.iterrows():
            message = row.to_dict()
            retries = 0

            while retries < MAX_RETRIES:
                try:
                    producer.send(KAFKA_TOPIC, value=message).get(timeout=10)
                    break  # success
                except KafkaError as e:
                    print(f"[Retry {retries + 1}] Kafka send failed: {e}")
                    retries += 1
                    sleep(RETRY_DELAY)

            else:
                print(f"[DLQ] Could not send message after {MAX_RETRIES} retries.")
                save_to_dlq(message, "Kafka send failed after retries")

        producer.flush()
        print(f"[✔] Finished processing: {os.path.basename(file_path)}")

    except Exception as e:
        print(f"[✖] Error reading file: {e}")
    finally:
        shutil.move(file_path, os.path.join(PROCESSED_DIR, os.path.basename(file_path)))
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
        send_file_to_kafka(file_path)
        time.sleep(30)

if __name__ == "__main__":
    run_producer()