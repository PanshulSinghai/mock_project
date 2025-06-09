# kafka_client/producer_stream.py

import os
import json
import time
import shutil
import pandas as pd
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

SPLIT_FILES_DIR = os.path.join(os.path.dirname(__file__), '../data/split_csv_files')
PROCESSED_DIR = os.path.join(os.path.dirname(__file__), '../data/processed')

KAFKA_TOPIC = 'covid-data'
STREAM_DURATION = 600  # 10 minutes
SLEEP_BETWEEN_SENDS = 20  # seconds

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:29092"),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_file_to_kafka(file_path):
    df = pd.read_csv(file_path)
    for _, row in df.iterrows():
        producer.send(KAFKA_TOPIC, value=row.to_dict())
    producer.flush()
    shutil.move(file_path, os.path.join(PROCESSED_DIR, os.path.basename(file_path)))
    print(f"[✔] Sent: {os.path.basename(file_path)}")

def run_producer():
    print("[🚀] Streaming producer started for 10 minutes")

    start = time.time()
    while time.time() - start < STREAM_DURATION:
        files = sorted([f for f in os.listdir(SPLIT_FILES_DIR) if f.endswith('.csv')])
        if files:
            send_file_to_kafka(os.path.join(SPLIT_FILES_DIR, files[0]))
        else:
            print("[⏳] No new file found")

        time.sleep(SLEEP_BETWEEN_SENDS)

    print("[✅] Producer finished streaming.")

if __name__ == "__main__":
    run_producer()