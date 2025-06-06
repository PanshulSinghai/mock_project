import os
import json
import time
import shutil
import pandas as pd
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# Update paths based on your folder
SPLIT_FILES_DIR = os.path.join(os.path.dirname(__file__), '../data/split_csv_files')
PROCESSED_DIR = os.path.join(os.path.dirname(__file__), '../data/processed')
print(SPLIT_FILES_DIR)
# Kafka setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

KAFKA_TOPIC = 'covid-data'

def send_file_to_kafka(file_path):
    try:
        df = pd.read_csv(file_path)
        for _, row in df.iterrows():
            message = row.to_dict()
            producer.send(KAFKA_TOPIC, value=message)
        producer.flush()  # Ensure all messages are sent
        print(f"[✔] Sent: {os.path.basename(file_path)}")
    except Exception as e:
        print(f"[✖] Error: {e}")
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

        # Pick the first file in sorted order
        file_to_send = files[0]
        file_path = os.path.join(SPLIT_FILES_DIR, file_to_send)

        send_file_to_kafka(file_path)
        time.sleep(30)  # Wait before sending the next one

if __name__ == "__main__":
    run_producer()