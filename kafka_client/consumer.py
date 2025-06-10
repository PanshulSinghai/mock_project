import sys
import os
import json
import logging
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

# Add paths for local modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../transforms'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../ingestion'))

from transformer import transform_record
from ingestion import push_to_elasticsearch

# Suppress verbose logs
for noisy_logger in ["urllib3", "elastic_transport"]:
    logging.getLogger(noisy_logger).setLevel(logging.CRITICAL)

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TweetConsumer")

# Kafka config from env
#BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKER", "localhost:9092")
INPUT_TOPIC = 'covid-data'
OUTPUT_TOPIC = 'transformed-tweets'

# DLQ and Unprocessed paths from env
#DLQ_FILE = os.getenv("DLQ_FILE", "../logs/failed_records.json")
#UNPROCESSED_FILE = os.getenv("UNPROCESSED_FILE", "../logs/unprocessed_records.json")

DLQ_FILE = "/opt/airflow/loggerFiles/failed_records.json"
KAFKA_BROKER = "kafka:29092"
UNPROCESSED_FILE="/opt/airflow/loggerFiles/unprocessed_records.json"











PREVENTIVE_HASHTAG_TO_SENTENCE = {
    "vaccine": "Vaccination is one of the most effective ways to prevent COVID-19.",
    "mask": "Wearing a mask can help prevent the spread of the virus.",
    "quarantine": "Quarantine helps prevent the spread of infection from potentially infected individuals.",
    "isolation": "Isolation is necessary to prevent spreading COVID-19 to others.",
    "sanitizer": "Using hand sanitizer regularly helps kill viruses that may be on your hands.",
    "distancing": "Maintaining physical distancing is important to reduce the spread.",
    "handwash": "Regular handwashing is crucial to reduce the chance of infection.",
    "lockdown": "Lockdowns help limit interactions and break the chain of virus transmission."
}

def save_failed_record(stage, record, error):
    os.makedirs(os.path.dirname(DLQ_FILE), exist_ok=True)
    with open(DLQ_FILE, "a") as f:
        f.write(json.dumps({
            "timestamp": datetime.utcnow().isoformat(),
            "stage": stage,
            "error": str(error),
            "record": record
        }) + "\n")
    logger.warning(f"🚨 Failed record saved to DLQ: {DLQ_FILE}")

def save_unprocessed_record(record, reason="Transformation returned None"):
    os.makedirs(os.path.dirname(UNPROCESSED_FILE), exist_ok=True)
    with open(UNPROCESSED_FILE, "a") as f:
        f.write(json.dumps({
            "timestamp": datetime.utcnow().isoformat(),
            "reason": reason,
            "record": record
        }) + "\n")
    logger.warning(f"⚠️ Unprocessed record saved: {UNPROCESSED_FILE}")

# Kafka Consumer setup for partition 0
consumer = KafkaConsumer(
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
consumer.assign([TopicPartition(INPUT_TOPIC, 0)])
logger.info(f"📦 Assigned to partitions: {consumer.assignment()}")

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

logger.info("🚀 Kafka Consumer started...")

# Process loop
for message in consumer:
    raw_record = message.value
    print("📥 Received:")
    print(json.dumps(raw_record, indent=2))

    try:
        transformed = transform_record(raw_record)

        if not transformed:
            save_unprocessed_record(raw_record)
            continue

        tweet_keywords = transformed.get("preventive_measures", {}).get("tweet_text", [])
        messages = [PREVENTIVE_HASHTAG_TO_SENTENCE.get(k.lower()) for k in tweet_keywords if k.lower() in PREVENTIVE_HASHTAG_TO_SENTENCE]
        transformed["preventive_messages"] = messages

        # Send to next topic
        producer.send(OUTPUT_TOPIC, value=transformed).get(timeout=10)
        logger.info("✅ Sent transformed record to Kafka")

        # Push to Elastic (optional)
        response=push_to_elasticsearch(transformed)

    except Exception as e:
        logger.error(f"❌ Error processing record: {e}")
        save_failed_record("transform/push", raw_record, e)