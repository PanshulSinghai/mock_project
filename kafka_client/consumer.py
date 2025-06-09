import sys
import os
import json
import logging
import traceback
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

# Add paths for local modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../transforms'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../ingestion'))

from transformer import transform_record
from ingestion import push_to_elasticsearch

# Suppress verbose retry logs from Elasticsearch/urllib3
for noisy_logger in ["urllib3", "elastic_transport"]:
    logging.getLogger(noisy_logger).setLevel(logging.CRITICAL)

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TweetConsumer")

# Kafka configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
INPUT_TOPIC = 'covid-data'
OUTPUT_TOPIC = 'transformed-tweets'

# DLQ file paths
DLQ_FILE = "/Users/panshulsinghai/Documents/mock_project/logs/failed_records.json"
UNPROCESSED_FILE = "/Users/panshulsinghai/Documents/mock_project/logs/unprocessed_records.json"

# Preventive hashtags to messages
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
    failure = {
        "timestamp": datetime.utcnow().isoformat(),
        "stage": stage,
        "error": str(error),
        "record": record
    }

    os.makedirs(os.path.dirname(DLQ_FILE), exist_ok=True)
    with open(DLQ_FILE, "a") as f:
        f.write(json.dumps(failure) + "\n")

    logger.warning(f"🚨 Saved failed record to {DLQ_FILE}")

def save_unprocessed_record(record, reason="Transformation returned None"):
    failure = {
        "timestamp": datetime.utcnow().isoformat(),
        "reason": reason,
        "record": record
    }

    os.makedirs(os.path.dirname(UNPROCESSED_FILE), exist_ok=True)
    with open(UNPROCESSED_FILE, "a") as f:
        f.write(json.dumps(failure) + "\n")

    logger.warning(f"⚠️ Saved unprocessed record to {UNPROCESSED_FILE}")

# Kafka Consumer setup (manually assign partition 0)
consumer = KafkaConsumer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

partition = TopicPartition(INPUT_TOPIC, 0)
consumer.assign([partition])
print(f"📦 Assigned to partitions: {consumer.assignment()}")

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

logger.info("🚀 Kafka Consumer is running...")

# Main Loop
for message in consumer:
    raw_record = message.value
    print("📥 Incoming record:")
    print(json.dumps(raw_record, indent=2))

    user_name = raw_record.get('user_name', 'Unknown')
    logger.info(f"🔄 Processing tweet from user: {user_name}")

    try:
        transformed = transform_record(raw_record)

        if not transformed:
            logger.warning("⚠️ Transformation returned None. Saving to unprocessed file.")
            save_unprocessed_record(raw_record)
            continue

        # Add preventive messages based on keywords
        tweet_keywords = transformed.get("preventive_measures", {}).get("tweet_text", [])
        hashtag_keywords = transformed.get("preventive_measures", {}).get("hashtags", [])
        combined_keywords = tweet_keywords + hashtag_keywords

        new_sentences = {
            PREVENTIVE_HASHTAG_TO_SENTENCE.get(word.lower())
            for word in combined_keywords
            if word.lower() in PREVENTIVE_HASHTAG_TO_SENTENCE
        }
        new_sentences.discard(None)

        if new_sentences:
            existing = set(transformed.get("preventive_measures", {}).get("sentences", []))
            transformed["preventive_measures"]["sentences"] = list(existing.union(new_sentences))

        # Print transformed output
        print("\n" + "=" * 50)
        print(f"✅ TRANSFORMED TWEET FROM: {user_name}")
        print("=" * 50)
        print(json.dumps(transformed, indent=2))
        print("=" * 50 + "\n")

        # Send to Kafka output topic
        try:
            producer.send(OUTPUT_TOPIC, value=transformed).get(timeout=10)
            logger.info("✅ Sent to Kafka output topic")
        except Exception as kafka_error:
            logger.error("❌ Kafka send failed")
            save_failed_record("kafka", transformed, kafka_error)

        # Push to Elasticsearch
        try:
            push_to_elasticsearch(transformed)
        except Exception as elastic_error:
            logger.warning("⚠️ Elasticsearch is not responding.")
            save_failed_record("elasticsearch", transformed, elastic_error)

    except Exception as e:
        logger.error(f"❌ Processing error: {e}")
        traceback.print_exc()
        save_failed_record("transform", raw_record, e)