import sys
import os
import json
import logging
from kafka import KafkaConsumer, KafkaProducer

# Path setup for importing local modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../transforms'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../ingestion'))

from transformer import transform_record
from ingestion import push_to_elasticsearch

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TweetConsumer")

# Kafka configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
INPUT_TOPIC = 'covid-data'
OUTPUT_TOPIC = 'transformed-tweets'

# Mapping keywords/hashtags to preventive sentences
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

# Kafka Consumer
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='tweet-transformer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

logger.info("🚀 Kafka Consumer is running...")

for message in consumer:
    raw_record = message.value
    user_name = raw_record.get('user_name', 'Unknown')
    logger.info(f"🔄 Processing tweet from user: {user_name}")

    try:
        transformed = transform_record(raw_record)

        if transformed:
            # Check for preventive keywords from tweet_text and hashtags
            tweet_keywords = transformed.get("preventive_measures", {}).get("tweet_text", [])
            hashtag_keywords = transformed.get("preventive_measures", {}).get("hashtags", [])

            combined_keywords = tweet_keywords + hashtag_keywords

            # Generate sentences (avoid duplicates)
            new_sentences = {
                PREVENTIVE_HASHTAG_TO_SENTENCE.get(word.lower())
                for word in combined_keywords
                if word.lower() in PREVENTIVE_HASHTAG_TO_SENTENCE
            }

            # Remove None if any
            new_sentences.discard(None)

            if new_sentences:
                # Add to existing if already present
                existing_sentences = set(
                    transformed.get("preventive_measures", {}).get("sentences", [])
                )
                transformed["preventive_measures"]["sentences"] = list(existing_sentences.union(new_sentences))

            # Print the transformed record
            print("\n" + "=" * 50)
            print(f"✅ TRANSFORMED TWEET FROM: {user_name}")
            print("=" * 50)
            print(json.dumps(transformed, indent=2))
            print("=" * 50 + "\n")

            # Send to Kafka topic
            producer.send(OUTPUT_TOPIC, value=transformed)
            logger.info("✅ Sent to Kafka")

            # Push to Elasticsearch
            push_to_elasticsearch(transformed)

        else:
            logger.warning("⚠️ Transformation returned None. Skipping...")

    except Exception as e:
        logger.error(f"❌ Error processing tweet: {e}")
        import traceback
        traceback.print_exc()