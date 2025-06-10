import logging
import os
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logger
logger = logging.getLogger("ElasticsearchIngestor")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Elastic Cloud credentials and Cloud ID from .env
CLOUD_ID = os.getenv("CLOUD_ID")
ELASTIC_USERNAME = os.getenv("ELASTIC_USERNAME", "elastic")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD")

# Connect to Elasticsearch Cloud
es = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD)
)

# Validate connection
if not es.ping():
    raise ValueError("❌ Connection to Elasticsearch Cloud failed!")

# Index name
INDEX_NAME = "covid-tweets"

def push_to_elasticsearch(record: dict):
    print("📤 Attempting to push record to Elasticsearch Cloud")
    try:
        response = es.index(index=INDEX_NAME, document=record)
        logger.info("📦 Successfully pushed record to Elasticsearch")
        return response
    except Exception as e:
        logger.error(f"❌ Failed to push to Elasticsearch: {e}")
        
        return None