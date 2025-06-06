# ingestion.py
from elasticsearch import Elasticsearch
import logging

# Set up logger
logger = logging.getLogger("ElasticsearchIngestor")

# Connect to Elasticsearch
es = Elasticsearch("http://localhost:9200")

# Validate connection
if not es.ping():
    raise ValueError("❌ Connection to Elasticsearch failed!")

# Index name
INDEX_NAME = "covid-tweets"

def push_to_elasticsearch(record: dict):
    print("print fomr ingestion file")
    print(dict)
    try:
        response = es.index(index=INDEX_NAME, body=record)
        logger.info("📦 Pushed record to Elasticsearch")
        return response
    except Exception as e:
        logger.error(f"❌ Failed to push to Elasticsearch: {e}")
        import traceback
        traceback.print_exc()
        return None