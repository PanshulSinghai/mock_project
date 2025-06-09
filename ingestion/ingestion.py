# ingestion.py
from elasticsearch import Elasticsearch, ConnectionError
import logging

# Set up logger
logger = logging.getLogger("ElasticsearchIngestor")

# Index name
INDEX_NAME = "covid-tweets"

def get_elastic_client():
    try:
        es = Elasticsearch("http://localhost:9200")
        if not es.ping():
            logger.warning("⚠️ Elasticsearch is not responding.")
            return None
        logger.info("✅ Connected to Elasticsearch")
        return es
    except Exception as e:
        logger.error(f"❌ Failed to connect to Elasticsearch: {e}")
        return None

def push_to_elasticsearch(record: dict):
    es = get_elastic_client()
    if es is None:
        raise ConnectionError("Elasticsearch not available. Skipping push.")
    
    try:
        response = es.index(index=INDEX_NAME, document=record)
        logger.info("📦 Pushed record to Elasticsearch")
        return response
    except Exception as e:
        logger.error(f"❌ Failed to push to Elasticsearch: {e}")
        raise