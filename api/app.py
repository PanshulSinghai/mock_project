from flask import Flask, jsonify, request
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta

app = Flask(__name__)
es = Elasticsearch("http://localhost:9200")
INDEX = "covid-tweets"

# Utility function to get the current date minus N days
def date_n_days_ago(days):
    return (datetime.utcnow() - timedelta(days=days)).strftime('%Y-%m-%d')

@app.route('/')
def home():
    return "Welcome to the Flask API!"

# 1. Tweets on coronavirus per country (last 3 months)
@app.route("/tweets/covid/countrywise", methods=["GET"])
def covid_tweets_per_country():
    query = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {"term": {"is_covid_related": True}},
                    {"range": {"date": {"gte": "2020-06-01", "lte": "2020-08-31"}}}
                ]
            }
        },
        "aggs": {
            "by_country": {
                "terms": {"field": "country.keyword", "size": 200}
            }
        }

    }  
    response = es.search(index=INDEX, body=query)
    result = {bucket["key"]: bucket["doc_count"] for bucket in response["aggregations"]["by_country"]["buckets"]}
    return jsonify(result)

# 2. Tweets per country on daily basis
@app.route("/tweets/daily/countrywise", methods=["GET"])
def daily_tweets_per_country():
    query = {
        "size": 0,
        "aggs": {
            "by_country": {
                "terms": {"field": "country.keyword", "size": 200},
                "aggs": {
                    "by_date": {
                        "date_histogram": {
                            "field": "date",
                            "calendar_interval": "day"
                        }
                    }
                }
            }
        }
    }
    response = es.search(index=INDEX, body=query)
    result = {}
    for country in response["aggregations"]["by_country"]["buckets"]:
        result[country["key"]] = {
            bucket["key_as_string"].split("T")[0]: bucket["doc_count"]
            for bucket in country["by_date"]["buckets"]
        }
    return jsonify(result)
# 3. Top 100 meaningful words globally in covid-related tweets
@app.route("/tweets/keywords/global", methods=["GET"])
def top_keywords_global():
    query = {
        "size": 0,
        "query": {"term": {"is_covid_related": True}},
        "aggs": {
            "top_keywords": {
                "terms": {"field": "covid_analysis.keywords.keyword", "size": 100}
            }
        }
    }
    response = es.search(index=INDEX, body=query)

    # Return list of dictionaries with keyword and its frequency
    words = [
        {"keyword": bucket["key"], "frequency": bucket["doc_count"]}
        for bucket in response["aggregations"]["top_keywords"]["buckets"]
    ]

    return jsonify(words)
# 4. Top 100 meaningful words per country in covid-related tweets
@app.route("/tweets/covid/topwords/countrywise", methods=["GET"])
def top_keywords_countrywise():
    query = {
        "size": 0,
        "query": {"term": {"is_covid_related": True}},
        "aggs": {
            "by_country": {
                "terms": {"field": "country.keyword", "size": 200},
                "aggs": {
                    "top_keywords": {
                        "terms": {"field": "covid_analysis.keywords.keyword", "size": 100}
                    }
                }
            }
        }
    }
    response = es.search(index=INDEX, body=query)
    result = {}
    for country in response["aggregations"]["by_country"]["buckets"]:
        result[country["key"]] = [
            {"keyword": bucket["key"], "frequency": bucket["doc_count"]}
            for bucket in country["top_keywords"]["buckets"]
        ]
    return jsonify(result)
# 5. Top 10 preventive/precautionary measures (Global and Country-wise)
@app.route("/tweets/covid/preventive-measures", methods=["GET"])
def top_preventive_measures():
    query = {
        "size": 0,
        "aggs": {
            "by_country": {
                "terms": {"field": "country.keyword", "size": 200},
                "aggs": {
                    "measures": {
                        "terms": {"field": "preventive_measures.hashtags.keyword", "size": 10}
                    }
                }
            }
        }
    }
    response = es.search(index=INDEX, body=query)
    result = {}
    for country in response["aggregations"]["by_country"]["buckets"]:
        result[country["key"]] = [bucket["key"] for bucket in country["measures"]["buckets"]]
    return jsonify(result)

# 6. Total donations country-wise
@app.route("/tweets/covid/donations", methods=["GET"])
def donations_by_country():
    query = {
        "size": 0,
        "query": {"term": {"contains_donation_terms": True}},
        "aggs": {
            "donations_by_country": {
                "terms": {"field": "country.keyword", "size": 200}
            }
        }
    }
    response = es.search(index=INDEX, body=query)
    result = {bucket["key"]: bucket["doc_count"] for bucket in response["aggregations"]["donations_by_country"]["buckets"]}
    return jsonify(result)

# 7. Country ranking (week-wise for last 2 months)
@app.route("/tweets/covid/countryranking", methods=["GET"])
def country_ranking_weekly():
    query = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {"term": {"is_covid_related": True}},
                    {"range": {"date": {"gte": date_n_days_ago(60)}}}
                ]
            }
        },
        "aggs": {
            "by_week": {
                "terms": {"field": "week.keyword", "size": 20, "order": {"_key": "asc"}},
                "aggs": {
                    "by_country": {
                        "terms": {"field": "country.keyword", "size": 10}
                    }
                }
            }
        }
    }
    response = es.search(index=INDEX, body=query)
    result = {}
    for week in response["aggregations"]["by_week"]["buckets"]:
        result[week["key"]] = {bucket["key"]: bucket["doc_count"] for bucket in week["by_country"]["buckets"]}
    return jsonify(result)

# 8. Economic impact analysis (global + country-wise trend)
@app.route("/tweets/covid/economy-impact", methods=["GET"])
def economic_impact():
    query = {
        "size": 0,
        "query": {"term": {"contains_economic_terms": True}},
        "aggs": {
            "by_country": {
                "terms": {"field": "country.keyword", "size": 200},
                "aggs": {
                    "by_date": {
                        "date_histogram": {
                            "field": "date",
                            "calendar_interval": "week"
                        }
                    }
                }
            }
        }
    }
    response = es.search(index=INDEX, body=query)
    result = {}
    for country in response["aggregations"]["by_country"]["buckets"]:
        result[country["key"]] = {bucket["key_as_string"]: bucket["doc_count"] for bucket in country["by_date"]["buckets"]}
    return jsonify(result)

# Run the app on custom port
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)