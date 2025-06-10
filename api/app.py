from flask import Flask, jsonify, request
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import requests

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Connect to Elastic Cloud
es = Elasticsearch(
    cloud_id=os.getenv("CLOUD_ID"),
    basic_auth=(os.getenv("ELASTIC_USERNAME"), os.getenv("ELASTIC_PASSWORD"))
)

INDEX = "covid-tweets"

# Utility: Get date N days ago
def date_n_days_ago(days):
    return (datetime.utcnow() - timedelta(days=days)).strftime('%Y-%m-%d')

@app.route('/')
def home():
    return "Welcome to the Flask API!"

# 1. Tweets on coronavirus per country (fixed 3-month range)
@app.route("/tweets/covid/countrywise", methods=["GET"])
def covid_tweets_per_country():
    query = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {"term": {"is_covid_related": True}},
                    {"range": {"date": {"gte": "2020-06-01", "lte": "2022-08-31"}}}
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

# 2. Tweets per country on a daily basis
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

# 3. Top 100 meaningful words globally
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
    words = [
        {"keyword": bucket["key"], "frequency": bucket["doc_count"]}
        for bucket in response["aggregations"]["top_keywords"]["buckets"]
    ]
    return jsonify(words)

# 4. Top 100 meaningful words per country
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

# 5. Top 10 preventive measures globally and country-wise
@app.route("/tweets/covid/preventive-measures", methods=["GET"])
def top_preventive_measures_sentences():
    query = {
        "_source": ["country", "preventive_measures.sentences"],
        "size": 1000,  # Adjust based on expected volume
        "query": {
            "bool": {
                "must": [
                    {"term": {"is_covid_related": True}},
                    {"exists": {"field": "preventive_measures.sentences"}}
                ]
            }
        }
    }

    response = es.search(index=INDEX, body=query)
    result = {}

    for hit in response["hits"]["hits"]:
        source = hit["_source"]
        country = source.get("country", "Unknown")
        sentences = source.get("preventive_measures", {}).get("sentences", [])

        if sentences:
            if country not in result:
                result[country] = []
            result[country].extend(sentences)

    # Optional: remove duplicates per country
    for country in result:
        result[country] = list(set(result[country]))

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
                    {"range": {"date": {"gte": "2020-01-01", "lte": "2022-11-30"}}}
                ]
            }
        },
        "aggs": {
            "by_week": {
                "terms": {
                    "field": "week.keyword",
                    "size": 20,
                    "order": {"_key": "asc"}
                },
                "aggs": {
                    "by_country": {
                        "terms": {
                            "field": "country.keyword",
                            "size": 100,
                            "order": {"_count": "desc"}
                        }
                    }
                }
            }
        }
    }

    response = es.search(index=INDEX, body=query)
    result = {}

    for week_bucket in response["aggregations"]["by_week"]["buckets"]:
        week = week_bucket["key"]
        country_buckets = week_bucket["by_country"]["buckets"]
        ranked_countries = [
            {"country": b["key"], "count": b["doc_count"], "rank": i + 1}
            for i, b in enumerate(country_buckets)
        ]
        result[week] = ranked_countries

    return jsonify(result)

# 8. Economic impact analysis (global + country-wise trend)
WORLD_BANK_BASE_URL = "http://api.worldbank.org/v2"
YEARS = ["2019", "2020", "2021", "2022"]
INDICATOR = "NY.GDP.MKTP.CD"

# Manual country code corrections
MANUAL_COUNTRY_CODE_FIXES = {
    "United States": "USA",
    "United Kingdom": "GBR",
    "Iran, Islamic Republic of": "IRN",
    "Taiwan, Province of China": "TWN",
    "Korea, Democratic People's Republic of": "PRK",
    "Tanzania, United Republic of": "TZA",
    "Russia": "RUS",
    "Egypt": "EGY",
    "Venezuela": "VEN",
}

@app.route("/tweets/economictrends", methods=["GET"])
def gdp_growth():
    # Step 1: Get distinct countries from Elasticsearch
    body = {
        "size": 0,
        "aggs": {
            "distinct_countries": {
                "terms": {"field": "country.keyword", "size": 200}
            }
        }
    }
    es_response = es.search(index=INDEX, body=body)
    buckets = es_response.get("aggregations", {}).get("distinct_countries", {}).get("buckets", [])
    countries = [bucket["key"] for bucket in buckets if bucket["key"] and bucket["key"] != "Unknown"]

    # Step 2: World Bank country name → code mapping
    wb_country_codes = {}
    try:
        res = requests.get(f"{WORLD_BANK_BASE_URL}/country?format=json&per_page=300").json()
        if isinstance(res, list) and len(res) > 1:
            for item in res[1]:
                if item["region"]["id"] != "NA":  # Skip aggregates
                    wb_country_codes[item["name"]] = item["id"]
    except:
        return jsonify({"error": "Failed to fetch World Bank country codes"}), 500

    result = {}
    global_gdp = {year: 0 for year in YEARS}

    # Step 3: For each ES country, fetch GDP and compute growth
    for cname in countries:
        code = MANUAL_COUNTRY_CODE_FIXES.get(cname) or wb_country_codes.get(cname)

        # Loose match if not found
        if not code:
            for wb_name, wb_code in wb_country_codes.items():
                if cname.lower() in wb_name.lower():
                    code = wb_code
                    break
        if not code:
            continue  # Skip if we still can't resolve it

        gdp_data = {}
        for year in YEARS:
            gdp_url = f"{WORLD_BANK_BASE_URL}/country/{code}/indicator/{INDICATOR}?date={year}&format=json"
            try:
                response = requests.get(gdp_url).json()
                value = response[1][0].get("value") if isinstance(response, list) and len(response) > 1 and response[1] else None
                gdp_data[year] = value
                if value is not None:
                    global_gdp[year] += value
            except:
                gdp_data[year] = None

        # Calculate country growth %
        growth = {}
        prev = None
        for year in YEARS:
            current = gdp_data.get(year)
            if prev is not None and current is not None and prev != 0:
                growth[year] = round(((current - prev) / prev) * 100, 2)
            else:
                growth[year] = None
            prev = current

        result[cname] = {
            "gdp": gdp_data,
            "growth_percentage": growth
        }

    # Step 4: Compute global growth only from available countries
    global_growth = {}
    prev = None
    for year in YEARS:
        current = global_gdp[year]
        if prev is not None and current != 0 and prev != 0:
            global_growth[year] = round(((current - prev) / prev) * 100, 2)
        else:
            global_growth[year] = None
        prev = current

    def replace_none_with_zero(data):
        if isinstance(data, dict):
            return {k: replace_none_with_zero(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [replace_none_with_zero(item) for item in data]
        elif data is None:
            return 0
        else:
            return data
    
    # Step 5: Return structured response
    return jsonify(replace_none_with_zero({
        "global": {
            "gdp": global_gdp,
            "growth_percentage": global_growth
        },
        "countries": result
    }))

if __name__ == "__main__":
    app.run(debug=True)