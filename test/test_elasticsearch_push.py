from elasticsearch import Elasticsearch

sample_data = {
    "country": "India",
    "country_code": "IN",
    "text": "CovidVaccine It has shown safety and efficacy.",
    "hashtags": ["covidvaccine"],
    "is_retweet": False,
    "is_covid_related": True,
    "contains_economic_terms": False,
    "contains_donation_terms": False,
    "tweet_date": "2020-05-11T11:23:32Z",
    "date": "2020-05-11",
    "week": "2020-W20",
    "keywords": ["efficacy", "covidvaccine", "safety"],
    "preventive_measures": {
        "tweet_text": [],
        "hashtags": ["vaccine"]
    },
    "covid_analysis": {
        "keywords": ["efficacy", "covidvaccine", "safety"],
        "economic_impact_terms": [],
        "donation_terms": [],
        "preventive_measures_count": 0
    }
}

es = Elasticsearch("http://localhost:9200")

if not es.ping():
    raise ValueError("Connection to Elasticsearch failed!")

index_name = "covid-tweets"

response = es.index(index=index_name, body=sample_data)

print("✅ Successfully pushed sample record to Elasticsearch")
print("📄 Response:", response)