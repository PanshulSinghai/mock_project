import re
import ast
import time
from datetime import datetime
import spacy
from geopy.geocoders import Nominatim
from geotext import GeoText
import pycountry
import logging
from functools import lru_cache
import unicodedata
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("tweet-transformer")

# Load spaCy English model
try:
    nlp = spacy.load("en_core_web_sm")
    logger.info("✅ spaCy model loaded successfully")
except OSError:
    logger.error("spaCy English model not found. Downloading...")
    from spacy.cli import download
    download("en_core_web_sm")
    nlp = spacy.load("en_core_web_sm")

# Initialize geopy with thread-safe configuration
geolocator = Nominatim(
    user_agent="covid_tweet_analyzer_kafka",
    timeout=10
)
logger.info("✅ Geolocator initialized")

# Define comprehensive keyword sets for analytics
COVID_KEYWORDS = {
    'covid', 'corona', 'coronavirus', 'virus', 'pandemic', 'epidemic', 
    'sars-cov-2', 'covid19', 'covid-19', 'ncov', 'outbreak', 'contagion'
}

ECONOMIC_TERMS = {
    'economy', 'recession', 'gdp', 'market', 'stock', 'unemployment', 
    'downturn', 'inflation', 'recovery', 'dow jones', 'economic', 'finance',
    'business', 'trade', 'supply chain', 'inflation', 'deficit', 'stimulus',
    'lockdown impact', 'economic crisis', 'financial crisis', 'job loss',
    'furlough', 'layoff', 'economic recovery', 'gig economy', 'remote work',
    'work from home', 'economic impact', 'market crash'
}

DONATION_TERMS = {
    'donate', 'donation', 'contribute', 'fund', 'fundraiser', 'charity',
    'support', 'help', 'gofundme', 'crowdfund', 'relief', 'aid', 'assistance',
    'pledge', 'sponsor', 'give', 'philanthropy', 'contribute', 'paypal.me',
    'venmo', 'cashapp', 'financial help', 'emergency fund', 'medical fund'
}

WHO_PREVENTIVE_MEASURES = {
    'wash hands', 'handwashing', 'sanitize', 'sanitizer', 'mask', 'face cover',
    'social distance', 'distancing', 'isolate', 'isolation', 'quarantine',
    'vaccine', 'vaccinate', 'ventilate', 'ventilation', 'disinfect', 'cover cough',
    'stay home', 'work home', 'remote work', 'avoid crowd', 'crowd avoid',
    'test', 'testing', 'contact trace', 'symptom', 'temperature check'
}



# Thread-safe country extraction cache
country_cache = {}

# Precompile regex patterns for efficiency
URL_PATTERN = re.compile(r"https?://\S+|www\.\S+")
MENTION_PATTERN = re.compile(r"@\w+")
ECONOMIC_PATTERNS = [re.compile(rf"\b{re.escape(term)}\b", re.IGNORECASE) for term in ECONOMIC_TERMS]
DONATION_PATTERNS = [re.compile(rf"\b{re.escape(term)}\b", re.IGNORECASE) for term in DONATION_TERMS]
WHO_PREVENTIVE_PATTERNS = [re.compile(rf"\b{re.escape(term)}\b", re.IGNORECASE) for term in WHO_PREVENTIVE_MEASURES]
## adding safe strip extra
def safe_str_strip(value):
    if isinstance(value, float):
        return ""  # Convert NaNs/float to empty string
    if isinstance(value, str):
        return value.strip()
    return str(value).strip() if value is not None else ""

def safe_bool_conversion(value):
    """Convert various truthy values to boolean"""
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    if not value or not str(value).strip():
        return False
    truthy = ("true", "1", "yes", "t", "y", "on")
    return str(value).strip().lower() in truthy

def parse_datetime(date_str, time_str):
    """Parse combined datetime with validation"""
    try:
        if not date_str or not date_str.strip() or not time_str or not time_str.strip():
            return None
        return datetime.strptime(f"{date_str.strip()} {time_str.strip()}", "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError) as e:
        logger.warning(f"Datetime parsing error: {e} for date={date_str}, time={time_str}")
        return None

def clean_text(text):
    """Clean and normalize text"""
    if not text:
        return ""
    
    # Normalize unicode
    text = unicodedata.normalize('NFKC', text)
    
    # Remove URLs, mentions, and special characters
    text = URL_PATTERN.sub('', text)
    text = MENTION_PATTERN.sub('', text)
    text = re.sub(r'[^\w\s.,!?;:\'"]', ' ', text)
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

@lru_cache(maxsize=1000)
def resolve_country(name):
    """Resolve country name and code with caching"""
    if not name or not name.strip():
        return "Unknown", "UNK"
    
    try:
        country = pycountry.countries.lookup(name)
        return country.name, country.alpha_2
    except (LookupError, AttributeError):
        return "Unknown", "UNK"

def extract_country(location):
    """Extract country from location text with caching and enhanced logic"""
    if not location or not location.strip():
        return "Unknown", "UNK"
    
    # Check cache first
    if location in country_cache:
        return country_cache[location]
    
    # Try direct country match
    place = GeoText(location)
    if place.countries:
        country_name, country_code = resolve_country(place.countries[0])
        if country_name != "Unknown":
            country_cache[location] = (country_name, country_code)
            return country_name, country_code
    
    # Try city resolution
    if place.cities:
        for city in place.cities:
            try:
                location_data = geolocator.geocode(city, language='en', exactly_one=True)
                if location_data and hasattr(location_data, 'raw'):
                    address = location_data.raw.get('display_name', '')
                    # Look for country in the last 3 parts of the address
                    for part in address.split(',')[-3:]:
                        country_name, country_code = resolve_country(part.strip())
                        if country_name != "Unknown":
                            country_cache[location] = (country_name, country_code)
                            return country_name, country_code
            except Exception as e:
                logger.warning(f"Geo lookup failed for '{city}': {str(e)}")
                time.sleep(1.5)  # Respect rate limits
    
    country_cache[location] = ("Unknown", "UNK")
    return "Unknown", "UNK"

def extract_keywords(text):
    """Extract meaningful keywords from text"""
    if not text:
        return []
    
    try:
        doc = nlp(text)
        keywords = [
            token.lemma_.lower() for token in doc
            if token.pos_ in ("NOUN", "VERB", "ADJ", "PROPN") 
            and not token.is_stop 
            and token.is_alpha 
            and len(token) > 2
            and token.lemma_.lower() not in COVID_KEYWORDS  # Exclude COVID keywords
        ]
        return list(set(keywords))
    except Exception as e:
        logger.error(f"Keyword extraction failed: {str(e)}")
        return []

def detect_keyword_groups(text, patterns):
    """Detect specific keyword groups in text"""
    if not text:
        return []
    
    detected = set()
    text_lower = text.lower()
    
    for pattern in patterns:
        for match in pattern.finditer(text_lower):
            detected.add(match.group(0))
    
    return list(detected)

def detect_hashtag_measures(hashtags):
    """Detect preventive measures in hashtags with partial matching"""
    if not hashtags:
        return []
    
    detected = set()
    for tag in hashtags:
        tag_lower = tag.lower()
        
        # Method 1: Check for exact preventive terms in hashtag
        for measure in WHO_PREVENTIVE_MEASURES:
            if measure in tag_lower:
                detected.add(measure)
        
        # Method 2: Check for partial matches (e.g., "vaccine" in "covidvaccine")
        for term in WHO_PREVENTIVE_MEASURES:
            for word in term.split():
                if word in tag_lower:
                    detected.add(term)
    
    return list(detected)

def is_covid_related(text, hashtags):
    """Determine if content is COVID-related"""
    if not text:
        return False
    
    # Check text for COVID keywords
    text_lower = text.lower()
    if any(kw in text_lower for kw in COVID_KEYWORDS):
        return True
    
    # Check hashtags for COVID keywords
    hashtag_text = ' '.join(hashtags).lower()
    if any(kw in hashtag_text for kw in COVID_KEYWORDS):
        return True
    
    return False

def parse_hashtags(raw_hashtags):
    """Safely parse hashtags from various formats"""
    if not raw_hashtags or not raw_hashtags.strip():
        return []
    
    try:
        # Handle list-like formats
        if isinstance(raw_hashtags, list):
            return raw_hashtags
        if raw_hashtags.startswith('[') and raw_hashtags.endswith(']'):
            return ast.literal_eval(raw_hashtags)
        
        # Handle comma-separated strings
        return [tag.strip().strip("'\"# ") for tag in raw_hashtags.split(",") if tag.strip()]
    except:
        # Fallback to simple splitting
        return [tag.strip() for tag in re.findall(r'#(\w+)', raw_hashtags)]

def transform_record(record):
    """Transform a raw tweet record into analytics-ready format"""
    try:
        # Extract essential columns
        user_location = safe_str_strip(record.get("user_location", "").strip())
        text = clean_text(record.get("text", ""))
        
        # Parse datetime
        tweet_datetime = parse_datetime(
            safe_str_strip(record.get("only_date", "")),
            safe_str_strip(record.get("only_time", ""))
        )
        
        # Process hashtags
        raw_hashtags = safe_str_strip(record.get("hashtags", ""))
        hashtag_list = parse_hashtags(raw_hashtags)
        hashtag_list = [clean_text(tag).lower() for tag in hashtag_list if tag.strip()]
        hashtag_list = list(set(hashtag_list))  # Deduplicate
        
        # Extract country information
        country_name, country_code = extract_country(user_location)
        
        # Build analytics flags
        covid_related = is_covid_related(text, hashtag_list)
        
        # Build transformed record
        transformed = {
            # Location information
            "country": country_name,
            "country_code": country_code,
            
            # Content information
            "text": text,
            "hashtags": hashtag_list,
            "is_retweet": safe_bool_conversion(record.get("is_retweet", False)),
            
            # Core analytics flags
            "is_covid_related": covid_related,
            "contains_economic_terms": bool(detect_keyword_groups(text, ECONOMIC_PATTERNS)) if text else False,
            "contains_donation_terms": bool(detect_keyword_groups(text, DONATION_PATTERNS)) if text else False,
            
            # Time-based fields
            "tweet_date": tweet_datetime.isoformat() + "Z" if tweet_datetime else None,
            "date": tweet_datetime.date().isoformat() if tweet_datetime else None,
            "week": f"{tweet_datetime.year}-W{tweet_datetime.isocalendar()[1]}" if tweet_datetime else None,
            
            # Keyword extraction
            "keywords": extract_keywords(text) if text else [],
            
            # Preventive measures detection
            "preventive_measures": {
                "tweet_text": detect_keyword_groups(text, WHO_PREVENTIVE_PATTERNS) if text else [],
                "hashtags": detect_hashtag_measures(hashtag_list),
            }
        }
        
        # Add full text analysis for COVID-related tweets
        if covid_related:
            transformed["covid_analysis"] = {
                "keywords": extract_keywords(text) if text else [],
                "economic_impact_terms": detect_keyword_groups(text, ECONOMIC_PATTERNS) if text else [],
                "donation_terms": detect_keyword_groups(text, DONATION_PATTERNS) if text else [],
                "preventive_measures_count": len(transformed["preventive_measures"]["tweet_text"])
            }
        
        return transformed
    
    except Exception as e:
        logger.error(f"Error transforming record: {str(e)}")
        logger.debug(traceback.format_exc())
        return None