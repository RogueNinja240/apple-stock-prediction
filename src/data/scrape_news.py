# ============================================================
# APPLE (AAPL) NEWS SCRAPER — STRATEGIC VERSION
# DUAL MODE: BULK OPTIMIZED + SMART BACKFILL
# FOLLOWS: Guardian Primary -> NYT Secondary -> GNews (2020+) -> AlphaVantage Validation -> Finnhub Recent Pass
# ============================================================

import pandas as pd
import requests
import time
import os
import re
import hashlib
import yaml
import pytz
from datetime import datetime, timedelta
from collections import defaultdict, Counter

# ================= CONFIG =================
with open("params.yaml", "r") as f:
    params_yaml = yaml.safe_load(f)["pipeline"]

COMPANY = params_yaml.get("company", "Apple")
TICKER = params_yaml.get("ticker", "AAPL")

START_DATE = datetime.strptime(params_yaml.get("start_date", "2017-01-01"), "%Y-%m-%d")
end_date_param = params_yaml.get("end_date", "2026-12-31")
if end_date_param.lower() == "today":
    END_DATE = datetime.now()
else:
    END_DATE = datetime.strptime(end_date_param, "%Y-%m-%d")

INPUT_DATASET = f"data/raw/{TICKER}_{START_DATE.year}_{END_DATE.year}_LOGGED_BULK.csv"
OUTPUT_FILE = f"data/raw/{TICKER}_news_data.csv"

# Article Limits
MAX_ARTICLES_PER_DATE = 35



# Sleep times (in seconds)
NYT_SLEEP = 12       # NYT rate limit: 5 requests/min (12s per req)
GUARDIAN_SLEEP = 0.1 # Guardian: 12 req/sec
SLEEP = 4            # Default for other APIs
ALPHA_SLEEP = 15     # Alpha Vantage is strict

# Strategic thresholds
TOP_VALIDATION_DAYS = 25     # Alpha Vantage only for top 25 highest-activity days

# ================= API KEYS =================
from dotenv import load_dotenv
load_dotenv() # Load from .env if present

NYT_KEY = os.getenv("NYT_KEY", "")
GNEWS_KEY = os.getenv("GNEWS_KEY", "")
GUARDIAN_KEY = os.getenv("GUARDIAN_KEY", "")
NEWSDATA_KEY = os.getenv("NEWSDATA_KEY", "")
FINNHUB_KEY = os.getenv("FINNHUB_KEY", "")
ALPHAVANTAGE_KEY = os.getenv("ALPHAVANTAGE_KEY", "")

# ================= FREE-TIER LIMITS =================
LIMITS = {
    "GUARDIAN": 5000,   # PRIMARY backbone (1999+, full text)
    "NYT": 500,         # SECONDARY backbone (Archive pulls or gap fill)
    "GNEWS": 100,       # SUPPLEMENT-2020+ (2020+ gap filler only)
    "FINNHUB": 300,     # RECENT-PATCH (Recent high volume, 1yr free)
    "ALPHAVANTAGE": 25  # SENTIMENT (Strict - only for validation/sentiment)
}

api_usage = defaultdict(int)
api_errors = defaultdict(list)
api_articles_added = defaultdict(int)
api_consecutive_failures = defaultdict(int)
MAX_CONSECUTIVE_FAILURES = 5
seen_hashes = set()
start_time = time.time()


# ====================================================
# TRADING DATE HELPER
# Converts any published timestamp to the correct
# "effective trading date" in US/Eastern time.
#
# Rules:
#   - Parse full timestamp → convert to US/Eastern
#   - If published >= 16:00 ET  → shift to next calendar day
#     (market is closed; earliest reaction is next open)
#   - If resulting date is Saturday → roll forward to Monday
#   - If resulting date is Sunday   → roll forward to Monday
#   - Unix timestamps (is_unix=True) and Alpha Vantage format
#     (is_alphavantage=True, e.g. "20240115T143000") are both supported
# ====================================================

def get_trading_date(published_timestamp, is_unix=False, is_alphavantage=False):
    """
    Return the effective trading date (YYYY-MM-DD) for a given publish timestamp.

    Parameters
    ----------
    published_timestamp : str | int | float
        Raw timestamp from the API response.
    is_unix : bool
        True if the timestamp is a Unix epoch integer (e.g. Finnhub).
    is_alphavantage : bool
        True if the timestamp uses Alpha Vantage format: 'YYYYMMDDTHHmmSS'.

    Returns
    -------
    str  YYYY-MM-DD of the effective trading date.
    """
    try:
        if is_unix:
            # Finnhub returns seconds since epoch
            dt = pd.to_datetime(published_timestamp, unit='s', utc=True)
            dt = dt.tz_convert('US/Eastern')

        elif is_alphavantage:
            # e.g. "20240115T143000" — no timezone info, treat as ET
            dt = pd.to_datetime(published_timestamp, format='%Y%m%dT%H%M%S')
            dt = dt.tz_localize('US/Eastern')

        else:
            # ISO 8601 strings from Guardian / NYT / GNews (UTC or offset-aware)
            dt = pd.to_datetime(published_timestamp)
            if dt.tzinfo is None:
                dt = dt.tz_localize('UTC')
            dt = dt.tz_convert('US/Eastern')

        # ── 4 PM cutoff: after market close → next calendar day ──
        if dt.hour >= 16:
            dt = dt + timedelta(days=1)

        # ── Weekend roll: market is closed Sat & Sun → Monday ──
        if dt.weekday() == 5:    # Saturday
            dt = dt + timedelta(days=2)
        elif dt.weekday() == 6:  # Sunday
            dt = dt + timedelta(days=1)

        return dt.strftime('%Y-%m-%d')

    except Exception:
        # Graceful fallback — avoids crashing on malformed timestamps
        if is_unix:
            return datetime.utcfromtimestamp(published_timestamp).strftime('%Y-%m-%d')
        elif isinstance(published_timestamp, str):
            if is_alphavantage and len(published_timestamp) >= 8:
                return f"{published_timestamp[:4]}-{published_timestamp[4:6]}-{published_timestamp[6:8]}"
            return published_timestamp[:10]
        return "2000-01-01"

# ====================================================
# EMPTY / PLACEHOLDER DETECTION & UTILS
# ====================================================

EMPTY_MARKERS = {
    "", " ", "no news", "no significant news",
    "no significant news reported", "none", "nan"
}

def is_effectively_empty(text):
    if text is None: return True
    t = str(text).strip().lower()
    if not t or t in EMPTY_MARKERS: return True
    if set(t) == {"|"}: return True
    return False

def print_header(text):
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70)

def print_section(text):
    print(f"\n{'─' * 70}")
    print(f"  {text}")
    print(f"{'─' * 70}")

def check_circuit_breaker(api):
    """Returns True if the API should NOT be called (exhausted or failing)."""
    if api_usage[api] >= LIMITS[api]:
        return True
    if api_consecutive_failures[api] >= MAX_CONSECUTIVE_FAILURES:
        return True
    return False


def log_api(date, api, status, message=""):
    timestamp = datetime.now().strftime("%H:%M:%S")
    status_icon = {"SUCCESS": "✅", "FAILED": "❌", "ERROR": "⚠️", "SKIPPED": "⏭️", "EMPTY_PAYLOAD": "📭"}.get(status, "ℹ️")
    print(f"   [{timestamp}] [{api:12s}] {status_icon} {status:15s} {message}")
    if status in ["FAILED", "ERROR"]:
        api_errors[api].append({"date": date, "status": status, "message": message})

def normalize_text(text):
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:500]

def article_hash(text):
    return hashlib.sha1(normalize_text(text).encode()).hexdigest()

def article_pack(*parts):
    return " ".join([str(p) for p in parts if p]).strip()

def count_articles_in_text(text):
    if is_effectively_empty(text): return 0
    return len([a for a in str(text).split("|") if a.strip()])

# ====================================================
# SAFE REQUEST
# ====================================================

def safe_request(url, params, api, identifier, verbose=True):

    if api_usage[api] >= LIMITS[api]:
        log_api(identifier, api, "SKIPPED", "Quota exhausted")
        return {}
    
    if api_consecutive_failures[api] >= MAX_CONSECUTIVE_FAILURES:
        log_api(identifier, api, "SKIPPED", f"Stopped after {MAX_CONSECUTIVE_FAILURES} failures")
        return {}

    try:
        r = requests.get(url, params=params, timeout=30)
        api_usage[api] += 1

        if r.status_code == 429:
            api_consecutive_failures[api] += 1
            log_api(identifier, api, "FAILED", "RATE LIMITED")
            return {}
        elif r.status_code == 401:
            api_consecutive_failures[api] += 1
            log_api(identifier, api, "FAILED", "AUTH FAILED")
            return {}
        elif r.status_code != 200:
            api_consecutive_failures[api] += 1
            log_api(identifier, api, "FAILED", f"HTTP {r.status_code}")
            return {}

        data = r.json()
        if data is None: 
            api_consecutive_failures[api] += 1
            return {}
        
        # Reset failure count on success
        api_consecutive_failures[api] = 0
        if verbose:
            log_api(identifier, api, "SUCCESS")
        return data if isinstance(data, (dict, list)) else {}

    except Exception as e:
        api_consecutive_failures[api] += 1
        log_api(identifier, api, "ERROR", str(e)[:50])
        return {}

def check_api_connectivity():
    print_section("🔌 CHECKING API CONNECTIVITY")
    tests = {
        "NYT":          {"url": "https://api.nytimes.com/svc/search/v2/articlesearch.json", "params": {"q": "test", "api-key": NYT_KEY}, "key": NYT_KEY},
        "GNEWS":        {"url": "https://gnews.io/api/v4/search",                           "params": {"q": "test", "token": GNEWS_KEY, "lang": "en"}, "key": GNEWS_KEY},
        "GUARDIAN":     {"url": "https://content.guardianapis.com/search",                  "params": {"q": "test", "api-key": GUARDIAN_KEY}, "key": GUARDIAN_KEY},
        "FINNHUB":      {"url": "https://finnhub.io/api/v1/company-news",                   "params": {"symbol": "NVDA", "from": "2024-01-01", "to": "2024-01-01", "token": FINNHUB_KEY}, "key": FINNHUB_KEY},
        "ALPHAVANTAGE": {"url": "https://www.alphavantage.co/query",                        "params": {"function": "NEWS_SENTIMENT", "tickers": "NVDA", "apikey": ALPHAVANTAGE_KEY}, "key": ALPHAVANTAGE_KEY}
    }

    status_dict = {}
    for api, config in tests.items():
        if not config["key"]:
            print(f"   [{api:12s}] 🔑 NO API KEY")
            status_dict[api] = "NO_KEY"
            continue
        try:
            r = requests.get(config["url"], params=config["params"], timeout=10)
            if r.status_code == 200:
                print(f"   [{api:12s}] ✅ CONNECTED")
                status_dict[api] = "CONNECTED"
            else:
                print(f"   [{api:12s}] ⚠️ HTTP {r.status_code}")
                status_dict[api] = f"HTTP_{r.status_code}"
        except Exception as e:
            print(f"   [{api:12s}] ❌ ERROR: {str(e)[:40]}")
            status_dict[api] = "ERROR"
    return status_dict

def print_quota_status():
    print_section("📊 INITIAL API QUOTA STATUS")
    for api, limit in LIMITS.items():
        used = api_usage[api]
        rem = limit - used
        print(f"   {api:12s} {limit:>12,} {used:>8,} {rem:>12,}")

# ====================================================
# FETCH FUNCTIONS
# ====================================================

def fetch_guardian_bulk(start_date, end_date):
    """Fetch Guardian over date range using pagination (PRIMARY)"""
    articles_by_date = defaultdict(list)
    print(f"\n   📦 Fetching Guardian (PRIMARY BACKBONE): {start_date.date()} to {end_date.date()}")

    total_articles = 0
    current_date = start_date
    while current_date <= end_date:
        if api_usage["GUARDIAN"] >= LIMITS["GUARDIAN"]: break

        next_month = current_date.replace(day=28) + timedelta(days=4)
        next_month = next_month - timedelta(days=next_month.day - 1)
        chunk_end = min(next_month - timedelta(days=1), end_date)

        page, pages_total = 1, 1
        while page <= pages_total:
            if check_circuit_breaker("GUARDIAN"): 
                log_api(f"Bulk {current_date.strftime('%b %Y')}", "GUARDIAN", "SKIPPED", "Circuit breaker active")
                break



            data = safe_request(
                "https://content.guardianapis.com/search",
                {
                    "q": "Apple OR AAPL OR iPhone OR Mac OR \"Tim Cook\"",
                    "from-date": current_date.strftime("%Y-%m-%d"),
                    "to-date": chunk_end.strftime("%Y-%m-%d"),
                    "show-fields": "headline,bodyText,trailText",
                    "page": page,
                    "page-size": 50,
                    "api-key": GUARDIAN_KEY
                },
                "GUARDIAN", f"Bulk {current_date.strftime('%b %Y')} p{page}",
                verbose=False
            )

            if not data: break
            response = data.get("response", {})
            pages_total = response.get("pages", 1)
            results = response.get("results", [])

            for r in results:
                pub_date = r.get("webPublicationDate", "")
                if not pub_date:
                    continue
                # FIX: convert full ISO timestamp → effective trading date (ET, 4PM cutoff + weekend roll)
                art_date = get_trading_date(pub_date)
                
                # LIMIT CHECK: Skip if date already has enough articles
                if len(articles_by_date[art_date]) >= MAX_ARTICLES_PER_DATE:
                    continue

                article_text = article_pack(
                    r.get("webTitle", ""),
                    r.get("fields", {}).get("headline", ""),
                    r.get("fields", {}).get("bodyText", "")
                )
                if article_text:
                    ahash = article_hash(article_text)
                    if ahash not in seen_hashes:
                        seen_hashes.add(ahash)
                        articles_by_date[art_date].append(article_text)
                        total_articles += 1

            page += 1
            time.sleep(GUARDIAN_SLEEP)

        current_date = chunk_end + timedelta(days=1)

    print(f"   ✅ Guardian: Fetched {total_articles} articles")
    return articles_by_date


def fetch_nyt_bulk(start_date, end_date):
    """Fetch NYT over date range using monthly chunks (SECONDARY)"""
    articles_by_date = defaultdict(list)
    print(f"\n   📦 Fetching NYT (SECONDARY BACKBONE): {start_date.date()} to {end_date.date()}")

    total_articles = 0
    current_date = start_date
    while current_date <= end_date:
        if api_usage["NYT"] >= LIMITS["NYT"]: break

        next_month = current_date.replace(day=28) + timedelta(days=4)
        next_month = next_month - timedelta(days=next_month.day - 1)
        chunk_end = min(next_month - timedelta(days=1), end_date)

        page = 0
        while page < 100:  # NYT max page is 100
            if check_circuit_breaker("NYT"): 
                log_api(f"Bulk {current_date.strftime('%b %Y')}", "NYT", "SKIPPED", "Circuit breaker active")
                break



            data = safe_request(
                "https://api.nytimes.com/svc/search/v2/articlesearch.json",
                {
                    "q": "Apple OR AAPL OR iPhone OR MacBook OR \"Tim Cook\"",
                    "begin_date": current_date.strftime("%Y%m%d"),
                    "end_date": chunk_end.strftime("%Y%m%d"),
                    "page": page,
                    "api-key": NYT_KEY
                },
                "NYT", f"Bulk {current_date.strftime('%b %Y')} p{page}",
                verbose=False
            )


            if not data: break
            docs = (data.get("response") or {}).get("docs") or []
            if not docs: break

            for doc in docs:
                pub_date = doc.get("pub_date", "")
                if not pub_date:
                    continue
                # FIX: convert full ISO timestamp → effective trading date (ET, 4PM cutoff + weekend roll)
                art_date = get_trading_date(pub_date)
                
                # LIMIT CHECK: Skip if date already has enough articles
                if len(articles_by_date[art_date]) >= MAX_ARTICLES_PER_DATE:
                    continue

                article_text = article_pack(
                    doc.get("headline", {}).get("main", ""),
                    doc.get("abstract", ""),
                    doc.get("lead_paragraph", "")
                )
                if article_text:
                    ahash = article_hash(article_text)
                    if ahash not in seen_hashes:
                        seen_hashes.add(ahash)
                        articles_by_date[art_date].append(article_text)
                        total_articles += 1

            page += 1
            time.sleep(NYT_SLEEP)

        current_date = chunk_end + timedelta(days=1)

    print(f"   ✅ NYT: Fetched {total_articles} articles")
    return articles_by_date


def fetch_guardian_for_dates(dates_list):
    """Fetch Guardian for specific dates (GAP FILLING)"""
    articles_by_date = defaultdict(list)
    print(f"\n   📦 Fetching Guardian (GAP FILLING): {len(dates_list)} dates")
    total_articles = 0

    total_dates = len(dates_list)
    for i, date_str in enumerate(dates_list, 1):
        if check_circuit_breaker("GUARDIAN"):
            log_api(date_str, "GUARDIAN", "SKIPPED", "Circuit breaker active")
            break
        
        # Display progress
        print(f"   📅 [Date {i}/{total_dates}] Processing {date_str}...")
        
        # PRE-FETCH LIMIT CHECK: Skip API call if date is already saturated
        if len(articles_by_date.get(date_str, [])) >= MAX_ARTICLES_PER_DATE:
            log_api(date_str, "GUARDIAN", "SKIPPED", f"Already has {MAX_ARTICLES_PER_DATE} articles")
            continue

        log_api(date_str, "GUARDIAN", "FETCHING")




        data = safe_request(
            "https://content.guardianapis.com/search",
            {
                "q": "Apple OR AAPL OR iPhone OR Mac OR \"Tim Cook\"",
                "from-date": date_str,
                "to-date": date_str,
                "show-fields": "headline,bodyText,trailText",
                "api-key": GUARDIAN_KEY
            },
            "GUARDIAN", date_str
        )
        if not data:
            if check_circuit_breaker("GUARDIAN"): break
            time.sleep(GUARDIAN_SLEEP)
            continue

        results = (data.get("response") or {}).get("results") or []
        for r in results:
            pub_date = r.get("webPublicationDate", "")
            # FIX: extract and convert the article's own timestamp so late-day
            # articles are correctly shifted to the next trading date.
            # Previously date_str_eff was never set, causing a NameError or
            # silently reusing a stale value from a prior loop iteration.
            art_date = get_trading_date(pub_date) if pub_date else date_str

            article_text = article_pack(
                r.get("webTitle", ""),
                r.get("fields", {}).get("headline", ""),
                r.get("fields", {}).get("bodyText", "")
            )
            if article_text:
                ahash = article_hash(article_text)
                if ahash not in seen_hashes:
                    seen_hashes.add(ahash)
                    articles_by_date[art_date].append(article_text)
                    total_articles += 1

        time.sleep(GUARDIAN_SLEEP)

    print(f"   ✅ Guardian: Added {total_articles} articles")
    return articles_by_date


def fetch_nyt_for_dates(dates_list):
    """Fetch NYT for specific dates (GAP FILLING)"""
    articles_by_date = defaultdict(list)
    print(f"\n   📦 Fetching NYT (GAP FILLING): {len(dates_list)} dates")
    total_articles = 0

    total_dates = len(dates_list)
    for i, date_str in enumerate(dates_list, 1):
        if check_circuit_breaker("NYT"): 
            log_api(date_str, "NYT", "SKIPPED", "Circuit breaker active")
            break
        
        # Display progress
        print(f"   📅 [Date {i}/{total_dates}] Processing {date_str}...")
        
        # PRE-FETCH LIMIT CHECK: Skip API call if date is already saturated
        if len(articles_by_date.get(date_str, [])) >= MAX_ARTICLES_PER_DATE:
            log_api(date_str, "NYT", "SKIPPED", f"Already has {MAX_ARTICLES_PER_DATE} articles")
            continue
            
        log_api(date_str, "NYT", "FETCHING")



        date_obj = datetime.strptime(date_str, "%Y-%m-%d")

        data = safe_request(
            "https://api.nytimes.com/svc/search/v2/articlesearch.json",
            {
                "q": "Apple OR AAPL OR iPhone OR MacBook OR \"Tim Cook\"",
                "begin_date": date_obj.strftime("%Y%m%d"),
                "end_date": date_obj.strftime("%Y%m%d"),
                "api-key": NYT_KEY
            },
            "NYT", date_str
        )
        if data:
            docs = (data.get("response") or {}).get("docs") or []
            for doc in docs:
                pub_date = doc.get("pub_date", "")
                # FIX: extract and convert the article's own timestamp.
                # Previously date_str_eff was never set in this function,
                # meaning the append target was always an undefined variable.
                art_date = get_trading_date(pub_date) if pub_date else date_str

                article_text = article_pack(
                    doc.get("headline", {}).get("main", ""),
                    doc.get("abstract", ""),
                    doc.get("lead_paragraph", "")
                )
                if article_text:
                    ahash = article_hash(article_text)
                    if ahash not in seen_hashes:
                        seen_hashes.add(ahash)
                        articles_by_date[art_date].append(article_text)
                        total_articles += 1

        if check_circuit_breaker("NYT"): break
        time.sleep(NYT_SLEEP)


    print(f"   ✅ NYT: Added {total_articles} articles")
    return articles_by_date


def fetch_gnews_for_gaps(articles_by_date, max_calls=100):
    """Fetch GNews - SUPPLEMENT 2020+"""
    empty_dates = [d for d, arts in articles_by_date.items() if len(arts) == 0 and d >= "2020-01-01"]
    empty_dates_sorted = sorted(empty_dates)
    print(f"\n   📦 Fetching GNews (SUPPLEMENT 2020+): {len(empty_dates_sorted)} dates")

    total_articles = 0
    dates_filled = 0
    total_dates = len(empty_dates_sorted)
    for i, date_str in enumerate(empty_dates_sorted, 1):
        if check_circuit_breaker("GNEWS") or dates_filled >= max_calls: 
            if check_circuit_breaker("GNEWS"):
                log_api(date_str, "GNEWS", "SKIPPED", "Circuit breaker active")
            break
        
        # Display progress
        print(f"   📅 [Date {i}/{total_dates}] Processing {date_str}...")
        
        log_api(date_str, "GNEWS", "FETCHING")




        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        data = safe_request(
            "https://gnews.io/api/v4/search",
            {
                "q": "Apple OR AAPL OR iPhone OR Mac",
                "from": date_obj.strftime("%Y-%m-%dT00:00:00Z"),
                "to": date_obj.strftime("%Y-%m-%dT23:59:59Z"),
                "lang": "en",
                "token": GNEWS_KEY
            },
            "GNEWS", date_str
        )
        if not data:
            time.sleep(1)
            continue

        articles_list = data.get("articles") or []
        for art in articles_list:
            pub_date = art.get("publishedAt", "")
            # get_trading_date handles UTC→ET conversion + 4PM cutoff + weekend roll
            art_date = get_trading_date(pub_date) if pub_date else date_str

            article_text = article_pack(art.get("title", ""), art.get("description", ""), art.get("content", ""))
            if article_text:
                ahash = article_hash(article_text)
                if ahash not in seen_hashes:
                    # LIMIT CHECK: Skip if date already has enough articles
                    if len(articles_by_date[art_date]) >= MAX_ARTICLES_PER_DATE:
                        continue
                    seen_hashes.add(ahash)
                    articles_by_date[art_date].append(article_text)
                    total_articles += 1

        if articles_list: dates_filled += 1
        time.sleep(1)

    print(f"   ✅ GNews: Filled {dates_filled} dates with {total_articles} articles")
    return articles_by_date


def fetch_finnhub_recent():
    """Fetch Finnhub articles - RECENT PATCH"""
    articles_by_date = defaultdict(list)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    print(f"\n   📦 Fetching Finnhub (RECENT PATCH): {start_date.date()} to {end_date.date()}")

    data = safe_request(
        "https://finnhub.io/api/v1/company-news",
        {
            "symbol": TICKER,
            "from": start_date.strftime("%Y-%m-%d"),
            "to": end_date.strftime("%Y-%m-%d"),
            "token": FINNHUB_KEY
        },
        "FINNHUB", "Finnhub Bulk",
        verbose=False
    )

    if not data or not isinstance(data, list): return articles_by_date

    total_articles = 0
    for art in data:
        timestamp = art.get("datetime", 0)
        # is_unix=True: Finnhub returns Unix epoch seconds
        art_date = get_trading_date(timestamp, is_unix=True)

        article_text = article_pack(art.get("headline", ""), art.get("summary", ""))
        if article_text:
            ahash = article_hash(article_text)
            if ahash not in seen_hashes:
                # LIMIT CHECK: Skip if date already has enough articles
                if len(articles_by_date[art_date]) >= MAX_ARTICLES_PER_DATE:
                    continue
                    
                seen_hashes.add(ahash)
                articles_by_date[art_date].append(article_text)
                total_articles += 1

    print(f"   ✅ Finnhub: Fetched {total_articles} articles")
    return articles_by_date


def fetch_alphavantage_for_top_days(top_dates):
    """Fetch Alpha Vantage - SENTIMENT VALIDATION"""
    articles_by_date = defaultdict(list)
    print(f"\n   📦 Fetching AlphaVantage (SENTIMENT): top {len(top_dates)} dates")

    total_articles = 0
    total_dates = len(top_dates[:TOP_VALIDATION_DAYS])
    for i, date_str in enumerate(top_dates[:TOP_VALIDATION_DAYS], 1):
        if check_circuit_breaker("ALPHAVANTAGE"): 
            log_api(date_str, "ALPHAVANTAGE", "SKIPPED", "Circuit breaker active")
            break
            
        # Display progress
        print(f"   📅 [Date {i}/{total_dates}] Processing {date_str}...")


        date_obj = datetime.strptime(date_str, "%Y-%m-%d")

        data = safe_request(
            "https://www.alphavantage.co/query",
            {
                "function": "NEWS_SENTIMENT",
                "tickers": TICKER,
                "time_from": date_obj.strftime("%Y%m%dT0000"),
                "time_to": date_obj.strftime("%Y%m%dT2359"),
                "sort": "EARLIEST",
                "limit": 50,
                "apikey": ALPHAVANTAGE_KEY
            },
            "ALPHAVANTAGE", date_str
        )
        if not data:
            time.sleep(ALPHA_SLEEP)
            continue

        feed = data.get("feed") or []
        for art in feed:
            pub_date = art.get("time_published", "")
            # is_alphavantage=True: format is "YYYYMMDDTHHmmSS", no tz info, treat as ET
            art_date = get_trading_date(pub_date, is_alphavantage=True) if pub_date else date_str

            article_text = article_pack(
                art.get("title", ""),
                art.get("summary", ""),
                f"[Sentiment: {art.get('overall_sentiment_label', 'N/A')}]"
            )
            if article_text:
                ahash = article_hash(article_text)
                if ahash not in seen_hashes:
                    # LIMIT CHECK: Skip if date already has enough articles
                    if len(articles_by_date[art_date]) >= MAX_ARTICLES_PER_DATE:
                        continue
                    seen_hashes.add(ahash)
                    articles_by_date[art_date].append(article_text)
                    total_articles += 1

        time.sleep(ALPHA_SLEEP)

    print(f"   ✅ Alpha Vantage: Added {total_articles} sentiment articles")
    return articles_by_date

# ====================================================
# MODES
# ====================================================

def run_bulk_mode():
    print_header("🏗️  BULK MODE - BUILDING FROM SCRATCH")
    # Include only business dates; weekends will be empty anyway as get_trading_date rolls them to Monday
    articles_by_dict = {
        d.date().isoformat(): [] 
        for d in pd.date_range(START_DATE, END_DATE, freq='B')
    }



    print_section("LAYER 1: GUARDIAN - PRIMARY BACKBONE")
    if GUARDIAN_KEY:
        gua_res = fetch_guardian_bulk(START_DATE, END_DATE)
        for d, arts in gua_res.items():
            if d in articles_by_dict: articles_by_dict[d].extend(arts)
            api_articles_added["GUARDIAN"] += len(arts)

    print_section("LAYER 2: NYT - SECONDARY BACKBONE")
    if NYT_KEY:
        nyt_res = fetch_nyt_bulk(START_DATE, END_DATE)
        for d, arts in nyt_res.items():
            if d in articles_by_dict: articles_by_dict[d].extend(arts)
            api_articles_added["NYT"] += len(arts)

    print_section("LAYER 3: GNEWS - 2020+ SUPPLEMENT")
    if GNEWS_KEY:
        # Update GNews gap filler to respect MAX_ARTICLES_PER_DATE implicitly in its logic
        articles_by_dict = fetch_gnews_for_gaps(articles_by_dict, 100)

    print_section("LAYER 4: FINNHUB - RECENT PATCH")
    if FINNHUB_KEY:
        fin_res = fetch_finnhub_recent()
        for d, arts in fin_res.items():
            if d in articles_by_dict: articles_by_dict[d].extend(arts)
            api_articles_added["FINNHUB"] += len(arts)

    print_section("LAYER 5: ALPHA VANTAGE - SENTIMENT")
    if ALPHAVANTAGE_KEY:
        high_activity_dates = sorted(
            [d for d, arts in articles_by_dict.items() if len(arts) >= 3],
            key=lambda d: len(articles_by_dict[d]), reverse=True
        )
        if high_activity_dates:
            alph_res = fetch_alphavantage_for_top_days(high_activity_dates)
            for d, arts in alph_res.items():
                if d in articles_by_dict: articles_by_dict[d].extend(arts)
                api_articles_added["ALPHAVANTAGE"] += len(arts)

    return articles_by_dict


def run_strategic_backfill(base_df):
    print_header("🧠 STRATEGIC BACKFILL MODE (Gap Filling)")

    articles_by_date = defaultdict(list)
    base_df["Date"] = base_df["Date"].astype(str)

    # 1. Load existing dates from CSV
    for _, row in base_df.iterrows():
        date_str = row["Date"]
        arts_text = row["news_articles"]

        if is_effectively_empty(arts_text):
            articles_by_date[date_str] = []
        else:
            lst = [a.strip() for a in str(arts_text).split("|") if a.strip()]
            articles_by_date[date_str] = lst
            for x in lst: seen_hashes.add(article_hash(x))

    # 2. Auto-Expand new dates if the END_DATE was bumped up in params.yaml (Business days only)
    expected_dates = [
        d.date().isoformat() 
        for d in pd.date_range(START_DATE, END_DATE, freq='B')
    ]


    new_dates_added = 0
    for d in expected_dates:
        if d not in articles_by_date:
            articles_by_date[d] = []
            new_dates_added += 1

    if new_dates_added > 0:
        print_section(f"📅 NEW DATES DETECTED: Injected {new_dates_added} empty rows for backfilling!")

    # 3. Calculate and display coverage stats for business days
    filled_biz_days = [d for d, arts in articles_by_date.items() if len(arts) > 0 and pd.to_datetime(d).dayofweek < 5]
    empty_biz_days  = [d for d, arts in articles_by_date.items() if len(arts) == 0 and pd.to_datetime(d).dayofweek < 5]
    
    print_section("📊 CURRENT DATA COVERAGE (Business Days Only)")
    print(f"   ✅ Dates with News: {len(filled_biz_days):,}")
    print(f"   📭 Empty Dates:     {len(empty_biz_days):,}")
    print(f"   📈 Total Coverage:  {len(filled_biz_days)/(len(filled_biz_days)+len(empty_biz_days))*100:.1f}%")

    backfill_pass = 1
    total_added = 0

    while True:
        print_header(f"🔄 BACKFILL PASS {backfill_pass}")
        added_this_pass = 0

        # 2016-2017 Critical Coverage (Exclude weekends)
        early_dates_empty = [
            d for d, arts in articles_by_date.items() 
            if len(arts) == 0 and d < "2018-01-01" 
            and pd.to_datetime(d).dayofweek < 5
        ]
        if early_dates_empty:
            print_section("BACKFILL 1 & 2: EARLY DATES (GUARDIAN & NYT)")
            if GUARDIAN_KEY:
                pre_cnt = sum(len(x) for x in articles_by_date.values())
                gua_res = fetch_guardian_for_dates(early_dates_empty)
                for d, a in gua_res.items(): articles_by_date[d].extend(a); api_articles_added["GUARDIAN"] += len(a)
                added_this_pass += sum(len(x) for x in articles_by_date.values()) - pre_cnt

            if NYT_KEY:
                pre_cnt = sum(len(x) for x in articles_by_date.values())
                nyt_res = fetch_nyt_for_dates(early_dates_empty)
                for d, a in nyt_res.items(): articles_by_date[d].extend(a); api_articles_added["NYT"] += len(a)
                added_this_pass += sum(len(x) for x in articles_by_date.values()) - pre_cnt

        # Fill generic empty dates (Guardian overall gap fill - Exclude weekends)
        all_empty = [
            d for d, arts in articles_by_date.items() 
            if len(arts) < MAX_ARTICLES_PER_DATE 
            and pd.to_datetime(d).dayofweek < 5
        ]
        if all_empty and GUARDIAN_KEY and api_usage["GUARDIAN"] < LIMITS["GUARDIAN"]:
            print_section("BACKFILL 3: GENERAL GAP FILL (GUARDIAN)")
            pre_cnt = sum(len(x) for x in articles_by_date.values())
            gua_res = fetch_guardian_for_dates(all_empty[:(LIMITS["GUARDIAN"] - api_usage["GUARDIAN"])])
            for d, a in gua_res.items(): articles_by_date[d].extend(a); api_articles_added["GUARDIAN"] += len(a)
            added_this_pass += sum(len(x) for x in articles_by_date.values()) - pre_cnt

        if GNEWS_KEY and api_usage["GNEWS"] < LIMITS["GNEWS"]:
            print_section("BACKFILL 4: GNEWS 2020+ GAP FILL")
            pre_cnt = sum(len(x) for x in articles_by_date.values())
            # fetch_gnews_for_gaps already filters for empty/low volume dates internally usually
            articles_by_date = fetch_gnews_for_gaps(articles_by_date)
            added_this_pass += sum(len(x) for x in articles_by_date.values()) - pre_cnt

        if FINNHUB_KEY and api_usage["FINNHUB"] < LIMITS["FINNHUB"]:
            print_section("BACKFILL 5: FINNHUB RECENT PATCH")
            pre_cnt = sum(len(x) for x in articles_by_date.values())
            fin_res = fetch_finnhub_recent()
            for d, a in fin_res.items():
                if d in articles_by_date: articles_by_date[d].extend(a); api_articles_added["FINNHUB"] += len(a)
            added_this_pass += sum(len(x) for x in articles_by_date.values()) - pre_cnt

        sorted_high_activity = sorted(
            [
                d for d, a in articles_by_date.items() 
                if len(a) >= 3 and len(a) < MAX_ARTICLES_PER_DATE 
                and pd.to_datetime(d).dayofweek < 5
            ],
            key=lambda x: len(articles_by_date[x]), reverse=True
        )
        if ALPHAVANTAGE_KEY and api_usage["ALPHAVANTAGE"] < LIMITS["ALPHAVANTAGE"] and sorted_high_activity:
            print_section("BACKFILL 6: ALPHA VANTAGE SENTIMENT")
            pre_cnt = sum(len(x) for x in articles_by_date.values())
            alph_res = fetch_alphavantage_for_top_days(sorted_high_activity)
            for d, a in alph_res.items(): articles_by_date[d].extend(a); api_articles_added["ALPHAVANTAGE"] += len(a)
            added_this_pass += sum(len(x) for x in articles_by_date.values()) - pre_cnt

        all_quotas_exhausted = all(api_usage[api] >= limit for api, limit in LIMITS.items() if limit > 0)
        if added_this_pass == 0 or all_quotas_exhausted:
            break

        backfill_pass += 1
        total_added += added_this_pass
        time.sleep(2)

    return articles_by_date

# ====================================================
# MAIN
# ====================================================

def run():
    print_header(f"🧠 {COMPANY.upper()} ({TICKER}) - STRATEGIC NEWS SCRAPER")
    connectivity = check_api_connectivity()
    print_quota_status()

    print_section("🔍 MODE DETECTION")
    mode = None
    base_df = None
    if os.path.exists(OUTPUT_FILE):
        try:
            base_df = pd.read_csv(OUTPUT_FILE)
            mode = "backfill"
            print(f"   → STRATEGIC BACKFILL MODE (Resuming from {OUTPUT_FILE})")
        except: pass
    if not mode and os.path.exists(INPUT_DATASET):
        try:
            base_df = pd.read_csv(INPUT_DATASET)
            mode = "backfill"
            print(f"   → STRATEGIC BACKFILL MODE (Using {INPUT_DATASET})")
        except: pass
    if not mode:
        print("   → BULK MODE")
        mode = "bulk"

    if mode == "bulk":
        articles_by_dict = run_bulk_mode()
    else:
        articles_by_dict = run_strategic_backfill(base_df)

    print_section("💾 SAVING OUTPUT")
    rows = [{"Date": d, "news_articles": " | ".join(a) if a else ""} for d, a in sorted(articles_by_dict.items())]
    final_df = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"   ✅ Saved: {OUTPUT_FILE} ({len(final_df)} rows)")

    execution_time = time.time() - start_time
    print_header("✅ COLLECTION COMPLETE")
    for api_name in ["GUARDIAN", "NYT", "GNEWS", "FINNHUB", "ALPHAVANTAGE"]:
        if api_articles_added[api_name] > 0:
            print(f"   {api_name:12s}: {api_articles_added[api_name]:>6,} articles")

    final_with_news = sum(1 for _, row in final_df.iterrows() if not is_effectively_empty(row["news_articles"]))
    print(f"\n📈 COVERAGE: Dates with news: {final_with_news:,}/{len(final_df):,} ({final_with_news/len(final_df)*100:.1f}%)")
    print(f"⏱️  TIME: {int(execution_time//60)}m {int(execution_time%60)}s")

if __name__ == "__main__":
    run()