"""
scripts/ingest_news.py
Fetches maritime shipping news from NewsAPI.org and uploads to BigQuery.
Source: newsapi.org
API key stored in GitHub Secret: NEWSDATA_API_KEY
Run: python scripts/ingest_news.py
"""
import os
import sys
import requests
import pandas as pd
from datetime import datetime, timedelta

NEWSDATA_API_KEY = os.getenv("NEWSDATA_API_KEY", "")
BQ_PROJECT       = os.getenv("BQ_PROJECT",  "msba305-shipping")
BQ_DATASET       = os.getenv("BQ_DATASET",  "shipping_data")
GCP_KEY          = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "gcp_key.json")
TABLE_NAME       = "shipping_news"

# NewsAPI.org endpoint
NEWSAPI_URL = "https://newsapi.org/v2/everything"

SEARCH_QUERIES = [
    "maritime shipping freight",
    "Suez Canal shipping",
    "Strait Hormuz tanker",
    "port congestion cargo delay",
    "shipping route disruption",
]

RISK_KEYWORDS = [
    "attack","strike","missile","drone","blockade","sanction",
    "conflict","tension","war","military","reroute","divert",
    "halt","closure","disruption","crisis","threat"
]

ROUTE_KEYWORDS = {
    "Asia-Europe via Suez":  ["suez","red sea","bab el-mandeb","aden","houthi"],
    "Strait of Hormuz":      ["hormuz","persian gulf","arabian gulf","iran","gulf"],
    "Strait of Malacca":     ["malacca","singapore","indonesia"],
    "Black Sea":             ["black sea","ukraine","russia","bosphorus","grain"],
    "Trans-Pacific":         ["pacific","us west coast","china","transpacific"],
    "North Sea / Dover":     ["dover","north sea","english channel","rotterdam"],
}


def fetch_news(api_key: str, query: str) -> list:
    from_date = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    params = {
        "q":        query,
        "language": "en",
        "sortBy":   "publishedAt",
        "pageSize": 5,
        "from":     from_date,
        "apiKey":   api_key,
    }
    try:
        resp = requests.get(NEWSAPI_URL, params=params, timeout=30)
        if resp.status_code == 401:
            print(f"  ✗ 401 Unauthorized — check NEWSDATA_API_KEY secret")
            return []
        if resp.status_code == 426:
            print(f"  ⚠ 426 — NewsAPI free tier limitation, skipping")
            return []
        resp.raise_for_status()
        data = resp.json()
        return data.get("articles", [])
    except Exception as e:
        print(f"  ✗ Query '{query}' failed: {e}")
        return []


def classify(title: str, description: str) -> dict:
    text = f"{title} {description}".lower()
    hits = sum(1 for kw in RISK_KEYWORDS if kw in text)
    if hits >= 3:   risk_level, risk_score = "HIGH",   min(90, 60 + hits*5)
    elif hits >= 1: risk_level, risk_score = "MEDIUM", 40 + hits*8
    else:           risk_level, risk_score = "LOW",    10

    routes = [r for r, kws in ROUTE_KEYWORDS.items() if any(k in text for k in kws)]
    return {
        "risk_level":      risk_level,
        "risk_score":      risk_score,
        "relevant_routes": ", ".join(routes) if routes else "General",
    }


def build_df(articles: list) -> pd.DataFrame:
    rows = []
    seen = set()
    for art in articles:
        title = (art.get("title") or "").strip()
        if not title or title in seen or title == "[Removed]":
            continue
        seen.add(title)
        desc = (art.get("description") or "")[:500]
        cl   = classify(title, desc)
        rows.append({
            "pub_date":        art.get("publishedAt",""),
            "title":           title[:300],
            "description":     desc,
            "source_name":     (art.get("source") or {}).get("name","unknown"),
            "url":             (art.get("url") or "")[:500],
            "risk_level":      cl["risk_level"],
            "risk_score":      cl["risk_score"],
            "relevant_routes": cl["relevant_routes"],
            "fetch_date":      datetime.now().strftime("%Y-%m-%d"),
            "source_api":      "newsapi.org",
        })
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["pub_date"] = pd.to_datetime(df["pub_date"], errors="coerce")
    return df.dropna(subset=["pub_date"]).sort_values("pub_date", ascending=False).reset_index(drop=True)


def upload(df: pd.DataFrame) -> None:
    import pathlib
    from google.cloud import bigquery
    from google.oauth2 import service_account
    kp = pathlib.Path(GCP_KEY)
    if kp.exists():
        creds  = service_account.Credentials.from_service_account_file(str(kp))
        client = bigquery.Client(project=BQ_PROJECT, credentials=creds)
    else:
        client = bigquery.Client(project=BQ_PROJECT)
    jc = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    client.load_table_from_dataframe(df, f"{BQ_PROJECT}.{BQ_DATASET}.{TABLE_NAME}", job_config=jc).result()
    print(f"✓ Uploaded {len(df):,} articles to BigQuery")


def main():
    print("\n=== SHIPPING NEWS INGESTION (NewsAPI.org) ===")

    if not NEWSDATA_API_KEY:
        print("⚠ NEWSDATA_API_KEY not set — skipping (pipeline continues)")
        sys.exit(0)

    all_articles = []
    for q in SEARCH_QUERIES:
        print(f"  Fetching: '{q}'...")
        arts = fetch_news(NEWSDATA_API_KEY, q)
        all_articles.extend(arts)
        print(f"  → {len(arts)} articles")

    df = build_df(all_articles)
    if df.empty:
        print("⚠ No articles fetched — skipping upload (non-fatal)")
        sys.exit(0)

    print(f"✓ {len(df):,} unique articles")
    upload(df)
    print("=== NEWS INGESTION COMPLETE ===\n")


if __name__ == "__main__":
    main()
