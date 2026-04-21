"""
scripts/ingest_news.py
Fetches latest maritime/shipping news from NewsData.io API and uploads to BigQuery.
Source: newsdata.io — real-time news API
API key stored in GitHub Secret: NEWSDATA_API_KEY
Run: python scripts/ingest_news.py
"""
import os
import sys
import json
import requests
import pandas as pd
from datetime import datetime

# ── CONFIG ─────────────────────────────────────────────────────────────────────
NEWSDATA_API_KEY = os.getenv("NEWSDATA_API_KEY", "")
BQ_PROJECT       = os.getenv("BQ_PROJECT",  "msba305-shipping")
BQ_DATASET       = os.getenv("BQ_DATASET",  "shipping_data")
GCP_KEY          = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "gcp_key.json")
TABLE_NAME       = "shipping_news"

# Search keywords covering all relevant maritime topics
SEARCH_QUERIES = [
    "maritime shipping freight",
    "shipping route disruption",
    "Suez Canal Strait Hormuz",
    "bulk carrier tanker cargo",
    "port congestion shipping delay",
]

NEWSDATA_URL = "https://newsdata.io/api/1/news"

# Geopolitical risk keywords — articles containing these get elevated risk flag
RISK_KEYWORDS = [
    "attack", "strike", "missile", "drone", "blockade", "sanction",
    "conflict", "tension", "war", "military", "reroute", "divert",
    "halt", "closure", "disruption", "crisis", "threat"
]

# Route relevance mapping
ROUTE_KEYWORDS = {
    "Asia-Europe via Suez":     ["suez", "red sea", "bab el-mandeb", "aden", "houthi"],
    "Strait of Hormuz":         ["hormuz", "persian gulf", "arabian gulf", "iran", "gulf"],
    "Strait of Malacca":        ["malacca", "singapore", "indonesia", "strait"],
    "Black Sea":                ["black sea", "ukraine", "russia", "bosphorus", "grain"],
    "Trans-Pacific":            ["pacific", "us west coast", "china", "transpacific"],
    "North Sea / Dover":        ["dover", "north sea", "english channel", "rotterdam"],
}


def fetch_news(api_key: str, query: str, max_results: int = 10) -> list:
    """Fetch news articles for a given query."""
    params = {
        "apikey":   api_key,
        "q":        query,
        "language": "en",
        "category": "business,world",
        "size":     max_results,
    }
    try:
        resp = requests.get(NEWSDATA_URL, params=params, timeout=30)
        if resp.status_code == 422:
            print(f"  ⚠ Query '{query}' — API limit or invalid params, skipping")
            return []
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])
    except Exception as e:
        print(f"  ✗ Query '{query}' failed: {e}")
        return []


def classify_article(title: str, description: str) -> dict:
    """Classify article risk level and route relevance."""
    text = f"{title} {description}".lower()

    # Risk level
    risk_hits = sum(1 for kw in RISK_KEYWORDS if kw in text)
    if risk_hits >= 3:
        risk_level = "HIGH"
        risk_score = min(90, 60 + risk_hits * 5)
    elif risk_hits >= 1:
        risk_level = "MEDIUM"
        risk_score = 40 + risk_hits * 8
    else:
        risk_level = "LOW"
        risk_score = max(5, 15 - risk_hits)

    # Route relevance
    relevant_routes = []
    for route, keywords in ROUTE_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            relevant_routes.append(route)

    return {
        "risk_level":      risk_level,
        "risk_score":      risk_score,
        "relevant_routes": ", ".join(relevant_routes) if relevant_routes else "General",
    }


def build_news_df(articles: list) -> pd.DataFrame:
    """Convert raw API articles to structured DataFrame."""
    rows = []
    seen_titles = set()  # deduplicate

    for art in articles:
        title = (art.get("title") or "").strip()
        if not title or title in seen_titles:
            continue
        seen_titles.add(title)

        description = (art.get("description") or art.get("content") or "")[:500]
        pub_date    = art.get("pubDate") or art.get("publishedAt") or ""
        source_name = art.get("source_id") or art.get("source", {}).get("name", "unknown")
        url         = art.get("link") or art.get("url") or ""

        classification = classify_article(title, description)

        rows.append({
            "pub_date":       pub_date,
            "title":          title[:300],
            "description":    description,
            "source_name":    source_name,
            "url":            url[:500],
            "risk_level":     classification["risk_level"],
            "risk_score":     classification["risk_score"],
            "relevant_routes":classification["relevant_routes"],
            "fetch_date":     datetime.now().strftime("%Y-%m-%d"),
            "source_api":     "newsdata.io",
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["pub_date"] = pd.to_datetime(df["pub_date"], errors="coerce")
    df = df.dropna(subset=["pub_date"])
    df = df.sort_values("pub_date", ascending=False).reset_index(drop=True)
    return df


def upload_to_bigquery(df: pd.DataFrame) -> None:
    """Upload news to BigQuery — WRITE_TRUNCATE (replace daily)."""
    import pathlib
    from google.cloud import bigquery
    from google.oauth2 import service_account
    key_path = pathlib.Path(GCP_KEY)
    if key_path.exists():
        creds  = service_account.Credentials.from_service_account_file(str(key_path))
        client = bigquery.Client(project=BQ_PROJECT, credentials=creds)
    else:
        client = bigquery.Client(project=BQ_PROJECT)
    table_ref  = f"{BQ_PROJECT}.{BQ_DATASET}.{TABLE_NAME}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"✓ Uploaded {len(df):,} news articles to {table_ref}")


def main():
    print("\n=== SHIPPING NEWS INGESTION (NewsData.io) ===")

    if not NEWSDATA_API_KEY:
        print("⚠ NEWSDATA_API_KEY not set — skipping news ingestion (pipeline continues)")
        sys.exit(0)  # exit 0 so GitHub Actions continues to next step

    all_articles = []
    for query in SEARCH_QUERIES:
        print(f"  Fetching: '{query}'...")
        articles = fetch_news(NEWSDATA_API_KEY, query, max_results=5)
        all_articles.extend(articles)
        print(f"  → {len(articles)} articles")

    df = build_news_df(all_articles)

    if df.empty:
        print("⚠ No news articles fetched — skipping upload (non-fatal)")
        sys.exit(0)

    print(f"\n✓ {len(df):,} unique articles after deduplication")
    high_risk = df[df["risk_level"] == "HIGH"]
    if not high_risk.empty:
        print(f"⚠ {len(high_risk)} HIGH RISK articles detected:")
        for _, row in high_risk.head(3).iterrows():
            print(f"   [{row['risk_score']}/100] {row['title'][:80]}")

    # Save local CSV fallback
    import pathlib
    out_path = pathlib.Path(__file__).resolve().parents[1] / "data" / "clean" / "shipping_news.csv"
    df.to_csv(out_path, index=False)
    print(f"✓ Saved CSV: {out_path}")

    upload_to_bigquery(df)
    print("=== NEWS INGESTION COMPLETE ===\n")


if __name__ == "__main__":
    main()
