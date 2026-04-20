"""
scripts/ingest_fuel.py
Fetches daily Brent crude oil prices from EIA API and uploads to BigQuery.
Source: U.S. Energy Information Administration (EIA) — api.eia.gov
API key stored in GitHub Secret: EIA_API_KEY
Run: python scripts/ingest_fuel.py
"""
import os
import sys
import json
import requests
import pandas as pd
from datetime import datetime, timedelta

# ── CONFIG ─────────────────────────────────────────────────────────────────────
EIA_API_KEY = os.getenv("EIA_API_KEY", "")
BQ_PROJECT  = os.getenv("BQ_PROJECT",  "msba305-shipping")
BQ_DATASET  = os.getenv("BQ_DATASET",  "shipping_data")
GCP_KEY     = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "gcp_key.json")
TABLE_NAME  = "fuel_prices_daily"
DAYS_BACK   = 90  # fetch last 90 days

# EIA series used
# RBRTE = Brent crude spot price (USD/barrel) — published daily by EIA
EIA_BASE_URL = "https://api.eia.gov/v2/petroleum/pri/spt/data/"


def fetch_brent(api_key: str, days: int = 90) -> pd.DataFrame:
    """Fetch Brent crude daily price from EIA API."""
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    params = {
        "api_key":                api_key,
        "frequency":              "daily",
        "data[0]":                "value",
        "facets[series][]":       "RBRTE",   # Brent crude spot price
        "sort[0][column]":        "period",
        "sort[0][direction]":     "asc",
        "start":                  start_date,
        "length":                 days,
    }
    try:
        resp = requests.get(EIA_BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("response", {}).get("data", [])
        if not rows:
            print("⚠ EIA returned no data rows")
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df = df.rename(columns={"period": "date", "value": "brent_usd_per_bbl"})
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["brent_usd_per_bbl"] = pd.to_numeric(df["brent_usd_per_bbl"], errors="coerce")
        df = df.dropna(subset=["date", "brent_usd_per_bbl"])
        return df[["date", "brent_usd_per_bbl"]].sort_values("date")
    except Exception as e:
        print(f"✗ EIA API call failed: {e}")
        return pd.DataFrame()


def enrich_fuel(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns useful for shipping cost analysis."""
    if df.empty:
        return df
    df = df.copy().sort_values("date").reset_index(drop=True)

    # Rolling averages
    df["brent_7d_avg"]  = df["brent_usd_per_bbl"].rolling(7,  min_periods=1).mean().round(2)
    df["brent_30d_avg"] = df["brent_usd_per_bbl"].rolling(30, min_periods=5).mean().round(2)

    # Fuel pressure score: how far today's price is from 30-day avg
    df["fuel_pressure_score"] = (
        (df["brent_usd_per_bbl"] - df["brent_30d_avg"]) / df["brent_30d_avg"] * 100
    ).round(2).fillna(0)

    # Pressure label for dashboard
    df["fuel_signal"] = df["fuel_pressure_score"].apply(
        lambda x: "HIGH"    if x >  10 else
                  "ELEVATED" if x >   3 else
                  "NORMAL"   if x > -3  else
                  "LOW"
    )

    # Estimated VLSFO (Very Low Sulphur Fuel Oil) price proxy
    # VLSFO ≈ Brent × 7.1 (approximate barrel-to-tonne conversion × premium)
    df["vlsfo_est_usd_per_mt"] = (df["brent_usd_per_bbl"] * 7.1).round(2)

    # Date parts for seasonality analysis
    df["year"]    = df["date"].dt.year
    df["month"]   = df["date"].dt.month
    df["weekday"] = df["date"].dt.day_name()

    # Source metadata
    df["fetch_date"] = datetime.now().strftime("%Y-%m-%d")
    df["source"]     = "EIA_RBRTE_API"

    return df


def upload_to_bigquery(df: pd.DataFrame) -> None:
    """Upload fuel data to BigQuery — WRITE_APPEND."""
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
        creds  = service_account.Credentials.from_service_account_file(GCP_KEY)
        client = bigquery.Client(project=BQ_PROJECT, credentials=creds)
        table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{TABLE_NAME}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"✓ Uploaded {len(df):,} rows to {table_ref}")
    except Exception as e:
        print(f"✗ BigQuery upload failed: {e}")
        raise


def main():
    print("\n=== FUEL PRICE INGESTION (EIA) ===")
    print(f"Source: U.S. Energy Information Administration — Brent Crude Spot Price (RBRTE)")

    if not EIA_API_KEY:
        print("✗ EIA_API_KEY not set — check GitHub Secrets")
        sys.exit(1)

    print(f"Fetching last {DAYS_BACK} days of Brent crude prices...")
    df = fetch_brent(EIA_API_KEY, DAYS_BACK)

    if df.empty:
        print("✗ No fuel data fetched — skipping upload")
        sys.exit(0)

    df = enrich_fuel(df)
    print(f"✓ {len(df):,} rows fetched | Latest: {df['date'].max().date()} | "
          f"Price: ${df['brent_usd_per_bbl'].iloc[-1]:.2f}/bbl | "
          f"Signal: {df['fuel_signal'].iloc[-1]}")

    # Save local CSV fallback
    import pathlib
    out_path = pathlib.Path(__file__).resolve().parents[1] / "data" / "clean" / "fuel_prices_daily.csv"
    df.to_csv(out_path, index=False)
    print(f"✓ Saved CSV: {out_path}")

    upload_to_bigquery(df)
    print("=== FUEL INGESTION COMPLETE ===\n")


if __name__ == "__main__":
    main()
