"""
scripts/ingest_bdi.py
Fetch the latest Baltic Dry Index (BDI) daily data from Stooq.com.
No API key required — Stooq provides free public CSV downloads.

How it works:
  1. Load existing data/clean/bdi_clean.csv (full history from your notebook)
  2. Find the last date already in the file
  3. Fetch only the new rows from Stooq (last_date+1 to today)
  4. Append new rows, then recompute ALL derived columns on the full dataset
     (rolling averages must be recalculated on full history to be correct)
  5. Save updated bdi_clean.csv
  6. Upload full dataset to BigQuery bdi_daily (WRITE_TRUNCATE)

Scheduled: GitHub Actions daily 20:00 UTC weekdays.
Runs BEFORE ingest_weather.py and update_combined.py so analytical
tables always get fresh BDI data on the same run.

First run / recovery: if bdi_clean.csv is missing, fetches all available history.
"""

import os
import io
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery

BQ_PROJECT = os.environ["BQ_PROJECT"]
BQ_DATASET = os.environ["BQ_DATASET"]
CLEAN_DIR  = "data/clean"
CLEAN_PATH = f"{CLEAN_DIR}/bdi_clean.csv"
RAW_DIR    = "data/raw"
RAW_PATH   = f"{RAW_DIR}/bdi_raw_latest.csv"

os.makedirs(CLEAN_DIR, exist_ok=True)
os.makedirs(RAW_DIR,   exist_ok=True)

STOOQ_SYMBOL = "bdiy.uk"
STOOQ_BASE   = "https://stooq.com/q/d/l/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}


def fetch_stooq(start_date=None, end_date=None, retries=3):
    """
    Fetch BDI from Stooq.com.
    start_date / end_date: 'YYYYMMDD' strings (optional).
    Returns raw DataFrame with columns: Date, Open, High, Low, Close, Volume
    """
    params = {"s": STOOQ_SYMBOL, "i": "d"}
    if start_date:
        params["d1"] = start_date
    if end_date:
        params["d2"] = end_date

    for attempt in range(retries):
        try:
            r = requests.get(STOOQ_BASE, params=params, headers=HEADERS, timeout=20)
            if r.status_code == 200 and "Date" in r.text:
                df = pd.read_csv(io.StringIO(r.text))
                if not df.empty and "Date" in df.columns:
                    return df
                print(f"  Stooq returned empty data (attempt {attempt + 1})")
            else:
                print(f"  Stooq HTTP {r.status_code} (attempt {attempt + 1})")
        except Exception as e:
            print(f"  Stooq fetch error (attempt {attempt + 1}): {e}")
        if attempt < retries - 1:
            time.sleep(3)
    return None


def clean_stooq(raw):
    """
    Clean Stooq CSV into the same schema as bdi_clean.csv.
    Mirrors the column renaming and numeric cleaning in notebook 02.
    Stooq columns: Date, Open, High, Low, Close, Volume
    """
    df = raw.copy()
    df.rename(columns={
        "Date":   "date",
        "Open":   "bdi_open",
        "High":   "bdi_high",
        "Low":    "bdi_low",
        "Close":  "bdi_value",
        "Volume": "bdi_volume",
    }, inplace=True)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()].copy()

    for col in ["bdi_value", "bdi_open", "bdi_high", "bdi_low"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", "", regex=False),
                errors="coerce",
            )
            df[col] = df[col].replace(0, np.nan)

    if "bdi_volume" in df.columns:
        df["bdi_volume"] = pd.to_numeric(df["bdi_volume"], errors="coerce").fillna(0)

    df["bdi_change_pct"] = np.nan  # recomputed in add_derived_columns
    df = df[df["bdi_value"].notna()].copy()
    df.sort_values("date", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def add_derived_columns(df):
    """
    Recompute all derived columns on the FULL dataset — mirrors notebook Cell 12.
    Must always run on full history so rolling windows are correct.
    """
    df = df.sort_values("date").copy()

    df["daily_change_pct"] = df["bdi_value"].pct_change().mul(100).round(4)
    df["bdi_change_pct"]   = df["daily_change_pct"]

    df["rolling_7d_avg"]   = df["bdi_value"].rolling(7,  min_periods=1).mean().round(2)
    df["rolling_30d_avg"]  = df["bdi_value"].rolling(30, min_periods=1).mean().round(2)
    df["rolling_90d_avg"]  = df["bdi_value"].rolling(90, min_periods=1).mean().round(2)

    overall_mean           = df["bdi_value"].mean()
    df["above_long_avg"]   = df["bdi_value"] > overall_mean

    df["is_spike"]         = df["daily_change_pct"] >  5.0
    df["is_drop"]          = df["daily_change_pct"] < -5.0

    std                    = df["bdi_value"].std()
    df["is_outlier"]       = (df["bdi_value"] - overall_mean).abs() > 3 * std

    df["year"]             = df["date"].dt.year
    df["month"]            = df["date"].dt.month
    df["quarter"]          = df["date"].dt.quarter
    df["weekday"]          = df["date"].dt.day_name()

    df["source"]           = "stooq_bdi"
    df["data_type"]        = "freight_index"
    df["granularity"]      = "daily"

    return df


if __name__ == "__main__":
    now_str   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    today_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    today_dt  = datetime.now(timezone.utc).date()

    print("=" * 60)
    print(f"ingest_bdi.py  —  {now_str}")
    print("=" * 60)

    # ── 1. Load existing history ──────────────────────────────────────────────
    print("\n[1/4] Loading existing BDI history...")
    if os.path.exists(CLEAN_PATH):
        existing = pd.read_csv(CLEAN_PATH, low_memory=False)
        existing["date"] = pd.to_datetime(existing["date"], errors="coerce")
        existing = existing[existing["date"].notna()].copy()
        last_date = existing["date"].max().date()
        print(f"  Loaded {len(existing):,} rows | last date: {last_date}")
    else:
        existing  = pd.DataFrame()
        last_date = None
        print("  bdi_clean.csv not found — will fetch full history from Stooq")

    # ── 2. Fetch new rows from Stooq ─────────────────────────────────────────
    print("\n[2/4] Fetching new BDI data from Stooq.com...")
    if last_date is not None and last_date >= today_dt:
        print("  Already up to date — no new rows needed")
        new_df = pd.DataFrame()
    else:
        start_str = (
            (last_date + timedelta(days=1)).strftime("%Y%m%d")
            if last_date else None
        )
        label = f"from {start_str}" if start_str else "full history"
        print(f"  Fetching {label} → {today_str} ...")
        raw = fetch_stooq(start_date=start_str, end_date=today_str)

        if raw is None or raw.empty:
            print("  ✗ Stooq returned no data.")
            print("    Likely cause: today is a weekend or holiday — no BDI published.")
            print("    Proceeding with existing data (no new rows appended).")
            new_df = pd.DataFrame()
        else:
            new_df = clean_stooq(raw)
            if new_df.empty:
                print("  No new valid rows after cleaning")
            else:
                print(f"  ✓ Fetched {len(new_df)} new row(s): "
                      f"{new_df['date'].min().date()} → {new_df['date'].max().date()}")
                new_df.to_csv(RAW_PATH, index=False)

    # ── 3. Merge and recompute derived columns ────────────────────────────────
    print("\n[3/4] Merging and recomputing derived columns on full dataset...")

    BASE_COLS = ["date", "bdi_value", "bdi_open", "bdi_high", "bdi_low",
                 "bdi_volume", "bdi_change_pct"]

    if not existing.empty and not new_df.empty:
        exist_base = existing[[c for c in BASE_COLS if c in existing.columns]].copy()
        new_base   = new_df[[c for c in BASE_COLS if c in new_df.columns]].copy()
        merged = (
            pd.concat([exist_base, new_base], ignore_index=True)
            .drop_duplicates("date")
            .sort_values("date")
        )
        print(f"  Merged: {len(merged):,} total rows ({len(new_df)} new)")
    elif existing.empty and not new_df.empty:
        merged = new_df[[c for c in BASE_COLS if c in new_df.columns]].copy()
        print(f"  Full Stooq history: {len(merged):,} rows")
    else:
        merged = existing[[c for c in BASE_COLS if c in existing.columns]].copy()
        print(f"  No new rows — recomputing on {len(merged):,} existing rows")

    full = add_derived_columns(merged)

    # Validation — mirrors notebook Cell 14
    errors = []
    if full["bdi_value"].isnull().sum() > 0: errors.append("FAIL: nulls in bdi_value")
    if (full["bdi_value"] < 0).any():        errors.append("FAIL: negative BDI value")
    if full.duplicated("date").sum() > 0:    errors.append("FAIL: duplicate dates")
    if errors:
        for e in errors:
            print(f"  {e}")
        raise ValueError("BDI validation failed — aborting upload.")

    print(f"  ✓ Validation passed | shape: {full.shape} | "
          f"spikes: {int(full['is_spike'].sum())} | drops: {int(full['is_drop'].sum())}")

    full["date"] = full["date"].dt.strftime("%Y-%m-%d")
    full.to_csv(CLEAN_PATH, index=False)
    print(f"  ✎ Saved → {CLEAN_PATH}")

    # ── 4. Upload to BigQuery ─────────────────────────────────────────────────
    print("\n[4/4] Uploading to BigQuery (bdi_daily, WRITE_TRUNCATE)...")
    try:
        client   = bigquery.Client(project=BQ_PROJECT)
        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.bdi_daily"
        job_cfg  = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )
        job = client.load_table_from_dataframe(full, table_id, job_config=job_cfg)
        job.result()
        tbl = client.get_table(table_id)
        print(f"  ✓ {len(full):,} rows → {table_id} (table total: {tbl.num_rows:,})")
    except Exception as e:
        print(f"  ✗ BigQuery upload failed: {e}")
        print("    bdi_clean.csv updated locally — upload manually if needed.")
        raise

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    latest    = full.iloc[-1]
    prev      = full.iloc[-6] if len(full) > 6 else full.iloc[0]
    change_5d = float(latest["bdi_value"]) - float(prev["bdi_value"])
    signal    = ("SPIKE (>+5%)" if latest["is_spike"]
                 else "DROP (>-5%)" if latest["is_drop"]
                 else "Normal")
    print(f"Latest BDI:   {latest['bdi_value']:.0f}  ({latest['date']})")
    print(f"5-day change: {change_5d:+.0f}")
    print(f"Today signal: {signal}")
    print(f"Total rows:   {len(full):,}")
    print("Done.")
