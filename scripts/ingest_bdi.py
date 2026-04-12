"""
scripts/ingest_bdi.py
Fetch the latest Baltic Dry Index (BDI) daily data.
No API key required.

Sources tried in order:
  1. macrotrends.net  — BDI embedded as JSON in page HTML (no key, no login)
  2. investing.com    — historical data endpoint (what your notebook uses)
  3. Manual CSV       — place a downloaded investing.com CSV at data/raw/bdi_manual.csv

MANUAL UPDATE (when automated fetch fails on weekends/holidays):
  1. Go to: https://www.investing.com/indices/baltic-dry-historical-data
  2. Click "Download Data" (top-right of the chart)
  3. Save the file as: data/raw/bdi_manual.csv
  4. Run: python scripts/ingest_bdi.py
  The script picks it up automatically.

How it works:
  1. Load existing data/clean/bdi_clean.csv (full history from your notebook)
  2. Find the last date already in the file
  3. Try to fetch new rows from web sources
  4. OR read from data/raw/bdi_manual.csv if present
  5. Append + recompute ALL derived columns on full dataset
  6. Save updated bdi_clean.csv + upload to BigQuery bdi_daily (WRITE_TRUNCATE)

Runs BEFORE ingest_weather.py and update_combined.py in GitHub Actions.
Pipeline never breaks — if no new data is found, existing data is re-uploaded.
"""

import os
import io
import re
import json
import time
import argparse
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
MANUAL_CSV = f"{RAW_DIR}/bdi_manual.csv"

os.makedirs(CLEAN_DIR, exist_ok=True)
os.makedirs(RAW_DIR,   exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
}


# ── SOURCE 1: macrotrends.net ─────────────────────────────────────────────────
def fetch_macrotrends():
    """
    Fetch BDI from macrotrends.net — data is embedded as JSON in the page HTML.
    URL: https://www.macrotrends.net/1660/baltic-dry-index-historical-chart
    Returns cleaned DataFrame or None.
    """
    url = "https://www.macrotrends.net/1660/baltic-dry-index-historical-chart"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        print(f"  macrotrends: HTTP {r.status_code}, {len(r.text):,} chars")

        if r.status_code != 200:
            print(f"  macrotrends: non-200 response")
            return None

        # macrotrends embeds data in JavaScript variables
        # Try several known patterns
        patterns = [
            r'var\s+rawData\s*=\s*(\[.*?\])\s*;',
            r'"data"\s*:\s*(\[\s*\{.*?\}\s*\])',
            r'chartData\s*=\s*(\[.*?\])\s*;',
            r'var\s+data\s*=\s*(\[.*?\])\s*;',
        ]

        raw_json = None
        for pat in patterns:
            match = re.search(pat, r.text, re.DOTALL)
            if match:
                try:
                    raw_json = json.loads(match.group(1))
                    print(f"  macrotrends: found data with pattern {pat[:30]}...")
                    break
                except json.JSONDecodeError:
                    continue

        if raw_json is None:
            # Try to extract any array of date/value objects
            match = re.search(
                r'\[\s*\{\s*"date"\s*:\s*"(\d{4}-\d{2}-\d{2})"',
                r.text
            )
            if match:
                # Found date format — extract the full array
                start = r.text.rfind('[', 0, match.start())
                if start != -1:
                    try:
                        raw_json = json.loads(r.text[start:r.text.index(']', match.end())+1])
                    except Exception:
                        pass

        if not raw_json:
            print("  macrotrends: could not extract data from page HTML")
            print(f"  Page title: {re.search(r'<title>(.*?)</title>', r.text).group(1) if re.search(r'<title>(.*?)</title>', r.text) else 'N/A'}")
            return None

        # Parse whichever key has the close/value price
        records = []
        for item in raw_json:
            if not isinstance(item, dict):
                continue
            date_val  = item.get("date") or item.get("Date")
            close_val = (item.get("close") or item.get("Close")
                         or item.get("value") or item.get("price"))
            if date_val and close_val is not None:
                records.append({
                    "date":      date_val,
                    "bdi_value": close_val,
                    "bdi_open":  item.get("open")   or item.get("Open"),
                    "bdi_high":  item.get("high")   or item.get("High"),
                    "bdi_low":   item.get("low")    or item.get("Low"),
                    "bdi_volume": 0,
                })

        if not records:
            print("  macrotrends: parsed JSON but found no date/value rows")
            return None

        df = pd.DataFrame(records)
        df["date"]      = pd.to_datetime(df["date"], errors="coerce")
        df["bdi_value"] = pd.to_numeric(df["bdi_value"], errors="coerce")
        for col in ["bdi_open","bdi_high","bdi_low"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").replace(0, np.nan)
        df["bdi_change_pct"] = np.nan
        df = df[df["bdi_value"].notna() & df["date"].notna()].copy()
        df.sort_values("date", inplace=True)
        df.reset_index(drop=True, inplace=True)

        if df.empty:
            print("  macrotrends: no valid rows after cleaning")
            return None

        print(f"  macrotrends: ✓ {len(df):,} rows "
              f"({df['date'].min().date()} → {df['date'].max().date()})")
        return df[["date","bdi_value","bdi_open","bdi_high",
                   "bdi_low","bdi_volume","bdi_change_pct"]]

    except Exception as e:
        print(f"  macrotrends: {e}")
        return None


# ── SOURCE 2: investing.com ───────────────────────────────────────────────────
def fetch_investing(start_date_str=None, end_date_str=None):
    """
    Fetch BDI from investing.com historical data endpoint.
    This is what your notebook uses (manually downloaded CSV).
    Returns cleaned DataFrame or None.
    """
    try:
        # First get a session cookie
        session = requests.Session()
        session.headers.update(HEADERS)
        session.get("https://www.investing.com/indices/baltic-dry-historical-data",
                    timeout=15)
        time.sleep(1)

        # Format dates MM/DD/YYYY
        if start_date_str:
            st = datetime.strptime(start_date_str, "%Y-%m-%d").strftime("%m/%d/%Y")
        else:
            st = "01/01/2015"
        if end_date_str:
            en = datetime.strptime(end_date_str, "%Y-%m-%d").strftime("%m/%d/%Y")
        else:
            en = datetime.now().strftime("%m/%d/%Y")

        post_data = {
            "curr_id":  "172",
            "smlID":    "111177",
            "header":   "BDI Historical Data",
            "st_date":  st,
            "end_date": en,
            "action":   "historical_data",
        }
        post_headers = {
            **HEADERS,
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.investing.com/indices/baltic-dry-historical-data",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "text/plain, */*; q=0.01",
        }

        r = session.post(
            "https://www.investing.com/instruments/HistoricalDataAjax",
            data=post_data, headers=post_headers, timeout=20,
        )
        print(f"  investing.com: HTTP {r.status_code}, {len(r.text)} chars")

        if r.status_code != 200 or len(r.text) < 100:
            print("  investing.com: blocked or empty response")
            return None

        # Response is HTML table — parse with pandas
        tables = pd.read_html(io.StringIO(r.text))
        if not tables:
            print("  investing.com: no HTML table found in response")
            return None

        df = tables[0]
        print(f"  investing.com: columns = {df.columns.tolist()}")

        # Rename to our schema
        col_map = {}
        for col in df.columns:
            lc = str(col).lower()
            if "price" in lc or "close" in lc:   col_map[col] = "bdi_value"
            elif "open"  in lc:                   col_map[col] = "bdi_open"
            elif "high"  in lc:                   col_map[col] = "bdi_high"
            elif "low"   in lc:                   col_map[col] = "bdi_low"
            elif "vol"   in lc:                   col_map[col] = "bdi_volume"
            elif "date"  in lc:                   col_map[col] = "date"
            elif "change" in lc:                  col_map[col] = "bdi_change_pct"
        df.rename(columns=col_map, inplace=True)

        df["date"]      = pd.to_datetime(df.get("date",""), errors="coerce")
        df["bdi_value"] = pd.to_numeric(
            df.get("bdi_value","").astype(str).str.replace(",","",regex=False),
            errors="coerce")
        for col in ["bdi_open","bdi_high","bdi_low"]:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",","",regex=False),
                    errors="coerce").replace(0, np.nan)
        if "bdi_volume" not in df.columns:
            df["bdi_volume"] = 0
        if "bdi_change_pct" in df.columns:
            df["bdi_change_pct"] = pd.to_numeric(
                df["bdi_change_pct"].astype(str).str.replace("%","",regex=False),
                errors="coerce")
        else:
            df["bdi_change_pct"] = np.nan

        df = df[df["bdi_value"].notna() & df["date"].notna()].copy()
        df.sort_values("date", inplace=True)
        df.reset_index(drop=True, inplace=True)

        if df.empty:
            print("  investing.com: no valid rows after cleaning")
            return None

        print(f"  investing.com: ✓ {len(df):,} rows "
              f"({df['date'].min().date()} → {df['date'].max().date()})")
        return df[["date","bdi_value","bdi_open","bdi_high",
                   "bdi_low","bdi_volume","bdi_change_pct"]]

    except Exception as e:
        print(f"  investing.com: {e}")
        return None


# ── SOURCE 3: manual CSV (investing.com download) ────────────────────────────
def read_manual_csv():
    """
    Read a manually downloaded investing.com CSV from data/raw/bdi_manual.csv.
    Matches the exact investing.com CSV format (Date, Price, Open, High, Low, Change %).
    After reading, renames the file so it is not read again next run.
    """
    if not os.path.exists(MANUAL_CSV):
        return None

    try:
        raw = pd.read_csv(MANUAL_CSV)
        print(f"  Manual CSV: found {len(raw)} rows, columns: {raw.columns.tolist()}")

        raw.rename(columns={
            "Date":     "date",
            "Price":    "bdi_value",
            "Open":     "bdi_open",
            "High":     "bdi_high",
            "Low":      "bdi_low",
            "Change %": "bdi_change_pct",
            "Vol.":     "bdi_volume",
        }, inplace=True)

        raw["date"] = pd.to_datetime(raw["date"], errors="coerce")
        for col in ["bdi_value","bdi_open","bdi_high","bdi_low"]:
            if col in raw.columns:
                raw[col] = pd.to_numeric(
                    raw[col].astype(str).str.replace(",","",regex=False),
                    errors="coerce")
                raw[col] = raw[col].replace(0, np.nan)
        if "bdi_change_pct" in raw.columns:
            raw["bdi_change_pct"] = pd.to_numeric(
                raw["bdi_change_pct"].astype(str).str.replace("%","",regex=False),
                errors="coerce")
        else:
            raw["bdi_change_pct"] = np.nan
        if "bdi_volume" not in raw.columns:
            raw["bdi_volume"] = 0

        raw = raw[raw["bdi_value"].notna() & raw["date"].notna()].copy()
        raw.sort_values("date", inplace=True)
        raw.reset_index(drop=True, inplace=True)

        if raw.empty:
            print("  Manual CSV: no valid rows after cleaning")
            return None

        print(f"  Manual CSV: ✓ {len(raw):,} rows "
              f"({raw['date'].min().date()} → {raw['date'].max().date()})")

        # Rename so it's not read again on next automated run
        done_path = MANUAL_CSV.replace(".csv", "_processed.csv")
        os.rename(MANUAL_CSV, done_path)
        print(f"  Manual CSV: renamed → {done_path}")

        return raw[["date","bdi_value","bdi_open","bdi_high",
                    "bdi_low","bdi_volume","bdi_change_pct"]]

    except Exception as e:
        print(f"  Manual CSV error: {e}")
        return None


# ── DERIVED COLUMNS ───────────────────────────────────────────────────────────
def add_derived_columns(df):
    """Mirrors notebook Cell 12. Must run on full history for correct rolling avgs."""
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
    df["source"]           = "bdi_auto"
    df["data_type"]        = "freight_index"
    df["granularity"]      = "daily"

    return df


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    now_str   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    today_dt  = datetime.now(timezone.utc).date()

    print("=" * 60)
    print(f"ingest_bdi.py  —  {now_str}")
    print("=" * 60)

    # ── 1. Load existing history ──────────────────────────────────────────────
    print("\n[1/4] Loading existing BDI history...")
    if os.path.exists(CLEAN_PATH):
        existing  = pd.read_csv(CLEAN_PATH, low_memory=False)
        existing["date"] = pd.to_datetime(existing["date"], errors="coerce")
        existing  = existing[existing["date"].notna()].copy()
        last_date = existing["date"].max().date()
        print(f"  Loaded {len(existing):,} rows | last date: {last_date}")
    else:
        existing  = pd.DataFrame()
        last_date = None
        print("  bdi_clean.csv not found — will fetch full history")

    # ── 2. Fetch new rows ─────────────────────────────────────────────────────
    print("\n[2/4] Fetching new BDI data...")

    start_str = (
        (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
        if last_date else None
    )
    new_df   = None
    src_used = None

    if last_date is not None and last_date >= today_dt:
        print("  Already up to date — skipping fetch")
        new_df = pd.DataFrame()
    else:
        label = f"from {start_str}" if start_str else "full history"
        print(f"  Need data: {label} → {today_str}")

        # Check manual CSV first (user-placed file takes priority)
        print("\n  Checking for manual CSV (data/raw/bdi_manual.csv)...")
        manual = read_manual_csv()
        if manual is not None and not manual.empty:
            # Filter to only new rows
            if last_date:
                manual = manual[manual["date"].dt.date > last_date]
            if not manual.empty:
                new_df   = manual
                src_used = "manual CSV (investing.com)"

        if new_df is None:
            print("\n  Trying macrotrends.net (primary automated)...")
            mt = fetch_macrotrends()
            if mt is not None and not mt.empty:
                # Filter to only new rows
                if last_date:
                    mt = mt[mt["date"].dt.date > last_date]
                if not mt.empty:
                    new_df   = mt
                    src_used = "macrotrends.net"

        if new_df is None:
            print("\n  Trying investing.com (fallback)...")
            inv = fetch_investing(start_str, today_str)
            if inv is not None and not inv.empty:
                new_df   = inv
                src_used = "investing.com"

        if new_df is None or (hasattr(new_df, 'empty') and new_df.empty):
            print("\n  ✗ No new data found from any source.")
            print("  Normal on weekends / market holidays.")
            print("  For manual update:")
            print("    1. Download BDI CSV from investing.com:")
            print("       https://www.investing.com/indices/baltic-dry-historical-data")
            print("    2. Save as: data/raw/bdi_manual.csv")
            print("    3. Re-run: python scripts/ingest_bdi.py")
            new_df = pd.DataFrame()

    # ── 3. Merge and recompute ────────────────────────────────────────────────
    print("\n[3/4] Merging and recomputing derived columns on full dataset...")

    BASE_COLS = ["date","bdi_value","bdi_open","bdi_high",
                 "bdi_low","bdi_volume","bdi_change_pct"]

    if not existing.empty and new_df is not None and not new_df.empty:
        exist_base = existing[[c for c in BASE_COLS if c in existing.columns]].copy()
        new_base   = new_df[[c   for c in BASE_COLS if c in new_df.columns]].copy()
        merged = (
            pd.concat([exist_base, new_base], ignore_index=True)
            .drop_duplicates("date")
            .sort_values("date")
        )
        print(f"  Merged: {len(merged):,} total rows ({len(new_df)} new | source: {src_used})")
    elif existing.empty and new_df is not None and not new_df.empty:
        merged = new_df[[c for c in BASE_COLS if c in new_df.columns]].copy()
        print(f"  Full history: {len(merged):,} rows (source: {src_used})")
    else:
        merged = existing[[c for c in BASE_COLS if c in existing.columns]].copy()
        print(f"  No new rows — recomputing on {len(merged):,} existing rows")

    full = add_derived_columns(merged)

    errors = []
    if full["bdi_value"].isnull().sum() > 0: errors.append("FAIL: nulls in bdi_value")
    if (full["bdi_value"] < 0).any():        errors.append("FAIL: negative BDI value")
    if full.duplicated("date").sum() > 0:    errors.append("FAIL: duplicate dates")
    if errors:
        for e in errors: print(f"  {e}")
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
        raise

    print("\n" + "=" * 60)
    latest    = full.iloc[-1]
    prev      = full.iloc[-6] if len(full) > 6 else full.iloc[0]
    change_5d = float(latest["bdi_value"]) - float(prev["bdi_value"])
    signal    = ("SPIKE" if latest["is_spike"] else "DROP" if latest["is_drop"] else "Normal")
    print(f"Source:       {src_used or 'none (existing data re-uploaded)'}")
    print(f"Latest BDI:   {latest['bdi_value']:.0f}  ({latest['date']})")
    print(f"5-day change: {change_5d:+.0f}")
    print(f"Today signal: {signal}")
    print(f"Total rows:   {len(full):,}")
    print("Done.")
