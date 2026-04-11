"""
scripts/update_combined.py
Merge all 4 clean sources into shipping_combined.csv and upload to BigQuery.

Run automatically after ingest_weather.py in GitHub Actions.
Can also be run manually: python scripts/update_combined.py

Sources read from data/clean/:
  - un_comtrade_clean.csv   (committed in repo, static)
  - bdi_clean.csv           (committed in repo, static)
  - aisstream_clean.csv     (committed in repo, static)
  - port_weather_clean.csv  (freshly updated by ingest_weather.py)

Output:
  - data/clean/shipping_combined.csv
  - BigQuery: shipping_data.shipping_combined (WRITE_TRUNCATE)
"""

import os
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

BQ_PROJECT = os.environ.get('BQ_PROJECT', os.environ.get('BIGQUERY_PROJECT', 'your-project-id'))
BQ_DATASET = os.environ.get('BQ_DATASET', os.environ.get('BIGQUERY_DATASET', 'shipping_data'))
CLEAN_DIR  = 'data/clean'

os.makedirs(CLEAN_DIR, exist_ok=True)

print('=' * 55)
print('UPDATE COMBINED — shipping_combined.csv')
print(f'Run time: {datetime.now().strftime("%Y-%m-%d %H:%M")}')
print('=' * 55)


# ── 1. Load all 4 clean sources ───────────────────────────────────────────
SOURCES = {
    'comtrade': f'{CLEAN_DIR}/un_comtrade_clean.csv',
    'bdi':      f'{CLEAN_DIR}/bdi_clean.csv',
    'weather':  f'{CLEAN_DIR}/port_weather_clean.csv',
    'ais':      f'{CLEAN_DIR}/aisstream_clean.csv',
}

frames = []
for name, path in SOURCES.items():
    if os.path.exists(path):
        df = pd.read_csv(path, low_memory=False)
        frames.append(df)
        print(f'  Loaded {name}: {len(df):,} rows x {df.shape[1]} cols')
    else:
        print(f'  SKIP {name}: {path} not found')

if not frames:
    raise RuntimeError('No clean CSV files found. Nothing to combine.')


# ── 2. Add date column where missing ─────────────────────────────────────
def ensure_date(df, name):
    if 'date' not in df.columns:
        if 'year' in df.columns:
            df['date'] = pd.to_datetime(
                df['year'].astype(str) + '-01-01', errors='coerce'
            ).dt.strftime('%Y-%m-%d')
        elif 'event_time_utc' in df.columns:
            df['date'] = pd.to_datetime(
                df['event_time_utc'], utc=True, errors='coerce'
            ).dt.strftime('%Y-%m-%d')
        elif 'fetch_date' in df.columns:
            df['date'] = df['fetch_date']
        elif 'fetched_at' in df.columns:
            df['date'] = pd.to_datetime(
                df['fetched_at'], errors='coerce'
            ).dt.strftime('%Y-%m-%d')
    else:
        df['date'] = pd.to_datetime(
            df['date'], errors='coerce'
        ).dt.strftime('%Y-%m-%d')
    return df

frames_ready = []
for df in frames:
    src = df['source'].iloc[0] if 'source' in df.columns else 'unknown'
    df = ensure_date(df, src)
    frames_ready.append(df)


# ── 3. UNION ALL ─────────────────────────────────────────────────────────
combined = pd.concat(frames_ready, ignore_index=True, sort=False)
combined['source'] = combined['source'].fillna('unknown')

# sort by source then date
combined.sort_values(['source', 'date'], inplace=True, na_position='last')
combined.reset_index(drop=True, inplace=True)
combined['row_id'] = combined.index + 1

print(f'\nCombined: {combined.shape[0]:,} rows x {combined.shape[1]} cols')
print('Sources:')
print(combined['source'].value_counts().to_string())


# ── 4. Save combined CSV ──────────────────────────────────────────────────
output_path = f'{CLEAN_DIR}/shipping_combined.csv'
combined.to_csv(output_path, index=False)
print(f'\nSaved → {output_path}')


# ── 5. Upload to BigQuery ─────────────────────────────────────────────────
try:
    client   = bigquery.Client(project=BQ_PROJECT)
    table_id = f'{BQ_PROJECT}.{BQ_DATASET}.shipping_combined'
    job_cfg  = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(combined, table_id, job_config=job_cfg)
    job.result()
    tbl = client.get_table(table_id)
    print(f'Uploaded {len(combined):,} rows → {table_id} (total: {tbl.num_rows:,})')
except Exception as e:
    print(f'BigQuery upload failed: {e}')
    print('Combined CSV saved locally — upload manually if needed.')
    raise

print('\nDone.')
