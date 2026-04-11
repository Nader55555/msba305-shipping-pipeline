"""
scripts/ingest_bdi.py — Fetch Baltic Dry Index from Nasdaq Data Link
Run by GitHub Actions daily at 20:00 UTC (weekdays)
"""
import os, time, pandas as pd, nasdaqdatalink
from datetime import datetime
from google.cloud import bigquery

NASDAQ_API_KEY = os.environ['NASDAQ_API_KEY']
BQ_PROJECT     = os.environ['BQ_PROJECT']
BQ_DATASET     = os.environ['BQ_DATASET']

nasdaqdatalink.ApiConfig.api_key = NASDAQ_API_KEY
os.makedirs('data/raw',   exist_ok=True)
os.makedirs('data/clean', exist_ok=True)

def fetch_bdi(retries=3):
    end = datetime.today().strftime('%Y-%m-%d')
    for i in range(retries):
        try:
            df = nasdaqdatalink.get('CHRIS/ICE_BDR1', start_date='2015-01-01', end_date=end)
            print(f'Fetched {len(df):,} BDI rows')
            return df
        except Exception as e:
            print(f'Attempt {i+1} failed: {e}')
            if i < retries-1: time.sleep(5)
    raise RuntimeError('BDI fetch failed after retries')

def clean(df):
    df = df.reset_index()
    df.rename(columns={'Date':'date','Settle':'bdi_value','Open':'bdi_open',
                       'High':'bdi_high','Low':'bdi_low','Volume':'bdi_volume'}, inplace=True)
    keep = [c for c in ['date','bdi_value','bdi_open','bdi_high','bdi_low','bdi_volume'] if c in df.columns]
    df = df[keep].copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df[df['bdi_value'].notna()].sort_values('date')
    df['daily_change_pct'] = df['bdi_value'].pct_change().mul(100).round(4)
    df['rolling_7d_avg']   = df['bdi_value'].rolling(7,  min_periods=1).mean().round(2)
    df['rolling_30d_avg']  = df['bdi_value'].rolling(30, min_periods=1).mean().round(2)
    df['rolling_90d_avg']  = df['bdi_value'].rolling(90, min_periods=1).mean().round(2)
    m = df['bdi_value'].mean()
    s = df['bdi_value'].std()
    df['is_spike']   = df['daily_change_pct'] >  5.0
    df['is_drop']    = df['daily_change_pct'] < -5.0
    df['is_outlier'] = (df['bdi_value'] - m).abs() > 3 * s
    df['year']       = df['date'].dt.year
    df['month']      = df['date'].dt.month
    df['quarter']    = df['date'].dt.quarter
    df['source']     = 'nasdaq_bdi'
    df['data_type']  = 'freight_index'
    df['date']       = df['date'].dt.strftime('%Y-%m-%d')
    return df

if __name__ == '__main__':
    raw   = fetch_bdi()
    raw.to_csv('data/raw/bdi_raw.csv')
    clean_df = clean(raw)
    clean_df.to_csv('data/clean/bdi_clean.csv', index=False)

    client   = bigquery.Client(project=BQ_PROJECT)
    table_id = f'{BQ_PROJECT}.{BQ_DATASET}.bdi_daily'
    job_cfg  = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )
    job = client.load_table_from_dataframe(clean_df, table_id, job_config=job_cfg)
    job.result()
    print(f'Uploaded {len(clean_df):,} rows → {table_id}')
