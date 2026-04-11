"""
scripts/ingest_weather.py — Fetch weather at top 20 ports via OpenWeatherMap
Run by GitHub Actions daily at 20:00 UTC (weekdays)
"""
import os, time, json, requests, pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery

WEATHER_API_KEY = os.environ['WEATHER_API_KEY']
BQ_PROJECT      = os.environ['BQ_PROJECT']
BQ_DATASET      = os.environ['BQ_DATASET']

os.makedirs('data/raw',   exist_ok=True)
os.makedirs('data/clean', exist_ok=True)

TOP_PORTS = [
    {'name':'Shanghai','country':'CN','lat':31.2304,'lon':121.4737,'rank':1},
    {'name':'Singapore','country':'SG','lat':1.2966,'lon':103.8006,'rank':2},
    {'name':'Ningbo-Zhoushan','country':'CN','lat':29.8683,'lon':121.5440,'rank':3},
    {'name':'Shenzhen','country':'CN','lat':22.5431,'lon':114.0579,'rank':4},
    {'name':'Guangzhou','country':'CN','lat':23.1291,'lon':113.2644,'rank':5},
    {'name':'Busan','country':'KR','lat':35.1796,'lon':129.0756,'rank':6},
    {'name':'Tianjin','country':'CN','lat':39.3434,'lon':117.3616,'rank':7},
    {'name':'Hong Kong','country':'HK','lat':22.3193,'lon':114.1694,'rank':8},
    {'name':'Rotterdam','country':'NL','lat':51.9225,'lon':4.4792,'rank':9},
    {'name':'Dubai','country':'AE','lat':24.9965,'lon':55.0272,'rank':10},
    {'name':'Port Klang','country':'MY','lat':3.0000,'lon':101.4000,'rank':11},
    {'name':'Antwerp','country':'BE','lat':51.2194,'lon':4.4025,'rank':12},
    {'name':'Xiamen','country':'CN','lat':24.4798,'lon':118.0894,'rank':13},
    {'name':'Los Angeles','country':'US','lat':33.7290,'lon':-118.2620,'rank':14},
    {'name':'Hamburg','country':'DE','lat':53.5753,'lon':10.0153,'rank':15},
    {'name':'Long Beach','country':'US','lat':33.7548,'lon':-118.2164,'rank':16},
    {'name':'Tanjung Pelepas','country':'MY','lat':1.3634,'lon':103.5521,'rank':17},
    {'name':'Kaohsiung','country':'TW','lat':22.6273,'lon':120.3014,'rank':18},
    {'name':'Dalian','country':'CN','lat':38.9140,'lon':121.6147,'rank':19},
    {'name':'New York','country':'US','lat':40.6943,'lon':-74.1239,'rank':20},
]

def fetch_port(port, api_key, retries=3):
    url = (f'https://api.openweathermap.org/data/2.5/weather'
           f'?lat={port["lat"]}&lon={port["lon"]}&appid={api_key}&units=metric')
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                d = r.json()
                ws = d['wind']['speed']
                bf = (0 if ws<0.3 else 1 if ws<1.6 else 2 if ws<3.4 else
                      3 if ws<5.5 else 4 if ws<8.0 else 5 if ws<10.8 else
                      6 if ws<13.9 else 7 if ws<17.2 else 8 if ws<20.8 else
                      9 if ws<24.5 else 10 if ws<28.5 else 11 if ws<32.7 else 12)
                return {
                    'port_name':       port['name'],
                    'country_iso':     port['country'],
                    'port_rank':       port['rank'],
                    'lat':             port['lat'],
                    'lon':             port['lon'],
                    'fetch_date':      datetime.now(timezone.utc).strftime('%Y-%m-%d'),
                    'fetched_at':      datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M'),
                    'temp_c':          d['main']['temp'],
                    'humidity_pct':    d['main']['humidity'],
                    'pressure_hpa':    d['main']['pressure'],
                    'wind_speed_ms':   ws,
                    'wind_gust_ms':    d['wind'].get('gust', ws),
                    'wind_deg':        d['wind'].get('deg', None),
                    'visibility_m':    d.get('visibility', 10000),
                    'weather_main':    d['weather'][0]['main'],
                    'weather_desc':    d['weather'][0]['description'],
                    'cloudiness_pct':  d['clouds']['all'],
                    'rain_1h_mm':      d.get('rain', {}).get('1h', 0),
                    'snow_1h_mm':      d.get('snow', {}).get('1h', 0),
                    'beaufort_number': bf,
                    'port_risk_flag':  bf >= 7,
                    'low_visibility':  d.get('visibility', 10000) < 1000,
                    'source':          'openweathermap',
                    'data_type':       'port_weather',
                    'granularity':     'daily',
                }
        except Exception as e:
            print(f'Attempt {attempt+1} for {port["name"]}: {e}')
            if attempt < retries-1: time.sleep(2)
    return None

if __name__ == '__main__':
    records = []
    for port in TOP_PORTS:
        r = fetch_port(port, WEATHER_API_KEY)
        if r:
            records.append(r)
            print(f"  {port['name']}: {r['temp_c']}°C, wind {r['wind_speed_ms']} m/s")
        time.sleep(0.5)

    with open('data/raw/port_weather_raw.json', 'w') as f:
        json.dump(records, f, indent=2)

    df = pd.DataFrame(records)
    df.to_csv('data/clean/port_weather_clean.csv', index=False)
    print(f'Saved {len(df)} port records')

    client   = bigquery.Client(project=BQ_PROJECT)
    table_id = f'{BQ_PROJECT}.{BQ_DATASET}.port_weather'
    job_cfg  = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_cfg)
    job.result()
    print(f'Uploaded {len(df)} rows → {table_id}')
