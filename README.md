# Maritime Shipping Intelligence Pipeline
**MSBA 305 — Data Processing Framework | Spring 2025/2026**
**Instructor:** Dr. Ahmad El-Hajj | **Domain:** Shipping & Supply Chain

---

## 4 Data Sources

| # | Source | Format | Frequency | API Key |
|---|--------|--------|-----------|---------|
| 1 | **UN Comtrade** — bilateral trade flows by commodity (5 HS codes) | CSV (manual download) | Annual | No |
| 2 | **BDI (investing.com)** — Baltic Dry Index freight cost indicator | CSV (manual download, re-run notebook 02) | Monthly or as needed | No |
| 3 | **OpenWeatherMap** — weather at top 20 global ports + 8 critical straits | JSON API | Daily (automated) | Yes — free at openweathermap.org |
| 4 | **AISStream** — vessel positions at 8 straits + 20 ports | WebSocket API | Daily (automated, 4-min collection) | Yes — free at aisstream.io |

---

## Analytical Questions Answered

| # | Question | Tables used |
|---|----------|-------------|
| 1 | Which trade routes have the highest export value? | `trade_flows` |
| 2 | Which months have the highest freight costs historically? | `bdi_daily` |
| 3 | Which countries are net exporters vs importers? | `trade_flows` |
| 4 | Does BDI predict global trade volume shifts? | `analysis_bdi_trade` |
| 5 | When does BDI signal bearish / bullish conditions? | `analysis_bdi_signals` |
| 6 | Which ports face weather disruption + high trade exposure? | `analysis_port_risk_trade` |
| 7 | Which maritime straits are currently at risk? | `analysis_strait_monitor` |
| 8 | Which global trade routes are disrupted today? | `analysis_route_disruption` |
| 9 | How dominant is China in each shipping commodity? | `analysis_china_concentration` |
| 10 | When is freight cheapest — booking calendar? | `analysis_seasonal_freight` |
| 11 | Is vessel traffic at key straits increasing or decreasing? | `analysis_strait_vessel_trend` |
| 12 | Are any vessels behaving anomalously at high-risk straits? | `vessel_movements` |

---

## Pipeline Architecture

```
[UN Comtrade CSV]    ── notebook 01 ──────────────────────────────────────┐
                                                                           │
[BDI investing.com]  ── notebook 02 (manual, commit bdi_clean.csv) ───────┤
                                                                           ├──► update_combined.py
[OpenWeatherMap API] ── ingest_weather.py (daily, GitHub Actions) ────────┤       │
                                                                           │       ▼
[AISStream API]      ── ingest_ais.py (daily, 4-min, GitHub Actions) ─────┘   BigQuery
                                                                                   │
                                                                               Streamlit dashboard
```

**GitHub Actions daily run order (weekdays 20:00 UTC):**
1. `ingest_weather.py` — fetch weather at 20 ports + 8 straits, upload to BigQuery
2. `ingest_ais.py` — 4-minute AIS collection at 8 straits + 20 ports, upload to BigQuery
3. `update_combined.py` — upload bdi_daily + rebuild all 12 analytical tables

**BDI runs manually** — see Setup Step 3.

---

## Repository Structure

```
.
├── .github/workflows/
│   └── daily_update.yml          # Automated daily pipeline
├── notebooks/                    # Run in Google Colab IN ORDER (one-time setup)
│   ├── 01_clean_comtrade.ipynb
│   ├── 02_ingest_clean_bdi_investing.ipynb
│   ├── 03_ingest_clean_weather.ipynb
│   ├── 04_ingest_clean_aisstream.ipynb   ← historical baseline only
│   ├── 05_combine_EDA.ipynb
│   └── 06_bigquery_upload.ipynb
├── scripts/                      # Run by GitHub Actions daily
│   ├── ingest_weather.py         # Port weather + strait conditions
│   ├── ingest_ais.py             # AIS vessel positions (4-min daily collection)
│   └── update_combined.py        # bdi_daily upload + 12 analytical JOIN tables
├── dashboard/
│   └── app.py                    # Streamlit dashboard (8 pages)
├── SQL/
│   └── queries.sql               # 18 analytical queries (simple → complex)
├── data/
│   ├── raw/                      # Never commit — original downloaded files
│   └── clean/                    # Output of cleaning notebooks (committed)
├── requirements.txt
└── README.md
```

---

## Setup (Step by Step)

### Step 1 — Get your API keys

**OpenWeatherMap (Port weather + straits):**
1. Go to https://openweathermap.org/api
2. Register → Account → API Keys → copy (takes ~10 min to activate)

**AISStream (Daily vessel tracking):**
1. Go to https://aisstream.io
2. Sign in via GitHub → API Keys → Generate new key → copy

### Step 2 — Run notebooks in order (Google Colab)
```
01 → 02 → 03 → 04 → 05 → 06
```
Notebook 04 provides the historical AIS baseline. From this point forward,
`ingest_ais.py` runs daily and appends fresh vessel data automatically.

### Step 3 — Update BDI when needed (monthly or as needed)
1. Go to: https://www.investing.com/indices/baltic-dry-historical-data
2. Click **Download Data** (top-right of the chart)
3. Save the file to `data/raw/`
4. Re-run notebook `02_ingest_clean_bdi_investing.ipynb` in Colab
5. Commit and push the updated `data/clean/bdi_clean.csv`

### Step 4 — Set GitHub Secrets
Go to: **GitHub repo → Settings → Secrets and variables → Actions → New repository secret**

| Secret name | Value |
|-------------|-------|
| `WEATHER_API_KEY` | Your OpenWeatherMap key |
| `AISSTREAM_API_KEY` | Your AISStream key |
| `GCP_KEY` | Full contents of your GCP service account JSON file |
| `BQ_PROJECT` | Your GCP project ID |
| `BQ_DATASET` | `shipping_data` |

### Step 5 — Enable GitHub Actions
The workflow triggers automatically weekdays at 20:00 UTC.
You can also trigger manually: **Actions tab → Daily Maritime Data Update → Run workflow**

---

## BigQuery Tables

### Source tables
| Table | Updated | Description |
|-------|---------|-------------|
| `trade_flows` | Annual (notebook 01) | UN Comtrade bilateral trade, 5 HS codes |
| `bdi_daily` | Daily auto-sync from bdi_clean.csv | Baltic Dry Index — full history |
| `port_weather` | Daily (ingest_weather.py) | OpenWeatherMap at 20 ports — grows daily |
| `strait_conditions` | Daily (ingest_weather.py) | 8 maritime chokepoints — grows daily |
| `vessel_movements` | Daily (ingest_ais.py) | AIS vessel positions — grows daily |

### Analytical tables (rebuilt daily by update_combined.py)
| Table | Join keys | Business insight |
|-------|-----------|-----------------|
| `analysis_bdi_trade` | year | BDI vs annual trade volume |
| `analysis_port_risk_trade` | country_iso | Port weather risk × trade exposure |
| `analysis_strait_monitor` | strait_name | Chokepoint disruption scores (0–100) |
| `analysis_commodity_bdi` | year × hs_code | Freight cost burden per commodity |
| `analysis_bdi_signals` | date | Daily BULLISH/BEARISH/NEUTRAL signals |
| `analysis_net_exporter_risk` | reporter_iso | Net exporter rank × port weather |
| `analysis_seasonal_freight` | month | Monthly BDI seasonality |
| `analysis_china_concentration` | hs_code × year | China supply dominance |
| `analysis_vessel_port_risk` | port_name | AIS vessel density × port weather |
| `analysis_route_disruption` | route | Daily status of 6 key trade routes |
| `analysis_strait_vessel_trend` | strait_name × date | Daily vessel counts per strait (trend) |

---

## Streamlit Dashboard (8 pages)

Run locally: `streamlit run dashboard/app.py`

| Page | What it shows |
|------|---------------|
| Executive Summary | Morning briefing — all sources at a glance |
| Strait Monitor | 8 chokepoints with disruption scores + map |
| Route Disruption | 6 key trade routes — clear / watch / disrupted |
| Baltic Dry Index | BDI trend, market signals, charter recommendations |
| Trade Analysis | Comtrade flows, trade balance, BDI correlation |
| Port Risk | 20 ports — weather + trade exposure in $ |
| Vessel Activity | Daily AIS: positions + strait traffic trends |
| Cross-Source Insights | Commodity freight burden, China concentration, booking calendar |

---

## AI Usage Documentation
All AI interactions documented in report Section 12 per rubric Section 7.
