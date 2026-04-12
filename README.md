# Maritime Shipping Intelligence Pipeline
**MSBA 305 — Data Processing Framework | Spring 2025/2026**
**Instructor:** Dr. Ahmad El-Hajj | **Domain:** Shipping & Supply Chain

---

## 4 Data Sources

| # | Source | Format | Frequency | API Key |
|---|--------|--------|-----------|---------|
| 1 | **UN Comtrade** — bilateral trade flows by commodity (5 HS codes) | CSV (manual download) | Annual | No |
| 2 | **BDI (investing.com)** — Baltic Dry Index freight cost indicator | CSV (manual download + Stooq daily auto-update) | Daily | No |
| 3 | **OpenWeatherMap** — weather at top 20 global ports + 8 critical straits | JSON API | Daily (automated) | Yes — free at openweathermap.org |
| 4 | **AISStream** — vessel movements at Singapore port | WebSocket (manual run) | Static snapshot | Yes — free at aisstream.io |

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
| 7 | Which maritime straits are currently at risk? (Hormuz, Malacca, Suez…) | `analysis_strait_monitor` |
| 8 | Which global trade routes are disrupted today? | `analysis_route_disruption` |
| 9 | How dominant is China in each shipping commodity? | `analysis_china_concentration` |
| 10 | When is freight cheapest — booking calendar? | `analysis_seasonal_freight` |

---

## Pipeline Architecture

```
[UN Comtrade CSV]    ── notebook 01 ──────────────────────────────────────┐
                                                                           │
[BDI investing.com]  ── notebook 02 → ingest_bdi.py (daily, Stooq) ──────┤
                                                                           ├──► update_combined.py
[OpenWeatherMap API] ── ingest_weather.py (daily, GitHub Actions) ─────── ┤       │
                                                                           │       ▼
[AISStream WS]       ── notebook 04 ──────────────────────────────────────┘   BigQuery
                                                                                   │
                                                                               Streamlit dashboard
```

**GitHub Actions daily run order (weekdays 20:00 UTC):**
1. `ingest_bdi.py` — fetch latest BDI from Stooq, append to bdi_clean.csv, upload to BigQuery
2. `ingest_weather.py` — fetch weather at 20 ports + 8 straits, upload to BigQuery
3. `update_combined.py` — rebuild all 10 analytical JOIN tables from fresh data

---

## Repository Structure

```
.
├── .github/workflows/
│   └── daily_update.yml          # Automated daily pipeline (BDI + Weather + Analytics)
├── notebooks/                    # Run in Google Colab IN ORDER (one-time setup)
│   ├── 01_clean_comtrade.ipynb
│   ├── 02_ingest_clean_bdi_investing.ipynb
│   ├── 03_ingest_clean_weather.ipynb
│   ├── 04_ingest_clean_aisstream.ipynb
│   ├── 05_combine_EDA.ipynb
│   └── 06_bigquery_upload.ipynb
├── scripts/                      # Run by GitHub Actions daily
│   ├── ingest_bdi.py             # BDI from Stooq.com (no API key)
│   ├── ingest_weather.py         # Port weather + strait conditions
│   └── update_combined.py        # 10 analytical JOIN tables
├── dashboard/
│   └── app.py                    # Streamlit dashboard (8 pages)
├── SQL/
│   └── queries.sql               # 6 analytical queries (simple → complex)
├── data/
│   ├── raw/                      # Never modify — original downloaded files
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

**AISStream (Vessel tracking — optional, for notebook 04 only):**
1. Go to https://aisstream.io
2. Register → API Keys → copy

**BDI — no API key needed.** The `ingest_bdi.py` script fetches automatically from Stooq.com (free, public).

### Step 2 — Run notebooks in order (Google Colab)
```
01 → 02 → 03 → 04 → 05 → 06
```
This populates `data/clean/` with the static source CSVs and uploads the base tables to BigQuery.

### Step 3 — Set GitHub Secrets
Go to: **GitHub repo → Settings → Secrets and variables → Actions → New repository secret**

| Secret name | Value |
|-------------|-------|
| `WEATHER_API_KEY` | Your OpenWeatherMap key |
| `GCP_KEY` | Full contents of your GCP service account JSON file |
| `BQ_PROJECT` | Your GCP project ID (e.g. `msba305-shipping-123`) |
| `BQ_DATASET` | `shipping_data` |

### Step 4 — Enable GitHub Actions
The workflow triggers automatically weekdays at 20:00 UTC.
You can also trigger it manually: **Actions tab → Daily Maritime Data Update → Run workflow**

---

## BigQuery Tables

### Source tables (uploaded by notebooks + daily scripts)
| Table | Updated | Description |
|-------|---------|-------------|
| `trade_flows` | Annual (notebook) | UN Comtrade bilateral trade, 5 HS codes |
| `bdi_daily` | Daily (ingest_bdi.py) | Baltic Dry Index — full history + daily append |
| `port_weather` | Daily (ingest_weather.py) | OpenWeatherMap at 20 ports — grows daily |
| `strait_conditions` | Daily (ingest_weather.py) | 8 maritime chokepoints — grows daily |
| `vessel_movements` | Static (notebook) | AISStream vessel positions at Singapore |

### Analytical tables (rebuilt daily by update_combined.py)
| Table | Join keys | Business insight |
|-------|-----------|-----------------|
| `analysis_bdi_trade` | year | BDI vs annual trade volume — freight cost correlation |
| `analysis_port_risk_trade` | country_iso | Port weather risk × trade exposure in $ |
| `analysis_strait_monitor` | strait_name | Chokepoint disruption scores (0–100) |
| `analysis_commodity_bdi` | year × hs_code | Freight cost burden per commodity |
| `analysis_bdi_signals` | date | Daily BULLISH/BEARISH/NEUTRAL signals + charter advice |
| `analysis_net_exporter_risk` | reporter_iso | Net exporter rank × port weather today |
| `analysis_seasonal_freight` | month | Monthly BDI seasonality — booking calendar |
| `analysis_china_concentration` | hs_code × year | China supply dominance + disruption exposure |
| `analysis_vessel_port_risk` | port_name | AIS vessel density × port weather |
| `analysis_route_disruption` | route | Daily status of 6 key global trade routes |

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
| Vessel Activity | AIS positions, vessel types, speed categories |
| Cross-Source Insights | Commodity freight burden, China concentration, booking calendar |

---

## AI Usage Documentation
All AI interactions documented in report Section 12 per rubric Section 7.
