# Maritime Shipping Intelligence Pipeline
**MSBA 305 — Data Processing Framework | Spring 2025/2026**
**Instructor:** Dr. Ahmad El-Hajj | **Domain:** Maritime Shipping & Supply Chain Intelligence

**Live Dashboard:** [Streamlit Community Cloud](https://share.streamlit.io) | **GitHub:** [Nader55555/msba305-shipping-pipeline](https://github.com/Nader55555/msba305-shipping-pipeline)

---

## 6 Data Sources

| # | Source | Format | Frequency | Key Required |
|---|--------|--------|-----------|-------------|
| 1 | **UN Comtrade** — bilateral trade flows, 5 HS codes (10,26,27,72,89) | CSV manual download | Annual | No |
| 2 | **BDI (investing.com)** — Baltic Dry Index freight cost indicator | CSV manual download | Monthly | No |
| 3 | **OpenWeatherMap** — weather at 20 ports + 8 critical straits | JSON REST API | Daily automated | Yes — free |
| 4 | **AISStream** — live vessel positions globally | WebSocket API | Daily automated (4 min) | Yes — free |
| 5 | **EIA (U.S. Energy Information Administration)** — Brent crude oil daily price | JSON REST API | Daily automated | Yes — free |
| 6 | **NewsAPI.org** — maritime shipping news, risk classification | JSON REST API | Daily automated | Yes — free |

---

## Pipeline Architecture

```
[UN Comtrade CSV]    ── notebook 01 ─────────────────────────────────────────┐
[BDI CSV (manual)]   ── notebook 02 ────────── commit bdi_clean.csv ─────────┤
                                                                              │
[OpenWeatherMap API] ── ingest_weather.py ───► BigQuery (daily) ─────────────┤
[AISStream WS API]   ── ingest_ais.py ───────► BigQuery (daily) ─────────────┤──► update_combined.py
[EIA REST API]       ── ingest_fuel.py ──────► BigQuery (daily) ─────────────┤       │
[NewsAPI.org]        ── ingest_news.py ──────► BigQuery (daily) ─────────────┤       ▼
                                                                              │   BigQuery (16+ tables)
                     build_route_analytics.py ◄──────────────────────────────┘       │
                            │                                                         ▼
                            └──────────────────────────────────────────────► Streamlit Dashboard
```

**GitHub Actions daily run order (weekdays 17:00 + 23:00 UTC):**
1. `ingest_weather.py` — 20 ports + 8 straits
2. `ingest_ais.py` — 4-minute global AIS collection
3. `ingest_fuel.py` — EIA Brent crude price
4. `ingest_news.py` — shipping news + risk classification
5. `build_route_analytics.py` — route impact vs historical baseline
6. `update_combined.py` — rebuild all 12+ analytical tables

---

## Repository Structure

```
.
├── .github/workflows/
│   └── daily_update.yml              # Automated pipeline (runs twice daily)
├── config/
│   └── routes.yaml                   # 5 trade route definitions
├── notebooks/                        # Run once in order (Google Colab)
│   ├── 01_clean_comtrade.ipynb       # UN Comtrade cleaning + EDA
│   ├── 02_ingest_clean_bdi_investing.ipynb  # BDI cleaning
│   ├── 03_ingest_clean_weather.ipynb # Weather baseline EDA
│   ├── 04_ingest_clean_aisstream.ipynb  # AIS historical baseline
│   ├── 05_combine_EDA.ipynb          # Combined EDA across all sources
│   └── 06_bigquery_upload.ipynb      # One-time BigQuery table setup
├── scripts/                          # Run daily by GitHub Actions
│   ├── ingest_weather.py             # OpenWeatherMap → BigQuery
│   ├── ingest_ais.py                 # AISStream → BigQuery
│   ├── ingest_fuel.py                # EIA API → BigQuery
│   ├── ingest_news.py                # NewsAPI.org → BigQuery
│   ├── build_route_analytics.py      # Route impact vs history → BigQuery
│   └── update_combined.py            # 12 analytical JOIN tables → BigQuery
├── dashboard/
│   ├── app.py                        # Streamlit dashboard (9 pages)
│   └── user_guide.md                 # Plain-language user guide
├── SQL/
│   └── queries.sql                   # 16 analytical queries (simple → complex)
├── data/
│   ├── raw/                          # Never committed — original downloads
│   └── clean/                        # Committed: cleaned CSVs + EDA figures
├── requirements.txt
├── AI_USAGE_LOG.md
└── README.md
```

---

## Setup (Step by Step)

### Step 1 — Get your API keys

| Service | URL | Cost |
|---------|-----|------|
| OpenWeatherMap | https://openweathermap.org/api | Free |
| AISStream | https://aisstream.io | Free (BETA) |
| EIA | https://www.eia.gov/opendata/ | Free |
| NewsAPI.org | https://newsapi.org | Free tier |

### Step 2 — Run notebooks once (Google Colab, in order)
```
01 → 02 → 03 → 04 → 05 → 06
```
Notebook 06 creates all BigQuery tables. Run it once before enabling Actions.

### Step 3 — Update BDI (monthly)
1. Go to: https://www.investing.com/indices/baltic-dry-historical-data
2. Click **Download Data** → save to `data/raw/`
3. Re-run notebook `02_ingest_clean_bdi_investing.ipynb`
4. Commit the updated `data/clean/bdi_clean.csv`

### Step 4 — Set GitHub Secrets

Go to: **GitHub repo → Settings → Secrets → Actions → New repository secret**

| Secret | Value |
|--------|-------|
| `WEATHER_API_KEY` | OpenWeatherMap API key |
| `AISSTREAM_API_KEY` | AISStream API key |
| `EIA_API_KEY` | EIA API key |
| `NEWSDATA_API_KEY` | NewsAPI.org API key |
| `GCP_KEY` | Full contents of your GCP service account JSON |
| `BQ_PROJECT` | Your GCP project ID (e.g. `msba305-shipping`) |
| `BQ_DATASET` | `shipping_data` |

### Step 5 — Enable GitHub Actions
The workflow triggers automatically. To run manually:
**Actions tab → Daily Maritime Data Update → Run workflow**

---

## BigQuery Tables (19 total)

### Source tables (5)
| Table | Updated by | Description |
|-------|-----------|-------------|
| `trade_flows` | Notebook 01 | UN Comtrade bilateral trade, 5 HS codes |
| `bdi_daily` | Manual monthly | Baltic Dry Index full history |
| `port_weather` | ingest_weather.py | OpenWeatherMap — 20 ports (APPEND) |
| `strait_conditions` | ingest_weather.py | 8 chokepoints (APPEND) |
| `vessel_movements` | ingest_ais.py | Global AIS vessel positions (APPEND) |
| `fuel_prices_daily` | ingest_fuel.py | EIA Brent crude price — 90 days |
| `shipping_news` | ingest_news.py | Maritime news with risk classification |

### Analytical tables (12, rebuilt daily by update_combined.py)
| Table | Join keys | Business insight |
|-------|-----------|-----------------|
| `analysis_bdi_trade` | year | BDI vs annual trade volume correlation |
| `analysis_port_risk_trade` | country_iso | Port weather × trade exposure |
| `analysis_strait_monitor` | strait_name | Chokepoint disruption scores (0–100) |
| `analysis_commodity_bdi` | year × hs_code | Freight cost burden per commodity |
| `analysis_bdi_signals` | date | Daily BULLISH/BEARISH/NEUTRAL signals |
| `analysis_net_exporter_risk` | reporter_iso | Net exporter rank × port weather |
| `analysis_seasonal_freight` | month | Monthly BDI seasonality |
| `analysis_china_concentration` | hs_code × year | China supply dominance |
| `analysis_vessel_port_risk` | port_name | AIS density × port weather |
| `analysis_route_disruption` | route | Daily status of 6 key trade routes |
| `analysis_strait_vessel_trend` | strait × date | Daily vessel counts per strait |

### Route analytics tables (3, rebuilt daily by build_route_analytics.py)
| Table | Description |
|-------|-------------|
| `route_baselines` | Historical average vessel counts per route |
| `analysis_current_vs_historical` | Today vs baseline — traffic, speed, fuel, news impact |
| `route_deviation_alerts` | Routes with significant deviations flagged |

---

## Dashboard (9 pages)

Run locally: `streamlit run dashboard/app.py`

| Page | What it shows |
|------|---------------|
| 🌐 Live Intelligence | Vessel map + fuel prices + shipping news + route impact |
| 📊 Executive Summary | Morning briefing — all 6 sources at a glance |
| ⚡ Strait Monitor | 8 chokepoints — disruption scores + world map |
| 🚢 Route Disruption | 6 trade routes — CLEAR / WATCH / DISRUPTED |
| 📈 Baltic Dry Index | BDI trend, market signals, charter recommendations |
| 🌍 Trade Analysis | Comtrade flows, trade balance, BDI correlation |
| ⚓ Port Risk | 20 ports — weather + trade exposure |
| 🛥 Vessel Activity | AIS positions + historical trends + strait traffic |
| 🔗 Cross-Source Insights | Freight burden, China concentration, booking calendar |

---

## AI Usage
All AI interactions documented in `AI_USAGE_LOG.md` and Architecture Report Section 10.
Tool used: Claude (Anthropic) — claude.ai
