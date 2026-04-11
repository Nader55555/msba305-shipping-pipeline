# Maritime Shipping Intelligence Pipeline
**MSBA 305 — Data Processing Framework | Spring 2025/2026**
**Instructor:** Dr. Ahmad El-Hajj | **Domain:** Shipping & Supply Chain

---

## 3 Data Sources

| # | Source | Format | Frequency | API Key |
|---|--------|--------|-----------|---------|
| 1 | **UN Comtrade** — bilateral trade flows by commodity | CSV (manual download) | Annual | No |
| 2 | **Nasdaq Data Link** — Baltic Dry Index (freight costs) | JSON API | Daily | Yes — free at data.nasdaq.com |
| 3 | **OpenWeatherMap** — weather at top 20 global ports | JSON API | Daily (real-time) | Yes — free at openweathermap.org |

---

## Analytical Questions Answered

| # | Question | Query | Tables |
|---|----------|-------|--------|
| 1 | Which trade routes have highest export value? | Query 1 | trade_flows |
| 2 | Which months have highest freight costs historically? | Query 2 | bdi_daily |
| 3 | Which countries are net exporters vs importers? | Query 3 | trade_flows |
| 4 | Does BDI predict trade volume shifts? | Query 4 | bdi_daily + trade_flows |
| 5 | When does BDI signal bearish conditions? | Query 5 | bdi_daily |
| 6 | Do weather conditions correlate with port trade volumes? | Query 6 | port_weather + trade_flows |

---

## Pipeline Architecture

```
[UN Comtrade CSV]   ──────────────────────────────────┐
                                                       ├──► [04_combine_EDA] ──► [05_bigquery_upload] ──► [BigQuery] ──► [Power BI]
[Nasdaq BDI API]    ── GitHub Actions (daily 20:00) ──┤
                                                       │
[OpenWeatherMap API]── GitHub Actions (daily 20:00) ──┘
```

---

## Repository Structure

```
.
├── .github/workflows/
│   └── daily_update.yml        # Schedules BDI + Weather pulls daily
├── notebooks/                  # Run in Google Colab IN ORDER
│   ├── 00_setup_test_apis.ipynb
│   ├── 01_clean_comtrade.ipynb
│   ├── 02_ingest_clean_bdi.ipynb
│   ├── 03_ingest_clean_weather.ipynb
│   ├── 04_combine_EDA.ipynb
│   └── 05_bigquery_upload.ipynb
├── scripts/                    # Run by GitHub Actions
│   ├── ingest_bdi.py
│   ├── ingest_weather.py
│   └── upload_bigquery.py
├── SQL/
│   └── queries.sql             # 6 queries, simple → complex
├── data/
│   ├── raw/                    # NEVER modify original files
│   └── clean/                  # Output of cleaning notebooks
├── dashboard/                  # Power BI .pbix
├── requirements.txt
└── README.md
```

---

## Setup (Step by Step)

### Step 1 — Get your API keys (5 min each)

**Nasdaq Data Link (BDI):**
1. Go to https://data.nasdaq.com/sign-up
2. Register → Profile → API Key → copy

**OpenWeatherMap (Port weather):**
1. Go to https://openweathermap.org/api
2. Register → Account → API Keys → copy (takes ~10 min to activate)

### Step 2 — Upload Comtrade CSV to Drive
Upload your `TradeData_*.csv` file to: `MyDrive/repo/data/raw/`

### Step 3 — Run notebooks in order
Open each notebook in Google Colab and run all cells:
```
00 → 01 → 02 → 03 → 04 → 05
```

### Step 4 — Set GitHub Secrets (for automated daily runs)
Go to: GitHub repo → Settings → Secrets → New repository secret

| Secret name | Value |
|-------------|-------|
| `NASDAQ_API_KEY` | Your Nasdaq key |
| `WEATHER_API_KEY` | Your OpenWeatherMap key |
| `GCP_KEY` | Full contents of your GCP service account JSON |
| `BQ_PROJECT` | Your GCP project ID |
| `BQ_DATASET` | `shipping_data` |

---

## BigQuery Tables

| Table | Rows (approx) | Description |
|-------|--------------|-------------|
| `trade_flows` | ~50K | UN Comtrade bilateral trade |
| `bdi_daily` | ~2,500 | Baltic Dry Index daily |
| `port_weather` | grows daily | OpenWeatherMap at 20 ports |
| `shipping_combined` | ~52K | Unified master table |
| `v_trade_balance` | view | Computed export − import per country |

---

## AI Usage Documentation
All AI interactions documented in report Section 12 per rubric Section 7.
