# Maritime Shipping Intelligence Dashboard — User Guide
**MSBA 305 | Maritime Intel App**
**Live:** https://msba305-shipping-pipeline-r2aztbz4hje8sbk4snzhru.streamlit.app/

---

## What is this dashboard?

This dashboard gives you a **daily shipping intelligence briefing** combining 6 live data sources — trade flows, freight costs, weather, vessel tracking, fuel prices, and shipping news — to answer one simple question:

> **"Is global shipping operating normally today, and if not, where, why, and what is driving it?"**

You don't need to know anything about shipping to use it. This guide explains everything in plain language.

---

## How to open the dashboard

**Online (recommended):** Open https://msba305-shipping-pipeline-r2aztbz4hje8sbk4snzhru.streamlit.app/ in any browser — no setup needed.

**Locally:** Open Git Bash in your repo folder and run:
```
python -m streamlit run dashboard/app.py
```
Then open `http://localhost:8501`. In the sidebar, toggle **Connect to BigQuery** and enter your credentials.

---

## The 6 data sources

| Source | What it provides | Updated |
|--------|-----------------|---------|
| **UN Comtrade** | Trade flows between 188 countries, 5 commodity categories | Annual |
| **BDI (investing.com)** | Baltic Dry Index — daily freight cost index 2015–2026 | Monthly (manual) |
| **OpenWeatherMap** | Real-time weather at 120 ports + 8 straits | Twice daily |
| **AISStream** | Live vessel positions globally (~21,000 ships/day) | Twice daily |
| **EIA (U.S. Energy)** | Brent crude oil daily price — last 90 days | Twice daily |
| **NewsAPI.org** | Maritime shipping news classified by risk level and route | Twice daily |

---

## The 120 monitored ports — why these?

These are the **120 busiest container and bulk cargo ports** by annual throughput, handling ~60% of global container trade:

Shanghai, Singapore, Ningbo-Zhoushan, Shenzhen, Guangzhou, Busan, Tianjin, Hong Kong, Rotterdam, Dubai, Port Klang, Antwerp, Xiamen, Los Angeles, Hamburg, Long Beach, Tanjung Pelepas, Kaohsiung, Dalian, New York.

---

## The 8 critical straits — why these 8?

The 8 straits are the world's most important **maritime chokepoints** — narrow waterways where massive amounts of trade must pass through. Each one, if blocked, would cause immediate global disruption.

| Strait | Location | Why it matters |
|--------|----------|----------------|
| **Strait of Hormuz** | Between Iran and Oman | 20% of all global oil passes here. Every tanker from Saudi Arabia, Kuwait, UAE, and Iraq must use it. |
| **Strait of Malacca** | Between Malaysia and Indonesia | Busiest strait in the world (~100,000 ships/year). Almost all Asia-Europe/Middle East traffic uses it. |
| **Suez Canal** | Egypt | Ships save ~14 days vs going around Africa. 12% of world trade. |
| **Bab el-Mandeb** | Between Yemen and Djibouti | Southern entrance to Suez. All Asia-Europe ships must pass here before reaching the canal. |
| **Bosphorus Strait** | Turkey | Connects Black Sea to Mediterranean. Critical for grain and oil from Russia and Ukraine. |
| **Strait of Gibraltar** | Between Spain and Morocco | Gateway between Atlantic and Mediterranean. |
| **Strait of Dover** | Between UK and France | World's busiest shipping lane by vessel count — 500+ ships per day. |
| **Lombok Strait** | Indonesia | Alternative to Malacca for supertankers too large to safely use Malacca. |

---

## Disruption score — what does "above 40" mean?

The disruption score (0–100) combines **weather conditions + geopolitical risk baseline** for each strait.

| Score | Color | Meaning | Action |
|-------|-------|---------|--------|
| 0–20 | 🟢 Green | Normal | Nothing needed |
| 21–40 | 🟢 Green | Moderate | Monitor |
| 41–60 | 🟠 Orange | **Elevated risk** | Plan contingency routes |
| 61–100 | 🔴 Red | **Critical** | Rerouting likely, expect cost increases |

Why 40 as threshold: above 40, the combination of weather and geopolitical factors is significant enough to meaningfully affect shipping decisions.

---

## The 5 commodity categories (HS codes)

| Code | Name | What it includes | Shipping sensitivity |
|------|------|-----------------|---------------------|
| **HS 10** | Cereals | Wheat, rice, corn | High — cheap bulk, freight is large % of value |
| **HS 26** | Ores & slag | Iron ore, copper ore | High — very high volume bulk |
| **HS 27** | Mineral fuels | Crude oil, gas, coal | Low — oil is so valuable freight is small % |
| **HS 72** | Iron & steel | Steel products | Medium |
| **HS 89** | Ships & boats | Vessels themselves | Low — tracks shipbuilding industry |

---

## Fuel price signals explained

The dashboard shows **Brent crude oil price** as a proxy for ship fuel costs (VLSFO — Very Low Sulphur Fuel Oil used by modern ships).

| Signal | Meaning | What it implies |
|--------|---------|----------------|
| 🔴 **HIGH** | Price >10% above 30-day average | Fuel costs surging — adds $50-100/tonne to voyage costs |
| 🟡 **ELEVATED** | Price 3-10% above 30-day average | Watch closely — fuel margin pressure building |
| 🟢 **NORMAL** | Price within ±3% of 30-day average | Standard fuel costs |
| 💙 **LOW** | Price >3% below 30-day average | Good time to book — fuel cheaper than recent average |

---

## Shipping news risk levels explained

Each news article is automatically classified by risk level based on keywords:

| Level | Meaning | Keywords detected |
|-------|---------|------------------|
| 🔴 **HIGH** | Active disruption news | attack, missile, blockade, sanction, reroute, closure |
| 🟡 **MEDIUM** | Developing situation | conflict, tension, disruption, threat |
| 🟢 **LOW** | Normal maritime news | no risk keywords detected |

Articles are also tagged with which trade route they relate to (Suez, Hormuz, Malacca, Black Sea, Trans-Pacific, Dover).

---

## Route impact score explained

The route impact score (0–100) combines four signals:

| Signal | Weight | What it measures |
|--------|--------|-----------------|
| Traffic drop vs baseline | 40% | Are fewer vessels using this route than historically? |
| Speed drop | 20% | Are vessels moving slower than normal (congestion)? |
| Fuel pressure | 20% | Is fuel cost elevated today? |
| News risk | 20% | Are there HIGH risk news articles about this route? |

**Status labels:**
- 🔴 **CRITICAL** (score ≥ 60) — significant combined pressure on route
- 🟡 **ELEVATED** (score 30–60) — above normal pressure, monitor closely
- 🟢 **NORMAL** (score < 30) — standard conditions

---

## BDI signals — BULLISH, BEARISH, NEUTRAL explained

| Signal | Meaning | What to do |
|--------|---------|------------|
| 🔺 **BULLISH** | Freight rates rising fast | Lock in long-term charter contracts now |
| 🔻 **BEARISH** | Freight rates falling | Use spot market, wait for cheaper rates |
| ↔ **NEUTRAL** | No clear direction | Monitor weekly, no urgent action |
| ⚠ **OVERBOUGHT** | Rates historically very high | Consider hedging |
| 💡 **OVERSOLD** | Rates historically very cheap | Good entry point for future capacity |

Simple analogy: think of BDI like petrol prices. BULLISH = prices at the pump rising fast, fill up now. BEARISH = prices falling, wait before filling up.

---

## The 9 pages — what each one tells you

---

### 🌐 1. Live Intelligence *(Start here)*
**The single most important page — updated twice daily.**

This page shows everything happening right now in one screen:

**Top section — Vessel map:**
- Every dot = one ship captured in today's AIS collection
- Filter by vessel type and location using the dropdowns above the map
- Hover over any dot for vessel name, speed, navigation status, destination
- Colors: 🔴 Tankers 🔵 Cargo 🟡 Tug/Support 🟢 Fishing 🟣 Passenger
- KPIs below the map: total vessels, moving, tankers, cargo ships

**Bottom section — 3 columns:**

**Left — Brent crude oil (last 90 days):**
- Orange line = daily Brent price. Blue dashed = 30-day average
- When orange rises sharply above blue = fuel cost pressure building
- Current price + fuel signal shown above the chart

**Middle — Shipping news alerts:**
- Latest shipping news ranked by risk score (highest risk first)
- 🔴 HIGH risk = active disruption reported. 🟡 MEDIUM = developing. 🟢 LOW = normal
- Shows which trade route each article relates to

**Right — Route impact vs historical baseline:**
- Each of the 5 trade routes scored 0-100 based on traffic, speed, fuel, news
- Traffic vs history % shows if vessel count is above/below the historical average for this route
- A negative % = fewer ships than normal = early warning signal

**What to look for in the morning:**
- Any route showing 🔴 CRITICAL or traffic significantly below baseline?
- Fuel signal HIGH + disrupted strait = double pressure on shipping costs
- Multiple HIGH risk news articles about the same route = situation developing

---

### 📋 2. Executive Summary
**Daily briefing across all 6 sources.**

| KPI | What it means |
|-----|--------------|
| **BDI** | Today's freight cost index |
| **Ports at risk** | How many of 20 ports have dangerous wind (Beaufort ≥ 7) |
| **Straits elevated risk** | How many of 8 chokepoints score above 40/100 |
| **Total export value** | Total tracked commodity exports (all years) |
| **Vessels tracked** | Unique ships captured today |
| **BDI Signal** | BULLISH / BEARISH / NEUTRAL |

Below the KPIs: route status cards, port weather alerts, strait mini-cards, BDI 90-day chart.

---

### ⚡ 3. Strait Monitor
**Real-time status of the 8 chokepoints.**

- **Critical / High risk**: How many straits above 40/100 out of 8
- **Trade% at elevated risk**: % of world seaborne trade passing through at-risk straits
- **Oil% at elevated risk**: % of world oil through at-risk straits
- Bar chart compares all 8 — dashed lines at 40 (elevated) and 60 (critical)
- World map shows each strait colored by risk level

---

### 🚢 4. Route Disruption
**Daily status of the 6 key global trade lanes.**

| Route | Key strait | Main cargo |
|-------|-----------|------------|
| **Middle East Oil → Asia** | Strait of Hormuz | Oil, LNG |
| **Asia → Europe (Suez)** | Bab el-Mandeb + Suez | Containers, steel |
| **Black Sea Grain Exports** | Bosphorus | Wheat, corn |
| **Atlantic / Europe ↔ Mediterranean** | Strait of Gibraltar | Containers |
| **Asia intra-regional** | Strait of Malacca | Containers |
| **Trans-Pacific (Asia → US West Coast)** | None | Electronics, steel |

**Status:** 🔴 DISRUPTED = active disruption. 🟡 WATCH = monitor. 🟢 CLEAR = normal.

---

### 📈 5. Baltic Dry Index
**The price of shipping — is freight getting cheaper or more expensive?**

- **Current BDI**: ~1,500 historical average. Above 2,500 = expensive. Below 800 = cheap.
- **Seasonal pattern:** Cheapest Jan-Mar, most expensive Sep-Nov
- Chart shows BDI history with major events annotated (COVID, Suez blockage, Ukraine war, Houthi attacks)
- Charter recommendations table: how many days the market spent in each signal type historically

---

### 🌍 6. Trade Analysis
**Who exports what, how much, and where?**

- **Top 15 Exporters**: USA #1 (mainly LNG). Saudi Arabia #2 (oil).
- **Trade Balance**: Green = net exporter. Red = net importer. China is a large net importer in these HS codes.
- **BDI vs Trade**: Blue bars = export value. Orange line = BDI. When orange rises faster than bars, shipping costs eating into margins.
- **YoY Heatmap** (expandable): Green = growth year. Red = decline.

---

### ⚓ 7. Port Risk
**Real-time weather at 120 major ports + trade exposure.**

**Beaufort scale — when does it affect shipping?**

| Beaufort | Wind | Impact |
|----------|------|--------|
| 0–3 | 0–5 m/s | No effect |
| 4–6 | 6–12 m/s | Light caution |
| **7** | **14 m/s** | **⚠ Crane operations may stop** |
| 8–9 | 18–24 m/s | Port severely disrupted |
| 10–12 | 25+ m/s | Port may close |

---

### 🛥 8. Vessel Activity
**Where are ships — and how does today compare to history?**

**Filters:** vessel type, speed category, location, minimum speed.

**Historical section:**
- Daily vessel count over time — drop = early disruption signal
- Vessels by type over time — tanker-specific drop = energy threat, not general
- Average speed trend — dropping speed = congestion building

**Strait traffic trends:** daily vessel counts per strait. A downward trend at a high-risk strait often appears **2–7 days before official news confirms** a disruption.

> **Coverage note:** AIS land-based receivers do not cover Persian Gulf, Red Sea, or Arabian Sea on the free tier. These regions require paid satellite AIS.

---

### 🔗 9. Cross-Source Insights
**Combined analysis across all 6 data sources.**

- **Commodity Freight Cost:** freight burden (BDI / trade value) by commodity
- **China Concentration:** China's share of each commodity + disruption exposure
- **Booking Calendar:** best months to book based on 10-year BDI history
- **All Analytical Tables:** browse all 15 analytical tables with download option

---

## Quick Reference — Morning Checklist

**Every morning, 3 steps:**

1. **Live Intelligence** → Any route 🔴 CRITICAL? Fuel signal HIGH? High-risk news?
2. **Strait Monitor** → Any straits above 60/100?
3. **Vessel Activity** → Is traffic dropping at any key strait?

**If fuel is HIGH + route is DISRUPTED** → shipping costs are under double pressure — factor into freight contracts.

**If news risk HIGH on a route AND vessel count dropping** → strongest early warning combination available.

---

## Data freshness

| Source | Update frequency | How |
|--------|-----------------|-----|
| BDI | Monthly | Manual: download from investing.com, re-run notebook 02 |
| Port weather | Twice daily (09:00 + 20:00 Lebanon) | Automatic — GitHub Actions |
| Strait conditions | Twice daily | Automatic — GitHub Actions |
| Vessel positions | Twice daily | Automatic — GitHub Actions (4-min AIS collection) |
| Fuel prices | Twice daily | Automatic — EIA API |
| Shipping news | Twice daily | Automatic — NewsAPI.org |
| Trade flows | Annual | Manual — UN Comtrade |

---

## Glossary

| Term | Plain language |
|------|---------------|
| **BDI** | Baltic Dry Index — daily price to rent a bulk cargo ship |
| **Beaufort** | Wind strength scale 0–12. Beaufort 7 (14+ m/s) affects port crane operations |
| **Brent crude** | Global benchmark price for crude oil. Proxy for ship fuel (VLSFO) costs |
| **BULLISH** | Freight rates rising — lock in charter contracts now |
| **BEARISH** | Freight rates falling — use spot market, wait for cheaper rates |
| **Charter** | Renting a ship for a specific voyage or time period |
| **Chokepoint** | A narrow waterway all ships must use — disrupting it affects global trade |
| **HS code** | Harmonized System — international product category code |
| **Impact score** | Route disruption composite score combining traffic, speed, fuel, news signals |
| **Knot (kn)** | Ship speed. 1 knot = 1.85 km/h |
| **MMSI** | Ship's unique ID number — like a license plate |
| **Rerouting** | Ships taking a longer path to avoid a blocked area |
| **Spot market** | Booking a ship at today's market price (vs long-term contract) |
| **VLSFO** | Very Low Sulphur Fuel Oil — standard ship fuel. ~7x Brent price per tonne |
| **VLCC** | Very Large Crude Carrier — largest oil tanker type |

---

*Maritime Intel Dashboard · MSBA 305 · American University of Beirut · Dr. Ahmad El-Hajj · Spring 2025/2026*
*Sources: UN Comtrade · BDI (investing.com) · OpenWeatherMap · AISStream · EIA · NewsAPI.org · Storage: Google BigQuery*
