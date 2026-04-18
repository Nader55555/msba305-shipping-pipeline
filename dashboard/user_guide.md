# Maritime Shipping Intelligence Dashboard — User Guide
**MSBA 305 | Maritime Intel App**

---

## What is this dashboard?

This dashboard gives you a daily briefing on global shipping conditions. It combines 4 data sources — trade flows, freight costs, weather, and live vessel tracking — to answer one simple question every morning:

> **"Is global shipping operating normally today, and if not, where and why?"**

You don't need to know anything about shipping to use it. This guide explains everything in plain language.

---

## How to open the dashboard

1. Open Git Bash in your repo folder
2. Run: `python -m streamlit run dashboard/app.py`
3. Open your browser at `http://localhost:8501`
4. In the left sidebar: toggle **Connect to BigQuery**, enter your Project ID and credentials to load live data

---

## The 8 pages — what each one tells you

---

### 📋 1. Executive Summary
**Your daily morning briefing — start here every day.**

This page shows the most important numbers at a glance:

| KPI | What it means |
|-----|--------------|
| **BDI** | The price of shipping bulk cargo today. Higher = more expensive. |
| **Ports at risk** | How many of the 20 monitored ports have dangerous wind conditions today (Beaufort ≥ 7) |
| **Straits elevated risk** | How many of the 8 critical chokepoints have disruption scores above 40/100 |
| **Total export value** | Total value of tracked commodities in the database |
| **Vessels tracked** | Number of unique ships seen by AIS today |
| **BDI Signal** | Today's market signal: BULLISH / BEARISH / NEUTRAL |

**Below the KPIs:**
- **Route Disruption Status** — the 6 main global shipping lanes, each labeled 🔴 DISRUPTED / 🟡 WATCH / 🟢 CLEAR
- **Port Weather Alerts** — any port with dangerous wind conditions
- **Strait Conditions** — all 8 chokepoints with their risk scores
- **BDI sparkline** — BDI trend over the last 90 days

**What to do with this page:**
- If you see 🔴 DISRUPTED routes → go to Route Disruption page for details
- If BDI signal says BULLISH → freight costs are rising, check Baltic Dry Index page
- If Straits elevated risk is 3 or more → check Strait Monitor

---

### ⚡ 2. Strait Monitor
**Real-time status of the 8 most critical shipping chokepoints in the world.**

Every large ship must pass through one of these narrow passages. If a strait is disrupted, thousands of ships may need to reroute, adding days and millions of dollars in cost.

**The 8 straits monitored:**

| Strait | Why it matters |
|--------|---------------|
| **Bab el-Mandeb** | Red Sea entrance to Suez Canal — all Asia↔Europe traffic |
| **Strait of Hormuz** | 20% of world's oil passes through here |
| **Suez Canal** | Saves 14 days vs going around Africa |
| **Bosphorus Strait** | Black Sea grain and oil exports |
| **Strait of Gibraltar** | Atlantic↔Mediterranean gateway |
| **Strait of Malacca** | Busiest strait by ship count — Asia hub |
| **Strait of Dover** | English Channel — 500+ ships per day |
| **Lombok Strait** | Alternative to Malacca for supertankers |

**Reading the disruption score (0–100):**
- **0–20**: Normal conditions ✅
- **21–40**: Moderate — monitor 👁
- **41–60**: Elevated risk — plan contingency ⚡
- **61–100**: Critical — rerouting likely 🔴

**The bar chart** compares all 8 straits visually. The dashed lines mark the elevated risk and critical thresholds.

**The map** shows where each strait is on the globe, colored by risk level.

---

### 🚢 3. Route Disruption
**Daily status of the 6 key global trade lanes.**

These are the main highways of global shipping. When they're disrupted, goods take longer and cost more to deliver.

| Route | Key strait | Main cargo |
|-------|-----------|------------|
| **Middle East Oil → Asia** | Strait of Hormuz | Oil, LNG |
| **Asia → Europe (Suez)** | Bab el-Mandeb + Suez | Containers, steel |
| **Black Sea Grain Exports** | Bosphorus | Wheat, corn |
| **Atlantic / Europe ↔ Mediterranean** | Strait of Gibraltar | Containers |
| **Asia intra-regional** | Strait of Malacca | Containers |
| **Trans-Pacific (Asia → US West Coast)** | None (open ocean) | Electronics, steel |

**Status labels:**
- 🔴 **DISRUPTED** — active disruption, ships likely rerouting
- 🟡 **WATCH** — risk is elevated, monitor closely
- 🟢 **CLEAR** — normal operations

Each card shows the strait score, affected commodities, alternative routing options, and a generated risk label based on the score (not hardcoded text).

The bar chart at the bottom shows all 6 routes ranked by their disruption score.

---

### 📈 4. Baltic Dry Index (BDI)
**The price of shipping — is freight getting cheaper or more expensive?**

The BDI is published daily by the Baltic Exchange in London. It measures the cost to rent a bulk cargo ship (for grain, iron ore, coal). Think of it like a price index for ship rentals.

**Key numbers:**
- **Current BDI**: Today's value. Historical average is around 1,500. Above 2,500 = expensive. Below 800 = cheap.
- **Period high/low**: The range over your selected date range
- **Spikes/Drops**: Days when BDI moved more than 5% in one day — indicates market volatility

**Market signals:**
| Signal | Meaning | What to do |
|--------|---------|------------|
| 🔺 **BULLISH** | Rates are rising fast | Lock in long-term charter contracts now before they get more expensive |
| 🔻 **BEARISH** | Rates are falling | Use spot market — wait for cheaper rates |
| ↔ **NEUTRAL** | No clear direction | Monitor, no urgent action |
| ⚠ **OVERBOUGHT** | Rates are very high vs history | Consider hedging or delaying non-urgent shipments |
| 💡 **OVERSOLD** | Rates are very low vs history | Good time to book future capacity at cheap rates |

**Seasonal pattern (Monthly BDI chart):**
- **Cheapest months**: January, February, March (post-Chinese New Year slowdown)
- **Most expensive**: September, October, November (pre-holiday season restocking)

The BDI history chart shows major events like COVID-19 (2020 crash), the Suez Canal blockage (2021 spike), and supply chain crisis peak (2021-2022).

---

### 🌍 5. Trade Analysis
**Who exports what, how much, and where do they send it?**

This page uses UN Comtrade data covering 2015–2025 for 5 commodity categories:
- **HS 10**: Cereals (wheat, rice, corn)
- **HS 26**: Ores & slag (iron ore, copper ore)
- **HS 27**: Mineral fuels (oil, gas, coal)
- **HS 72**: Iron & steel
- **HS 89**: Ships & boats

**Top 15 Exporters chart**: Who sells the most of these commodities globally. USA #1 is correct — mainly due to mineral fuels (LNG) exports.

**Trade Balance chart**: Green bar = net exporter (sells more than it buys). Red bar = net importer (buys more than it sells). China being a large net importer means it needs a lot of ships coming IN with raw materials.

**BDI vs Trade correlation**: When freight costs (orange BDI line) rise faster than trade values (blue bars), shipping is consuming a larger share of commodity profits — especially damaging for low-value bulk cargo like grain.

**Year-on-Year heatmap**: Green = export growth, Red = export decline. NaN = data not available for that year.

---

### ⚓ 6. Port Risk
**Weather conditions at the 20 most important ports in the world.**

**Beaufort wind scale — what it means for ships:**
| Beaufort | Speed | Impact |
|----------|-------|--------|
| 0–3 | 0–5 m/s | No effect |
| 4–6 | 6–12 m/s | Light caution |
| **7** | **14–17 m/s** | **⚠ Loading/unloading may stop** |
| 8–9 | 18–24 m/s | Port operations severely disrupted |
| 10–12 | 25+ m/s | Port may close entirely |

**Port cards** show each port's current temperature, wind speed, weather type, humidity, and trade exposure. The 🟡 badge = disruption warning.

**"Trade: N/A"** means the port's country didn't match a trade-flow record in the analytical table for that filter — the weather data is still real and accurate. It does not mean the port has no trade.

**Net Exporters table**: Shows major exporting countries and whether their home ports currently have weather disruptions.

---

### 🛥 7. Vessel Activity
**Where are ships right now — across 8 straits and major ports globally.**

This page shows data from AISStream — live vessel position data collected automatically every weekday during a 4-minute collection window at 20:00 UTC.

**Key metrics (top row):**
- **Total messages**: Raw AIS position messages received
- **Unique vessels**: Number of distinct ships seen today
- **Moving vessels**: Ships currently underway (speed > 0.5 knots)
- **Avg speed**: Average speed across all vessels in knots
- **Tankers**: Number of oil/chemical tankers specifically tracked

**Congestion alerts**: Ports where the vessel count is unusually high trigger a 🟡 CONGESTION warning. Rotterdam at 2,700+ vessels is the busiest port in Europe — that's normal. Los Angeles and New York at 100+ is also normal. A CONGESTION alert combined with bad weather at the same port is a serious delay signal.

**Vessel positions map (world map):**
All captured vessels plotted on a real map across all monitored locations. Hover over any dot to see the ship's name, speed, navigation status, and destination.
- Larger dots = moving vessels
- Smaller faded dots = stationary/anchored vessels
- Colors match the legend: 🔴 Tankers · 🔵 Cargo · 🟡 Tug/Support · 🟢 Fishing

> **Coverage note:** AIS signals are received by land-based stations (~40–50 nautical miles from shore). The Persian Gulf, Red Sea, and Strait of Hormuz have sparser AIS receiver coverage on the free tier. If no vessels appear in those areas, it means the receivers in that region didn't capture signals during the 4-minute collection window — not that the area has no traffic.

**Vessel types pie chart**: Breakdown by ship category — Cargo, Tanker, Passenger, Fishing, Tug/Support, Other.

**Speed categories:**
- Stationary (< 0.5 kn): Anchored or moored
- Slow/manoeuvring (0.5–3 kn): Entering/leaving port
- Transit (3–10 kn): Normal sailing
- Cruising (> 10 kn): Open ocean sailing

**Strait vessel traffic trends** (below the map):
Shows daily vessel counts per strait over time, built from the automated daily AIS collection. This is one of the most powerful features in the dashboard:

| What you see | What it means |
|---|---|
| Vessel count dropping at Hormuz | Ships may be avoiding the Persian Gulf — early disruption signal |
| Tanker count dropping while cargo stays normal | Energy-specific threat, not a general disruption |
| Count rising sharply at a port | Unusual congestion — possible port delays |
| Stable counts across all straits | Normal global shipping conditions |

A downward trend at a high-risk strait often appears **2–7 days before official news** confirms a disruption. This is one of the earliest warning signals available anywhere.

---

### 🔗 8. Cross-Source Insights
**What happens when you combine all 4 data sources?**

This page has 4 tabs:

**📦 Commodity Freight Cost**
For each of the 5 commodities, how does the freight cost (BDI) compare to the trade value?
- High ratio = freight is eating into profits for this commodity
- Cereals and ores are most sensitive (cheap cargo, expensive to ship)
- Mineral fuels (oil) are least sensitive (high value, so freight is a small %)

**🇨🇳 China Concentration**
How much does China dominate each commodity?
- High China share + disrupted Chinese ports = global supply risk
- This is why Shanghai weather matters globally

**📅 Booking Calendar**
Based on 10 years of BDI history — when is the cheapest time to book cargo?
- Green months = historically cheap (book here)
- Orange months = historically expensive (avoid if possible)

**📊 All Analytical Tables**
Technical view of all 12 analytical tables with row counts and source descriptions. Useful for understanding what data powers each chart.

---

## Quick Reference — What to check when you open the app

**Every morning, do this 3-step check:**

1. **Executive Summary** → Are any routes DISRUPTED? Is BDI signal BULLISH?
2. **Strait Monitor** → Which straits are above 40/100 score?
3. **Vessel Activity** → Is vessel traffic normal at high-risk straits?

**If BDI is BULLISH:**
→ Go to Baltic Dry Index → Check if you need to lock in freight contracts now

**If a route is DISRUPTED:**
→ Go to Route Disruption → See which commodities and alternate routes exist
→ Go to Strait Monitor → See the specific chokepoint's score and weather

**If a port shows weather alert:**
→ Go to Port Risk → See if the port handles commodities in your supply chain
→ Check Trade Exposure column for $B at risk

**If vessel counts drop at a strait:**
→ This is often the EARLIEST signal of disruption — ships are rerouting before news confirms it

---

## Data freshness

| Source | Update frequency | How |
|--------|-----------------|-----|
| BDI | Monthly or as needed | Manual: download CSV from investing.com, re-run notebook 02, commit |
| Port weather | Daily, weekdays 20:00 UTC | Automatic — GitHub Actions |
| Strait conditions | Daily, weekdays 20:00 UTC | Automatic — GitHub Actions |
| Vessel positions | Daily, weekdays 20:00 UTC | Automatic — GitHub Actions (4-min AIS collection) |
| Trade flows (Comtrade) | Annual | Manual: re-run notebook 01, commit |

---

## Glossary

| Term | Plain language definition |
|------|--------------------------|
| **BDI** | Baltic Dry Index — daily price to rent a bulk cargo ship |
| **Beaufort scale** | Wind strength scale 0–12. 7+ affects port operations |
| **Charter** | Renting a ship for a specific voyage or time period |
| **Chokepoint** | A narrow waterway all ships must use — disrupting it affects global trade |
| **HS code** | International product category code for trade statistics |
| **Knot (kn)** | Ship speed unit. 1 knot = 1.85 km/h |
| **MMSI** | Ship's unique ID number, like a license plate |
| **Rerouting** | Ships going a longer alternative path to avoid a dangerous area |
| **Spot market** | Booking a ship at today's price instead of a long-term contract |
| **VLCC** | Very Large Crude Carrier — largest oil tanker type |

---

*Maritime Intel Dashboard · MSBA 305 · Data sources: UN Comtrade, BDI (investing.com), OpenWeatherMap, AISStream · Storage: Google BigQuery · 12 analytical tables*
