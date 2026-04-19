# Maritime Shipping Intelligence Dashboard — User Guide
**MSBA 305 | Maritime Intel App**

---

## What is this dashboard?

This dashboard gives you a **daily morning briefing on global shipping conditions**. It combines 4 live data sources — trade flows, freight costs, weather, and vessel tracking — to answer one simple question:

> **"Is global shipping operating normally today, and if not, where and why?"**

You don't need to know anything about shipping to use it. This guide explains everything in plain language.

---

## How to open the dashboard

1. Open Git Bash in your repo folder
2. Run: `python -m streamlit run dashboard/app.py`
3. Open your browser at `http://localhost:8501`
4. In the left sidebar: toggle **Connect to BigQuery**, enter your Project ID and credentials to load live data

---

## The 20 monitored ports — why these specifically?

Yes, 20 ports are monitored. These are **the 20 busiest container and bulk cargo ports in the world by annual throughput (TEUs and tonnage)**, chosen because disruptions at these ports affect the largest share of global trade. They are:

Shanghai, Singapore, Ningbo-Zhoushan, Shenzhen, Guangzhou, Busan, Tianjin, Hong Kong, Rotterdam, Dubai, Port Klang, Antwerp, Xiamen, Los Angeles, Hamburg, Long Beach, Tanjung Pelepas, Kaohsiung, Dalian, New York.

Together these 20 ports handle approximately **60% of global container trade**. If a port not on this list is disrupted, it has a much smaller impact on global supply chains.

---

## The 8 critical straits — why these 8?

The 8 straits are the world's most important **maritime chokepoints** — narrow waterways where massive amounts of trade must pass through. They were chosen because each one, if blocked, would cause immediate global disruption.

| Strait | Location | Why it matters |
|--------|----------|----------------|
| **Strait of Hormuz** | Between Iran and Oman | 20% of all global oil passes here. Every tanker from Saudi Arabia, Kuwait, UAE, and Iraq must use it. |
| **Strait of Malacca** | Between Malaysia and Indonesia | The busiest strait in the world by ship count (~100,000 ships/year). Almost everything between Asia and Europe/Middle East uses it. |
| **Suez Canal** | Egypt | Ships save ~14 days vs going around Africa. 12% of world trade passes through. |
| **Bab el-Mandeb** | Between Yemen and Djibouti | The southern entrance to Suez. Ships coming from Asia must pass here before reaching the canal. |
| **Bosphorus Strait** | Turkey | Connects Black Sea to Mediterranean. Critical for Russian and Ukrainian grain and oil exports. |
| **Strait of Gibraltar** | Between Spain and Morocco | Gateway between Atlantic and Mediterranean. All Europe-Asia traffic via Suez must pass through. |
| **Strait of Dover** | Between UK and France | The world's busiest shipping lane by vessel count — over 500 ships per day. |
| **Lombok Strait** | Indonesia | Alternative to Malacca for supertankers (VLCCs) too large or fully laden to safely use Malacca. |

---

## Disruption score — what does "above 40" mean?

The disruption score (0–100) combines **weather conditions + geopolitical risk baseline** for each strait. It is not just weather — geopolitical risk is built in from the start (for example, Bab el-Mandeb starts at 40/100 even on a perfectly calm day because of active regional conflict risk).

| Score range | Color | What it means | What to do |
|-------------|-------|---------------|------------|
| 0–20 | 🟢 Green | Normal conditions | Nothing — standard operations |
| 21–40 | 🟢 Green | Moderate — slightly elevated | Monitor, no action needed |
| 41–60 | 🟠 Orange | **Elevated risk** | Plan contingency routes, factor into logistics |
| 61–100 | 🔴 Red | **Critical** | Rerouting likely, expect cost and time increases |

**Why 40 as the threshold?** A score above 40 means the combination of weather and geopolitical factors is significant enough to meaningfully affect shipping decisions. Below 40, the risk is present but manageable within normal operations.

---

## The 5 commodity categories (HS codes)

The dashboard tracks these 5 product categories from UN Comtrade:

| Code | Name | What it includes | Why tracked |
|------|------|-----------------|-------------|
| **HS 10** | Cereals | Wheat, rice, corn, barley | Transported on bulk carriers; highly sensitive to BDI and port disruptions |
| **HS 26** | Ores & slag | Iron ore, copper ore, bauxite | Very high volume, bulk ships; main cargo from Australia/Brazil to China |
| **HS 27** | Mineral fuels | Crude oil, natural gas, coal | Transported by tankers; directly affected by Hormuz/Bab el-Mandeb disruptions |
| **HS 72** | Iron & steel | Steel products, pig iron | Heavy bulk cargo; connects China with global markets |
| **HS 89** | Ships & boats | Vessels themselves | Tracks the shipbuilding industry (South Korea, China, Japan) |

These 5 categories were chosen because they represent **the majority of bulk and tanker cargo** — the types of shipping directly measured by the Baltic Dry Index.

---

## BDI signals — BULLISH, BEARISH, NEUTRAL explained

The Baltic Dry Index (BDI) is the daily price to rent a bulk cargo ship. It moves based on supply (how many ships are available) and demand (how much cargo needs shipping).

| Signal | What it means | What to do |
|--------|--------------|------------|
| 🔺 **BULLISH** | Rates are rising quickly — more demand than supply | **Lock in long-term charter contracts now** before rates get more expensive. Avoid the spot market. |
| 🔻 **BEARISH** | Rates are falling — more ships than cargo | **Use spot market** — wait and book at cheaper rates. Don't commit to long-term charters now. |
| ↔ **NEUTRAL** | No clear direction — market is balanced | Monitor weekly. No urgent action needed. |
| ⚠ **OVERBOUGHT** | Rates are historically very high | Consider **hedging** — shipping costs may be unsustainable and could fall. |
| 💡 **OVERSOLD** | Rates are historically very cheap | Excellent time to **lock in future cargo capacity** at low rates. |

**Simple analogy:** Think of BDI like petrol prices. BULLISH = prices at the pump are rising fast, fill up now. BEARISH = prices falling, wait before filling up.

---

## 🟡 WATCH status — what does it mean?

**WATCH** (yellow) means conditions are above normal but not yet at a level requiring immediate action. It is a warning to:
- Check this route/strait daily over the next few days
- Have a contingency routing plan ready
- Avoid committing to tight delivery deadlines through this area

Think of it like an **amber traffic light** — not stopped, but slow down and be prepared.

---

## The 8 pages — what each one tells you

---

### 📋 1. Executive Summary
**Your daily morning briefing — start here every day.**

This page shows the most important numbers at a glance:

| KPI | What it means |
|-----|--------------|
| **BDI** | Today's freight cost index. Higher = more expensive to ship. |
| **Ports at risk** | How many of the 20 monitored ports have dangerous wind (Beaufort ≥ 7 = near gale) |
| **Straits elevated risk** | How many of the 8 chokepoints have a score above 40/100 |
| **Total export value** | Total value of tracked commodities across all years in the database |
| **Vessels tracked (AIS)** | Number of unique ships captured in today's AIS collection |
| **BDI Signal** | Today's market signal: BULLISH / BEARISH / NEUTRAL (explained above) |

**Below the KPIs:**
- 🔴/🟡/🟢 **Route status** — the 6 main global shipping lanes
- **Port Weather Alerts** — any port with dangerous wind conditions
- **Strait Conditions** — all 8 chokepoints with scores
- **BDI sparkline** — last 90 days trend

**Morning routine:**
1. Is any route 🔴 DISRUPTED? → Go to Route Disruption page
2. Is BDI signal BULLISH? → Go to Baltic Dry Index page
3. Are 3+ straits above 40? → Go to Strait Monitor page

---

### ⚡ 2. Strait Monitor
**Real-time status of the 8 most critical chokepoints.**

**Key metrics explained:**
- **Straits monitored**: Always 8
- **Critical / High risk**: How many straits have a score above 40 out of 8. If this says "1 / 4" it means 1 is critical and 4 are elevated. A ratio above 4/8 = more than half the world's chokepoints are at risk — serious situation.
- **Trade% at elevated risk**: What percentage of world seaborne trade passes through straits with score >40. If 46%, nearly half of all global trade is at some risk of disruption.
- **Oil% at elevated risk**: What percentage of world oil supply passes through straits with score >40. This directly affects energy prices globally.

**The disruption score bar chart**: All 8 straits ranked. The dashed line at 40 = elevated risk threshold. The line at 60 = critical threshold.

**The map**: Each strait shown as a colored dot on the world map — 🔴 critical, 🟠 elevated, 🟢 normal.

---

### 🚢 3. Route Disruption
**Daily status of the 6 key global trade lanes.**

| Metric | Explanation |
|--------|-------------|
| **Routes monitored** | Always 6 |
| **Disrupted / Watch** | How many routes are disrupted vs on watch. "3 / 1" means 3 disrupted, 1 on watch. More than 2 disrupted = serious supply chain pressure globally. |
| **Avg strait score** | Average disruption score across all 6 route straits. Above 40/100 = generally elevated conditions worldwide. |

**Status labels:**
- 🔴 **DISRUPTED** — score ≥ 40 or critical geopolitical risk. Ships are likely rerouting.
- 🟡 **WATCH** — score between 20–40. Conditions worth monitoring daily.
- 🟢 **CLEAR** — score below 20. Normal operations.

---

### 📈 4. Baltic Dry Index (BDI)
**The price of shipping — is freight getting cheaper or more expensive?**

**Key numbers:**
- **Current BDI**: Today's value. Historical average ~1,500. Above 2,500 = expensive. Below 800 = cheap. A context line below the KPIs shows exactly how today's BDI compares to the 10-year average in %.
- **Period high/low**: The range over your selected date range
- **Spikes/Drops**: Days when BDI moved >5% in one day — indicates volatility

**Seasonal pattern (Monthly BDI Seasonality chart):**
- 🟢 **Cheapest months**: January, February, March — post-Chinese New Year slowdown in factory output
- 🟠 **Most expensive**: September, October, November — pre-holiday season restocking drives demand

**Charter Recommendations table:**
- **BEARISH** → Use spot market, avoid long-term charter
- **BULLISH** → Lock in long-term charter now
- **NEUTRAL** → Monitor, no action
- **OVERBOUGHT** → Consider hedging
- **OVERSOLD** → Good entry point for future capacity

---

### 🌍 5. Trade Analysis
**Who exports what, how much, and where?**

**Top 15 Exporters**: USA ranks #1 mainly due to LNG (liquefied natural gas = HS 27) exports. Saudi Arabia and Australia are #2 and #3 from mineral fuels and iron ore.

**Trade Balance chart**: 🟢 Green = net exporter (sells more than it buys). 🔴 Red = net importer (buys more than it sells). China being a large net importer in these categories means it relies heavily on incoming bulk cargo — making Chinese port weather critically important.

**BDI vs Trade Correlation chart**: Blue bars = total export value by year (left axis). Single orange line = average annual BDI (right axis). When both rise together = healthy demand-driven shipping market. When BDI rises but trade falls = shipping is expensive due to supply shortage, not real demand.

**Year-on-Year Heatmap**: 🟢 Green = export growth that year. 🔴 Red = export decline. NaN = data not submitted to UN Comtrade that year.

---

### ⚓ 6. Port Risk
**Weather at the 20 most important ports.**

**Beaufort wind scale — when does it matter?**

| Beaufort | Wind speed | Impact on shipping |
|----------|-----------|-------------------|
| 0–3 | 0–5 m/s | No effect |
| 4–6 | 6–12 m/s | Light caution |
| **7** | **14–17 m/s** | ⚠ **Near Gale — crane operations may stop, loading/unloading delayed** |
| 8–9 | 18–24 m/s | Port operations severely disrupted |
| 10–12 | 25+ m/s | Port may close entirely |

**Why Beaufort 7 is the threshold**: At 7 (Near Gale, 14+ m/s), most port cranes must stop operating for safety. Container ships cannot safely berth or depart. This is the internationally accepted threshold where port operations become seriously affected.

**"Trade: N/A"**: Means the port's country didn't match a trade-flow record in that filter — weather data is still real.

---

### 🛥 7. Vessel Activity
**Where are ships right now — across 8 straits and major ports globally.**

Vessel data is collected automatically every weekday at 20:00 UTC via a 4-minute AIS collection window.

**Key metrics:**
- **Total messages**: Raw AIS position signals received
- **Unique vessels**: Distinct ships identified
- **Moving vessels**: Ships with speed > 0.5 knots
- **Avg speed**: In knots (1 knot = 1.85 km/h)
- **Tankers**: Oil/chemical tankers specifically

**Congestion alerts**: High vessel count at a port = 🟡 CONGESTION warning. Rotterdam at 3,000+ vessels is normal — it's Europe's busiest port. CONGESTION combined with bad weather = serious delay risk.

**Vessel positions map**: World map showing all tracked vessels. Hover to see name, speed, status, destination. Larger dot = moving vessel.

**Speed categories:**
- **Stationary** (< 0.5 kn): At anchor or moored
- **Slow/manoeuvring** (0.5–3 kn): Entering or leaving port
- **Transit** (3–10 kn): Normal sailing
- **Cruising** (> 10 kn): Open ocean passage

**Top vessel destinations chart**: Shows the 15 most common declared destinations from ship AIS transponders. Useful for understanding where cargo is actually heading — confirms or challenges route disruption signals. Destinations starting with "@" are AIS padding and are filtered out automatically.

**Speed distribution histogram**: Shows how fast moving vessels are travelling. A large spike in the "Slow" range at a specific strait or port = congestion. Normal distribution centered around 8–12 knots = healthy traffic flow.

**Strait Vessel Traffic Trends** (below the map):
Daily vessel counts per strait over time. This is one of the most powerful early-warning signals available:

| Pattern | What it means |
|---------|---------------|
| Count dropping at Hormuz | Ships avoiding Persian Gulf — potential early disruption signal |
| Tanker count drops, cargo stays normal | Energy-specific threat, not a general disruption |
| Count rising sharply | Unusual congestion — possible port or strait delays |
| All counts stable | Normal global shipping |

A drop in vessel count at a critical strait often appears **2–7 days before official news confirms** a disruption.

> **Coverage note:** AIS signals require land-based receivers within ~40–50 nautical miles. The Persian Gulf, Red Sea, and Arabian Sea are not covered by AISStream's free tier infrastructure. Satellite AIS providers (e.g., Spire Maritime) would be required for those regions.

---

### 🔗 8. Cross-Source Insights
**What happens when you combine all 4 data sources together?**

**📦 Commodity Freight Cost tab:**
For each commodity, how does freight cost (BDI proxy) compare to trade value?
- High ratio = shipping costs eating into profit margins
- **Cereals (HS 10) and Ores (HS 26)** are most sensitive — cheap bulk cargo, so freight is a large % of value
- **Mineral fuels (HS 27)** are least sensitive — oil is so valuable that freight is a small % even when expensive

**🇨🇳 China Concentration tab:**
What % of each commodity's global exports does China control?
- High China % + disrupted Chinese ports = immediate global supply chain impact
- This is why Shanghai and Shenzhen weather matters for the whole world

**📅 Booking Calendar tab:**
Based on 10 years of BDI data — when is the cheapest time to book cargo?
- 🟢 Green months = historically cheap (book here if possible)
- 🟠 Orange months = historically expensive (avoid if possible)
- Pattern: cheapest Jan–Mar, most expensive Sep–Nov

**📊 All Analytical Tables tab:**
Technical view of all 12 analytical tables with descriptions. For data analysts.

---

## Quick Reference — 3-step morning check

**Every morning:**
1. **Executive Summary** → Any 🔴 disrupted routes? BDI signal BULLISH?
2. **Strait Monitor** → Any straits above 60/100 (critical)?
3. **Vessel Activity** → Is vessel count at key straits dropping?

**If BDI is BULLISH** → Go to Baltic Dry Index → Consider locking in freight contracts

**If a route is DISRUPTED** → Go to Route Disruption → Check which commodities affected and alternate routes

**If a port shows weather alert** → Go to Port Risk → See trade exposure in $B at risk

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
| **Beaufort scale** | Wind strength scale 0–12. 7+ (Near Gale, 14+ m/s) affects port crane operations |
| **BULLISH** | Market signal: freight rates rising — lock in contracts now |
| **BEARISH** | Market signal: freight rates falling — use spot market, wait for cheaper rates |
| **Charter** | Renting a ship for a specific voyage or time period |
| **Chokepoint** | A narrow waterway all ships must use — disrupting it affects global trade |
| **Congestion** | Too many ships at a port — causes delays for all vessels waiting to berth |
| **HS code** | Harmonized System — international product category code for trade statistics |
| **Knot (kn)** | Ship speed unit. 1 knot = 1.85 km/h |
| **MMSI** | Maritime Mobile Service Identity — a ship's unique ID number, like a license plate |
| **Rerouting** | Ships taking a longer alternative path to avoid a dangerous or blocked area |
| **Spot market** | Booking a ship at today's market price instead of a pre-agreed long-term rate |
| **TEU** | Twenty-foot Equivalent Unit — standard container size, used to measure port volume |
| **VLCC** | Very Large Crude Carrier — the largest type of oil tanker |

---

*Maritime Intel Dashboard · MSBA 305 · Data sources: UN Comtrade, BDI (investing.com), OpenWeatherMap, AISStream · Storage: Google BigQuery · 12 analytical tables*
