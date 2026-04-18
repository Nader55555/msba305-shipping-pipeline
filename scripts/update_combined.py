"""
scripts/update_combined.py
Merge all 4 clean sources into meaningful ANALYTICAL tables (not a raw UNION ALL).

Run automatically after ingest_weather.py in GitHub Actions.
Can also be run manually: python scripts/update_combined.py

Sources:
  data/clean/un_comtrade_clean.csv   — static (from notebooks, annual)
  data/clean/bdi_clean.csv           — manual update (see README, re-run notebook 02)
  data/clean/ais_latest.csv          — updated daily by ingest_ais.py (falls back to aisstream_clean.csv)
  data/clean/port_weather_clean.csv  — updated daily by ingest_weather.py
  data/clean/strait_conditions.csv   — updated daily by ingest_weather.py

Analytical tables produced (BigQuery, WRITE_TRUNCATE):
  1.  analysis_bdi_trade         — BDI × Comtrade JOIN by year
  3.  analysis_port_risk_trade   — Port weather × Trade JOIN by country_iso
  4.  analysis_strait_monitor    — Strait conditions + trade exposure enrichment
  5.  analysis_commodity_bdi     — Each HS commodity trade value × annual BDI cost burden
  6.  analysis_bdi_signals       — Bull/bear/neutral market signal timeline
  7.  analysis_net_exporter_risk — Net exporter countries + current port weather risk
  8.  analysis_seasonal_freight  — Monthly BDI seasonality × commodity trade patterns
  9.  analysis_china_concentration — China's export share per HS code + BDI impact
  10. analysis_vessel_port_risk  — AIS vessel density × port weather (congestion + weather)
  11. analysis_route_disruption  — Key trade routes × strait + port conditions today
  12. analysis_strait_vessel_trend — Daily vessel counts per strait (trend over time)
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from google.cloud import bigquery

BQ_PROJECT = os.environ.get("BQ_PROJECT", os.environ.get("BIGQUERY_PROJECT", "your-project-id"))
BQ_DATASET = os.environ.get("BQ_DATASET", os.environ.get("BIGQUERY_DATASET", "shipping_data"))
CLEAN_DIR  = "data/clean"

os.makedirs(CLEAN_DIR, exist_ok=True)

print("=" * 60)
print("UPDATE COMBINED — Analytical shipping tables")
print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
print("=" * 60)


# ── HELPERS ───────────────────────────────────────────────────────────────────
def load(name: str, path: str) -> pd.DataFrame:
    if os.path.exists(path):
        df = pd.read_csv(path, low_memory=False)
        print(f"  ✓ Loaded {name}: {len(df):,} rows × {df.shape[1]} cols")
        return df
    print(f"  ✗ SKIP {name}: {path} not found")
    return pd.DataFrame()


def upload(df: pd.DataFrame, table: str, disposition: str = "WRITE_TRUNCATE") -> None:
    try:
        client   = bigquery.Client(project=BQ_PROJECT)
        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table}"
        job_cfg  = bigquery.LoadJobConfig(
            write_disposition=getattr(bigquery.WriteDisposition, disposition),
            autodetect=True,
        )
        job = client.load_table_from_dataframe(df, table_id, job_config=job_cfg)
        job.result()
        print(f"  ↑ {table}: {len(df):,} rows")
    except Exception as e:
        print(f"  ✗ {table} upload failed: {e}")


def save(df: pd.DataFrame, filename: str) -> None:
    path = f"{CLEAN_DIR}/{filename}"
    df.to_csv(path, index=False)
    print(f"  ✎ Saved {filename} ({len(df):,} rows)")


HS_NAMES = {10: "Cereals", 26: "Ores & slag", 27: "Mineral fuels",
            72: "Iron & steel", 89: "Ships & boats"}


# ── 1. LOAD ALL SOURCES ───────────────────────────────────────────────────────
print("\n[1] Loading clean sources...")
comtrade = load("comtrade", f"{CLEAN_DIR}/un_comtrade_clean.csv")
bdi      = load("bdi",      f"{CLEAN_DIR}/bdi_clean.csv")
weather  = load("weather",  f"{CLEAN_DIR}/port_weather_clean.csv")
# AIS: prefer fresh daily snapshot from ingest_ais.py; fall back to static notebook CSV
_ais_latest = f"{CLEAN_DIR}/ais_latest.csv"
_ais_static = f"{CLEAN_DIR}/aisstream_clean.csv"
if os.path.exists(_ais_latest):
    ais = load("ais (daily)", _ais_latest)
else:
    ais = load("ais (static)", _ais_static)
straits  = load("straits",  f"{CLEAN_DIR}/strait_conditions.csv")

# Parse dates
if not bdi.empty and "date" in bdi.columns:
    bdi["date"] = pd.to_datetime(bdi["date"], errors="coerce")
    bdi["year"] = bdi["date"].dt.year
if not comtrade.empty and "year" in comtrade.columns:
    comtrade["year"] = pd.to_numeric(comtrade["year"], errors="coerce")
if not comtrade.empty and "hs_code" in comtrade.columns:
    comtrade["hs_code"] = pd.to_numeric(comtrade["hs_code"], errors="coerce")
    comtrade["commodity_name"] = comtrade["hs_code"].map(HS_NAMES).fillna("Other")





# ── 2. UPLOAD BDI SOURCE TABLE TO BIGQUERY ───────────────────────────────────
# bdi_clean.csv is manually updated (notebook 02 + investing.com download).
# Uploading it here keeps BigQuery bdi_daily in sync every daily run,
# so SQL queries and the dashboard always reflect the latest committed CSV.
print("\n[2] Uploading bdi_daily source table to BigQuery...")
if not bdi.empty:
    upload(bdi, "bdi_daily", "WRITE_TRUNCATE")
else:
    print("  ✗ bdi_clean.csv not found — bdi_daily not updated")


# ══════════════════════════════════════════════════════════════════════════════
# ANALYTICAL TABLES (proper JOINs)
# ══════════════════════════════════════════════════════════════════════════════

print("\n[3] Building analytical tables...")


# ── TABLE 1: BDI × Trade annual correlation ───────────────────────────────────
# Business Q: Does BDI predict global trade volume? When BDI spikes, which
# commodities are most affected? Used for freight cost forecasting and
# charter rate negotiation timing.
print("\n  [A] analysis_bdi_trade  (BDI × Comtrade JOIN by year)")

if not bdi.empty and not comtrade.empty:
    bdi_yr = (
        bdi.groupby("year")
        .agg(
            avg_bdi        =("bdi_value",       "mean"),
            max_bdi        =("bdi_value",        "max"),
            min_bdi        =("bdi_value",        "min"),
            bdi_volatility =("bdi_value",        "std"),
            spike_days     =("is_spike",         "sum"),
            drop_days      =("is_drop",          "sum"),
            trading_days   =("bdi_value",      "count"),
        )
        .reset_index()
    )
    bdi_yr["avg_bdi"]        = bdi_yr["avg_bdi"].round(2)
    bdi_yr["bdi_volatility"] = bdi_yr["bdi_volatility"].round(2)

    trade_yr = (
        comtrade[comtrade["flow_direction"] == "Export"]
        .groupby("year")
        .agg(
            export_value_usd  =("trade_value_usd",  "sum"),
            unique_countries  =("reporter_iso",   "nunique"),
            commodity_count   =("hs_code",        "nunique"),
        )
        .reset_index()
    )
    trade_yr["export_value_trillion"] = (trade_yr["export_value_usd"] / 1e12).round(3)

    # Dominant HS code per year by value
    dominant_hs = (
        comtrade[comtrade["flow_direction"] == "Export"]
        .groupby(["year", "hs_code"])["trade_value_usd"]
        .sum()
        .reset_index()
        .sort_values(["year", "trade_value_usd"], ascending=[True, False])
        .drop_duplicates("year")
        .rename(columns={"hs_code": "dominant_hs", "trade_value_usd": "dominant_hs_value"})
    )
    dominant_hs["dominant_commodity"] = dominant_hs["dominant_hs"].map(HS_NAMES).fillna("Other")

    # Merge BDI + Trade + dominant commodity
    t1 = pd.merge(bdi_yr, trade_yr, on="year", how="inner")
    t1 = pd.merge(t1, dominant_hs[["year","dominant_commodity","dominant_hs"]], on="year", how="left")

    # YoY changes
    t1 = t1.sort_values("year")
    t1["bdi_yoy_pct"]   = t1["avg_bdi"].pct_change() * 100
    t1["trade_yoy_pct"] = t1["export_value_trillion"].pct_change() * 100
    t1["bdi_yoy_pct"]   = t1["bdi_yoy_pct"].round(2)
    t1["trade_yoy_pct"] = t1["trade_yoy_pct"].round(2)

    # Signal: do they move together?
    t1["correlation_signal"] = t1.apply(
        lambda r: "Both rising"   if r.bdi_yoy_pct > 2 and r.trade_yoy_pct > 2
             else "Both falling"  if r.bdi_yoy_pct < -2 and r.trade_yoy_pct < -2
             else "BDI up, trade down (freight squeeze)"
                  if r.bdi_yoy_pct > 5 and r.trade_yoy_pct < 0
             else "BDI down, trade up (cheap shipping)"
                  if r.bdi_yoy_pct < -5 and r.trade_yoy_pct > 0
             else "Neutral / mixed",
        axis=1,
    )

    # Estimated freight cost burden (bdi per unit of trade)
    t1["freight_intensity"] = (
        t1["avg_bdi"] / (t1["export_value_trillion"] * 1000)
    ).round(4)

    t1["last_updated"] = datetime.now().strftime("%Y-%m-%d")
    save(t1, "analysis_bdi_trade.csv")
    upload(t1, "analysis_bdi_trade")
    print(f"  → {len(t1)} year-rows, signals: {t1.correlation_signal.value_counts().to_dict()}")


# ── TABLE 2: Port weather × Trade exposure ────────────────────────────────────
# Business Q: Which ports serve countries with the highest trade value
# and are currently facing adverse weather? Priority alert list for
# shipping operators and cargo insurance teams.
print("\n  [B] analysis_port_risk_trade  (Port weather × Trade JOIN by country_iso)")

if not weather.empty and not comtrade.empty:
    # Trade by country (both directions)
    trade_country = (
        comtrade[comtrade.get("year", pd.Series()).notna() if "year" in comtrade.columns else comtrade.index.notna()]
        .groupby("reporter_iso")
        .agg(
            total_export_usd  =("trade_value_usd",   lambda x: x[comtrade.loc[x.index, "flow_direction"] == "Export"].sum() if "flow_direction" in comtrade.columns else x.sum()),
            total_import_usd  =("trade_value_usd",   lambda x: x[comtrade.loc[x.index, "flow_direction"] == "Import"].sum() if "flow_direction" in comtrade.columns else 0),
            top_commodity     =("commodity_name",    lambda x: x.mode()[0] if len(x) > 0 else "Unknown"),
        )
        .reset_index()
        .rename(columns={"reporter_iso": "country_iso"})
    )

    # Simpler approach for trade by country
    exp_c = (
        comtrade[comtrade["flow_direction"] == "Export"]
        .groupby("reporter_iso")["trade_value_usd"]
        .sum()
        .reset_index()
        .rename(columns={"reporter_iso": "country_iso", "trade_value_usd": "total_export_usd"})
    )
    imp_c = (
        comtrade[comtrade["flow_direction"] == "Import"]
        .groupby("reporter_iso")["trade_value_usd"]
        .sum()
        .reset_index()
        .rename(columns={"reporter_iso": "country_iso", "trade_value_usd": "total_import_usd"})
    )
    trade_by_country = pd.merge(exp_c, imp_c, on="country_iso", how="outer").fillna(0)
    trade_by_country["total_trade_usd"]  = (
        trade_by_country["total_export_usd"] + trade_by_country["total_import_usd"]
    )
    trade_by_country["export_B"]  = (trade_by_country["total_export_usd"] / 1e9).round(2)
    trade_by_country["import_B"]  = (trade_by_country["total_import_usd"] / 1e9).round(2)
    trade_by_country["total_B"]   = (trade_by_country["total_trade_usd"]  / 1e9).round(2)
    trade_by_country["trade_position"] = trade_by_country.apply(
        lambda r: "Net Exporter" if r.total_export_usd > r.total_import_usd else "Net Importer", axis=1
    )

    # Weather (latest snapshot — most recent fetch_date per port)
    if "fetch_date" in weather.columns:
        wx_latest = (
            weather.sort_values("fetch_date", ascending=False)
            .drop_duplicates("port_name")
        )
    else:
        wx_latest = weather

    wx_cols = ["port_name", "country_iso", "port_rank", "temp_c", "wind_speed_ms",
               "beaufort_number", "beaufort_desc", "port_risk_flag", "low_visibility",
               "weather_main", "weather_desc", "visibility_m", "humidity_pct",
               "fetch_date", "fetched_at"]
    wx_cols = [c for c in wx_cols if c in wx_latest.columns]
    wx_latest = wx_latest[wx_cols].copy()

    t2 = pd.merge(wx_latest, trade_by_country, on="country_iso", how="left")

    # Business alert classification
    def port_alert(row):
        risk = bool(row.get("port_risk_flag", False))
        trade_B = float(row.get("total_B", 0) or 0)
        bf = int(row.get("beaufort_number", 0) or 0)
        if risk and trade_B > 500:
            return "🔴 CRITICAL — High-value port at risk"
        elif risk and trade_B > 100:
            return "🟠 HIGH — Major port at risk"
        elif risk:
            return "🟡 MODERATE — Port at risk, lower trade"
        elif bf >= 5 and trade_B > 200:
            return "🟡 WATCH — Fresh breeze at major port"
        else:
            return "🟢 NORMAL — Operations unaffected"

    t2["business_alert"]  = t2.apply(port_alert, axis=1)
    t2["alert_priority"]  = t2["business_alert"].map({
        "🔴 CRITICAL — High-value port at risk":  1,
        "🟠 HIGH — Major port at risk":            2,
        "🟡 MODERATE — Port at risk, lower trade": 3,
        "🟡 WATCH — Fresh breeze at major port":  4,
        "🟢 NORMAL — Operations unaffected":       5,
    })
    t2 = t2.sort_values(["alert_priority", "total_B"], ascending=[True, False])
    t2["last_updated"] = datetime.now().strftime("%Y-%m-%d")

    save(t2, "analysis_port_risk_trade.csv")
    upload(t2, "analysis_port_risk_trade")
    alerts = t2[t2.business_alert.str.startswith(("🔴","🟠"))]
    if not alerts.empty:
        for _, r in alerts.iterrows():
            print(f"  ⚠  {r['port_name']} ({r['country_iso']}) — {r['business_alert']}")


# ── TABLE 3: Strait monitor enriched ──────────────────────────────────────────
# Business Q: Which chokepoints are currently disrupted? What is the trade
# exposure? Used by logistics managers and freight forwarders to decide
# on rerouting.
print("\n  [C] analysis_strait_monitor  (Strait conditions + trade exposure)")

if not straits.empty:
    # Latest strait record per strait_name
    if "fetch_date" in straits.columns:
        st_latest = (
            straits.sort_values("fetch_date", ascending=False)
            .drop_duplicates("strait_name")
        )
    else:
        st_latest = straits.copy()

    t3 = st_latest.copy()

    # Estimated annual trade through each strait (billions USD, rough proxy)
    # Using trade_pct_global × ~$25T global seaborne trade
    GLOBAL_SEABORNE_TRADE_T = 25  # USD Trillion estimate
    t3["est_trade_through_B"] = (
        t3["trade_pct_global"] / 100 * GLOBAL_SEABORNE_TRADE_T * 1000
    ).round(0)

    # Daily trade disruption at risk (if fully closed)
    t3["daily_trade_at_risk_B"] = (t3["est_trade_through_B"] / 365).round(1)

    # Rerouting cost flag
    def reroute_note(row):
        geo = str(row.get("geopolitical_risk", ""))
        score = int(row.get("disruption_score", 0))
        name = str(row.get("strait_name", ""))
        if "Hormuz" in name:
            return "Cape of Good Hope or Suez (adds ~7 days, +$300K/voyage)"
        elif "Suez" in name or "Bab el-Mandeb" in name:
            return "Cape of Good Hope (adds ~14 days, +$1M/voyage est.)"
        elif "Malacca" in name:
            return "Lombok or Sunda Strait (adds ~2 days)"
        elif "Bosphorus" in name:
            return "No alternative — Black Sea landlocked"
        elif "Gibraltar" in name:
            return "No practical alternative for Mediterranean access"
        else:
            return "Alternative routing possible"

    t3["reroute_note"]  = t3.apply(reroute_note, axis=1)
    t3["combined_risk"] = t3.apply(
        lambda r: "🔴 CRITICAL" if r.get("risk_level") == "Critical"
            else  "🟠 HIGH"     if r.get("risk_level") == "High"
            else  "🟡 MODERATE" if r.get("risk_level") == "Moderate"
            else  "🟢 NORMAL",
        axis=1,
    )
    t3 = t3.sort_values("disruption_score", ascending=False)
    t3["last_updated"] = datetime.now().strftime("%Y-%m-%d")

    save(t3, "analysis_strait_monitor.csv")
    upload(t3, "analysis_strait_monitor")
    print(f"  → {len(t3)} straits | critical/high: "
          f"{t3[t3.risk_level.isin(['High','Critical'])].shape[0]}")


# ── TABLE 4: Commodity × BDI freight cost burden ─────────────────────────────
# Business Q: For each shipping commodity, how does the annual freight cost
# (BDI proxy) compare to trade value? High BDI years = shipping cost eats
# a larger share of low-margin commodities (e.g. cereals, ores).
print("\n  [D] analysis_commodity_bdi  (HS commodity × annual BDI)")

if not comtrade.empty and not bdi.empty:
    # Annual commodity trade value
    comm_yr = (
        comtrade[comtrade["flow_direction"] == "Export"]
        .groupby(["year", "hs_code", "commodity_name"])["trade_value_usd"]
        .sum()
        .reset_index()
    )
    comm_yr["export_B"] = (comm_yr["trade_value_usd"] / 1e9).round(2)

    # BDI annual
    bdi_a = bdi_yr[["year", "avg_bdi", "bdi_volatility", "spike_days"]].copy() if "bdi_yr" in dir() else (
        bdi.groupby("year").agg(avg_bdi=("bdi_value","mean"), bdi_volatility=("bdi_value","std")).round(2).reset_index()
    )

    t4 = pd.merge(comm_yr, bdi_a, on="year", how="inner")

    # Freight intensity proxy: BDI / commodity value
    # Higher = freight costs are proportionally higher for that commodity
    t4["freight_burden_index"] = (
        t4["avg_bdi"] / (t4["export_B"] + 0.01)
    ).round(4)

    # Commodity shipping sensitivity label
    # Cereals & ores = bulk cargo, highly BDI-sensitive
    # Fuels = tanker rates (VLCC/BDTI), less directly BDI-correlated
    SENSITIVITY = {10: "High (bulk, BDI-direct)", 26: "High (bulk, BDI-direct)",
                   27: "Medium (tanker, BDTI-related)", 72: "High (bulk/break-bulk)",
                   89: "Low (vessel itself)"}
    t4["bdi_sensitivity"] = t4["hs_code"].map(SENSITIVITY).fillna("Unknown")

    # Best year to ship (low BDI = cheap freight)
    best_bdi_yr = bdi_a.loc[bdi_a["avg_bdi"].idxmin(), "year"] if len(bdi_a) > 0 else None
    t4["is_cheapest_freight_year"] = t4["year"] == best_bdi_yr

    t4 = t4.sort_values(["commodity_name", "year"])
    t4["last_updated"] = datetime.now().strftime("%Y-%m-%d")

    save(t4, "analysis_commodity_bdi.csv")
    upload(t4, "analysis_commodity_bdi")
    print(f"  → {len(t4)} commodity-year rows across {t4.commodity_name.nunique()} commodities")


# ── TABLE 5: BDI market signals timeline ─────────────────────────────────────
# Business Q: When should you lock in charter rates? Bull signals = lock
# early (rates rising); bear signals = wait for spot market.
print("\n  [E] analysis_bdi_signals  (Bull/bear/neutral signal timeline)")

if not bdi.empty:
    t5 = bdi.copy()

    # Ensure date and rolling columns exist
    t5 = t5.sort_values("date")

    if "rolling_7d_avg" not in t5.columns:
        t5["rolling_7d_avg"]  = t5["bdi_value"].rolling(7,  min_periods=1).mean()
    if "rolling_30d_avg" not in t5.columns:
        t5["rolling_30d_avg"] = t5["bdi_value"].rolling(30, min_periods=1).mean()
    if "rolling_90d_avg" not in t5.columns:
        t5["rolling_90d_avg"] = t5["bdi_value"].rolling(90, min_periods=1).mean()
    if "daily_change_pct" not in t5.columns:
        t5["daily_change_pct"] = t5["bdi_value"].pct_change() * 100

    # Signal logic
    def market_signal(row):
        v   = row["bdi_value"]
        d7  = row.get("rolling_7d_avg", v)
        d30 = row.get("rolling_30d_avg", v)
        d90 = row.get("rolling_90d_avg", v)
        chg = row.get("daily_change_pct", 0) or 0
        if v > d7 and v > d30 and chg > 2:
            return "BULLISH"
        elif v < d7 and v < d30 and chg < -2:
            return "BEARISH"
        elif v > d90 * 1.15:
            return "OVERBOUGHT"
        elif v < d90 * 0.85:
            return "OVERSOLD"
        else:
            return "NEUTRAL"

    t5["market_signal"] = t5.apply(market_signal, axis=1)

    # Charter rate recommendation
    SIGNAL_ACTION = {
        "BULLISH":    "Lock in long-term charter now — rates rising",
        "BEARISH":    "Use spot market — avoid long-term charter",
        "OVERBOUGHT": "Rates above long-term avg — consider hedging",
        "OVERSOLD":   "Rates below long-term avg — good entry for charters",
        "NEUTRAL":    "Monitor — no clear directional signal",
    }
    t5["charter_recommendation"] = t5["market_signal"].map(SIGNAL_ACTION)

    # Keep only date + signal columns to keep table lean
    keep = ["date", "year", "bdi_value", "daily_change_pct",
            "rolling_7d_avg", "rolling_30d_avg", "rolling_90d_avg",
            "is_spike", "is_drop", "market_signal", "charter_recommendation"]
    keep = [c for c in keep if c in t5.columns]
    t5 = t5[keep].copy()

    save(t5, "analysis_bdi_signals.csv")
    upload(t5, "analysis_bdi_signals")
    sig_counts = t5["market_signal"].value_counts().to_dict()
    print(f"  → {len(t5)} daily signal rows: {sig_counts}")
    latest_sig = t5.sort_values("date").iloc[-1]
    print(f"  → Latest signal: {latest_sig['market_signal']} on {str(latest_sig['date'])[:10]} "
          f"(BDI={latest_sig['bdi_value']:.0f})")


# ── TABLE 6: Net exporter countries + current port risk ───────────────────────
# Business Q: Which net-exporting countries are currently exposed to port
# weather disruption? Prioritised list for cargo re-routing decisions and
# supply chain risk management.
print("\n  [F] analysis_net_exporter_risk  (Net exporter ranking + port weather today)")

if not comtrade.empty and not weather.empty:
    # Net trade balance by country
    exp = (
        comtrade[comtrade["flow_direction"] == "Export"]
        .groupby(["reporter_iso", "reporter_country"])["trade_value_usd"]
        .sum()
        .reset_index()
        .rename(columns={"trade_value_usd": "export_usd"})
    )
    imp = (
        comtrade[comtrade["flow_direction"] == "Import"]
        .groupby("reporter_iso")["trade_value_usd"]
        .sum()
        .reset_index()
        .rename(columns={"trade_value_usd": "import_usd"})
    )
    balance = pd.merge(exp, imp, on="reporter_iso", how="outer").fillna(0)
    balance["trade_balance_B"] = (
        (balance["export_usd"] - balance["import_usd"]) / 1e9
    ).round(2)
    balance["is_net_exporter"] = balance["export_usd"] > balance["import_usd"]

    # Commodity specialisation per country
    top_hs = (
        comtrade[comtrade["flow_direction"] == "Export"]
        .groupby(["reporter_iso", "commodity_name"])["trade_value_usd"]
        .sum()
        .reset_index()
        .sort_values("trade_value_usd", ascending=False)
        .drop_duplicates("reporter_iso")
        .rename(columns={"commodity_name": "top_export_commodity"})
    )
    balance = pd.merge(balance, top_hs[["reporter_iso","top_export_commodity"]],
                       on="reporter_iso", how="left")

    # Join with latest port weather (one port per country)
    wx_risk = (
        weather[["country_iso","port_name","port_rank","beaufort_number",
                 "beaufort_desc","port_risk_flag","wind_speed_ms","weather_main","fetch_date"]]
        .copy()
        if all(c in weather.columns for c in ["port_risk_flag","wind_speed_ms"])
        else weather[["country_iso","port_name"]].copy()
    )
    # Pick highest-ranked port per country
    if "port_rank" in wx_risk.columns:
        wx_risk = wx_risk.sort_values("port_rank").drop_duplicates("country_iso")

    t6 = pd.merge(
        balance,
        wx_risk,
        left_on="reporter_iso",
        right_on="country_iso",
        how="left",
    )
    t6 = t6.sort_values("export_usd", ascending=False)
    t6["weather_impact"] = t6.apply(
        lambda r: "Port disruption active" if bool(r.get("port_risk_flag", False))
            else "Moderate conditions"      if int(r.get("beaufort_number", 0) or 0) >= 5
            else "Normal operations",
        axis=1,
    )
    t6["last_updated"] = datetime.now().strftime("%Y-%m-%d")

    save(t6, "analysis_net_exporter_risk.csv")
    upload(t6, "analysis_net_exporter_risk")
    print(f"  → {len(t6)} countries ranked | "
          f"at-risk exporters: {int(t6[t6.weather_impact=='Port disruption active'].shape[0])}")


# ── TABLE 7: Seasonal freight patterns ────────────────────────────────────────
# Business Q: Which months have cheapest / most expensive freight?
# Which commodities peak when freight is most expensive?
# Used for cargo booking calendar optimisation.
print("\n  [G] analysis_seasonal_freight  (Monthly BDI × commodity trade patterns)")

if not bdi.empty and not comtrade.empty:
    # Monthly BDI stats
    bdi_m = bdi.copy()
    bdi_m["month"] = bdi_m["date"].dt.month
    MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    monthly_bdi = (
        bdi_m.groupby("month")
        .agg(
            avg_bdi   =("bdi_value", "mean"),
            min_bdi   =("bdi_value",  "min"),
            max_bdi   =("bdi_value",  "max"),
            std_bdi   =("bdi_value",  "std"),
            count_obs =("bdi_value","count"),
        )
        .round(1)
        .reset_index()
    )
    monthly_bdi["month_name"]    = monthly_bdi["month"].apply(lambda x: MONTHS[x-1])
    monthly_bdi["freight_level"] = monthly_bdi["avg_bdi"].apply(
        lambda v: "Expensive (high demand)" if v > monthly_bdi["avg_bdi"].quantile(0.67)
             else "Cheap (low demand)"       if v < monthly_bdi["avg_bdi"].quantile(0.33)
             else "Moderate"
    )

    # Seasonal commodity trade (which months have highest trade — approximated by HS code)
    # Comtrade is annual, so we use BDI month as shipping-cost proxy
    t7 = monthly_bdi.copy()
    t7["booking_advice"] = t7.apply(
        lambda r: "Book ahead — pre-holiday restocking demand"
                  if r.month in [9, 10, 11]
            else "Cheapest window — post-Chinese-New-Year lull"
                  if r.month in [2, 3]
            else "Moderate — check spot rates",
        axis=1,
    )
    t7["last_updated"] = datetime.now().strftime("%Y-%m-%d")

    save(t7, "analysis_seasonal_freight.csv")
    upload(t7, "analysis_seasonal_freight")
    cheap  = t7[t7.freight_level == "Cheap (low demand)"]["month_name"].tolist()
    costly = t7[t7.freight_level == "Expensive (high demand)"]["month_name"].tolist()
    print(f"  → Cheapest months: {cheap} | Most expensive: {costly}")


# ── TABLE 8: China concentration risk ────────────────────────────────────────
# Business Q: How dominant is China in each HS code export category?
# If China's ports face disruption, what is global supply chain exposure?
print("\n  [H] analysis_china_concentration  (China export share × BDI impact)")

if not comtrade.empty:
    exp_all = (
        comtrade[comtrade["flow_direction"] == "Export"]
        .groupby(["year", "hs_code", "commodity_name"])["trade_value_usd"]
        .sum()
        .reset_index()
        .rename(columns={"trade_value_usd": "world_export_usd"})
    )
    exp_china = (
        comtrade[
            (comtrade["flow_direction"] == "Export") &
            (comtrade["reporter_iso"] == "CHN")
        ]
        .groupby(["year", "hs_code"])["trade_value_usd"]
        .sum()
        .reset_index()
        .rename(columns={"trade_value_usd": "china_export_usd"})
    )

    t8 = pd.merge(exp_all, exp_china, on=["year","hs_code"], how="left").fillna(0)
    t8["china_share_pct"] = (
        t8["china_export_usd"] / t8["world_export_usd"].replace(0, np.nan) * 100
    ).round(1)

    # China port weather risk today
    china_wx = weather[weather["country_iso"] == "CN"].copy() if not weather.empty else pd.DataFrame()
    china_risk = False
    china_max_bf = 0
    if not china_wx.empty and "port_risk_flag" in china_wx.columns:
        china_risk   = bool(china_wx["port_risk_flag"].any())
        china_max_bf = int(china_wx["beaufort_number"].max() if "beaufort_number" in china_wx.columns else 0)

    t8["china_port_risk_today"]  = china_risk
    t8["china_max_beaufort"]     = china_max_bf
    t8["supply_concentration"]   = t8["china_share_pct"].apply(
        lambda x: "Critical (>50%)" if x > 50
            else  "High (30-50%)"   if x > 30
            else  "Moderate (15-30%)" if x > 15
            else  "Low (<15%)"
    )
    t8["disruption_exposure_B"] = (
        t8["china_export_usd"] / 1e9 * (china_max_bf / 12)
    ).round(2)  # Rough proxy: Bf12 = full disruption of China's exports

    # Add BDI signal for context
    if not bdi.empty:
        latest_bdi = float(bdi.sort_values("date").iloc[-1]["bdi_value"])
        t8["current_bdi"] = latest_bdi

    t8["last_updated"] = datetime.now().strftime("%Y-%m-%d")

    save(t8, "analysis_china_concentration.csv")
    upload(t8, "analysis_china_concentration")
    top_china = t8[t8.china_share_pct > 40].groupby("commodity_name")["china_share_pct"].max()
    print(f"  → China >40% share: {top_china.to_dict()}")


# ── TABLE 9: Vessel density × port weather ────────────────────────────────────
# Business Q: At ports where AIS shows high vessel density, does adverse
# weather create compounded risk (congestion + weather = major delay)?
print("\n  [I] analysis_vessel_port_risk  (AIS density × port weather)")

if not ais.empty and not weather.empty:
    # AIS: count vessels per port_guess (AIS already has port_guess column)
    if "port_guess" in ais.columns:
        ais_density = (
            ais.groupby("port_guess")
            .agg(
                vessel_count    =("mmsi",         "nunique"),
                tanker_count    =("vessel_category", lambda x: (x == "Tanker").sum()),
                cargo_count     =("vessel_category", lambda x: (x == "Cargo").sum()),
                moving_count    =("is_moving",    "sum"),
                avg_speed_knots =("sog_knots",    "mean"),
            )
            .reset_index()
            .rename(columns={"port_guess": "port_name"})
        )

        # Join with port weather
        t9 = pd.merge(ais_density, weather, on="port_name", how="inner")

        def congestion_weather_risk(row):
            vc  = int(row.get("vessel_count", 0))
            bf  = int(row.get("beaufort_number", 0) or 0)
            vis = int(row.get("visibility_m", 10000) or 10000)
            if vc > 50 and bf >= 6:
                return "🔴 HIGH — Dense traffic + strong winds"
            elif vc > 30 and vis < 5000:
                return "🟠 MODERATE — Dense traffic + limited visibility"
            elif bf >= 7:
                return "🟡 WEATHER — Near-gale conditions"
            elif vc > 50:
                return "🟡 CONGESTION — High vessel density"
            else:
                return "🟢 NORMAL"

        t9["congestion_weather_risk"] = t9.apply(congestion_weather_risk, axis=1)
        t9["avg_speed_knots"] = t9["avg_speed_knots"].round(1)
        t9["last_updated"] = datetime.now().strftime("%Y-%m-%d")

        save(t9, "analysis_vessel_port_risk.csv")
        upload(t9, "analysis_vessel_port_risk")
        print(f"  → {len(t9)} port-vessel-weather records")
    else:
        print("  ✗ Skipped: port_guess column not in AIS data")


# ── TABLE 10: Route disruption dashboard ─────────────────────────────────────
# Business Q: Give me ONE table I can look at every morning to see which
# global shipping routes are disrupted today. Combines straits + port risk.
print("\n  [J] analysis_route_disruption  (Daily route disruption overview)")

ROUTE_DEFINITIONS = [
    {
        "route":         "Asia → Europe (Suez)",
        "origin_ports":  ["Shanghai", "Ningbo-Zhoushan", "Shenzhen"],
        "dest_ports":    ["Rotterdam", "Antwerp", "Hamburg"],
        "strait":        "Suez Canal",
        "alt_strait":    "Bab el-Mandeb",
        "hs_codes":      [72, 89, 10],  # Steel, ships, cereals
        "notes":         "Main Asia-Europe container route; 14 days via Suez",
    },
    {
        "route":         "Middle East Oil → Asia",
        "origin_ports":  ["Dubai"],
        "dest_ports":    ["Shanghai", "Singapore", "Busan"],
        "strait":        "Strait of Hormuz",
        "alt_strait":    "Strait of Malacca",
        "hs_codes":      [27],  # Mineral fuels
        "notes":         "Persian Gulf → Malacca → East Asia VLCC route",
    },
    {
        "route":         "Asia intra-regional",
        "origin_ports":  ["Singapore", "Port Klang", "Tanjung Pelepas"],
        "dest_ports":    ["Hong Kong", "Shanghai", "Busan"],
        "strait":        "Strait of Malacca",
        "alt_strait":    "Lombok Strait",
        "hs_codes":      [72, 10, 26],
        "notes":         "Southeast Asia feeder and transhipment routes",
    },
    {
        "route":         "Atlantic / Europe ↔ Mediterranean",
        "origin_ports":  ["Hamburg", "Antwerp", "Rotterdam"],
        "dest_ports":    ["Dubai", "Singapore"],
        "strait":        "Strait of Gibraltar",
        "alt_strait":    None,
        "hs_codes":      [72, 89],
        "notes":         "Northern Europe ↔ Mediterranean / onward to Suez",
    },
    {
        "route":         "Black Sea Grain Exports",
        "origin_ports":  [],
        "dest_ports":    ["Rotterdam", "Hamburg", "Antwerp"],
        "strait":        "Bosphorus Strait",
        "alt_strait":    None,
        "hs_codes":      [10],  # Cereals
        "notes":         "Ukraine/Russia grain exports via Black Sea; war impact",
    },
    {
        "route":         "Trans-Pacific (Asia → US West Coast)",
        "origin_ports":  ["Shanghai", "Ningbo-Zhoushan", "Busan"],
        "dest_ports":    ["Los Angeles", "Long Beach"],
        "strait":        None,
        "alt_strait":    None,
        "hs_codes":      [72, 89, 26],
        "notes":         "No strait — open Pacific; port congestion is main risk",
    },
]

if not straits.empty or not weather.empty:
    route_rows = []
    st_map = {}
    if not straits.empty and "strait_name" in straits.columns:
        if "fetch_date" in straits.columns:
            st_latest_map = (
                straits.sort_values("fetch_date", ascending=False)
                .drop_duplicates("strait_name")
                .set_index("strait_name")
            )
        else:
            st_latest_map = straits.set_index("strait_name")
        st_map = st_latest_map.to_dict("index")

    wx_map = {}
    if not weather.empty and "port_name" in weather.columns:
        if "fetch_date" in weather.columns:
            wx_latest_map = (
                weather.sort_values("fetch_date", ascending=False)
                .drop_duplicates("port_name")
                .set_index("port_name")
            )
        else:
            wx_latest_map = weather.set_index("port_name")
        wx_map = wx_latest_map.to_dict("index")

    for route in ROUTE_DEFINITIONS:
        # Strait conditions
        s_data   = st_map.get(route["strait"], {}) if route["strait"] else {}
        s_score  = int(s_data.get("disruption_score", 0))
        s_risk   = str(s_data.get("risk_level", "Unknown"))
        s_geo    = str(s_data.get("geopolitical_risk", "Unknown"))
        s_bf     = int(s_data.get("beaufort_number", 0) or 0)

        # Origin port conditions
        origin_risks = []
        for p in route["origin_ports"]:
            p_data = wx_map.get(p, {})
            if bool(p_data.get("port_risk_flag", False)):
                origin_risks.append(p)

        # Destination port conditions
        dest_risks = []
        for p in route["dest_ports"]:
            p_data = wx_map.get(p, {})
            if bool(p_data.get("port_risk_flag", False)):
                dest_risks.append(p)

        # Overall route status
        if s_risk in ("Critical", "High") or s_geo == "Very High":
            overall = "🔴 DISRUPTED"
        elif s_risk == "Moderate" or origin_risks or dest_risks:
            overall = "🟡 WATCH"
        elif s_risk == "Normal" or not s_data:
            overall = "🟢 CLEAR"
        else:
            overall = "⚪ UNKNOWN"

        # Commodity exposure
        comm_names = [HS_NAMES.get(h, str(h)) for h in route["hs_codes"]]

        route_rows.append({
            "route":                route["route"],
            "strait":               route["strait"] or "None",
            "strait_disruption_score": s_score,
            "strait_risk_level":    s_risk,
            "geopolitical_risk":    s_geo,
            "strait_beaufort":      s_bf,
            "origin_ports_at_risk": ", ".join(origin_risks) or "None",
            "dest_ports_at_risk":   ", ".join(dest_risks)   or "None",
            "overall_status":       overall,
            "commodities_affected": ", ".join(comm_names),
            "route_notes":          route["notes"],
            "alt_route":            route["alt_strait"] or "None",
            "fetch_date":           datetime.now().strftime("%Y-%m-%d"),
            "last_updated":         datetime.now().strftime("%Y-%m-%d %H:%M"),
        })

    t10 = pd.DataFrame(route_rows)

    # Sort: disrupted first
    status_order = {"🔴 DISRUPTED": 0, "🟡 WATCH": 1, "🟢 CLEAR": 2, "⚪ UNKNOWN": 3}
    t10["_sort"] = t10["overall_status"].map(status_order).fillna(9)
    t10 = t10.sort_values("_sort").drop(columns="_sort")

    save(t10, "analysis_route_disruption.csv")
    upload(t10, "analysis_route_disruption")
    for _, r in t10.iterrows():
        print(f"  {r['overall_status']}  {r['route']}")


# ── DONE ─────────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("All analytical tables complete.")
print("\nTables in BigQuery:")
tables = [
    ("analysis_bdi_trade",        "BDI × annual trade volume (JOIN by year)"),
    ("analysis_port_risk_trade",  "Port weather × trade exposure (JOIN by country)"),
    ("analysis_strait_monitor",   "Strait chokepoint conditions + trade exposure"),
    ("analysis_commodity_bdi",    "HS commodity trade value × freight cost (BDI)"),
    ("analysis_bdi_signals",      "Daily bull/bear signal + charter recommendation"),
    ("analysis_net_exporter_risk","Net exporter rank + current port weather risk"),
    ("analysis_seasonal_freight", "Monthly BDI seasonality + booking calendar"),
    ("analysis_china_concentration","China supply concentration per HS code"),
    ("analysis_vessel_port_risk", "AIS vessel density × port weather"),
    ("analysis_route_disruption", "Daily route disruption overview (morning briefing)"),
]
for name, desc in tables:
    print(f"  {name:<35} — {desc}")
# ── TABLE 12: Strait vessel traffic trends ────────────────────────────────────
# Business Q: Is vessel traffic at Hormuz/Malacca/Suez increasing or decreasing
# over the past weeks? A drop in tanker count at Hormuz before official news
# often signals geopolitical escalation. Built from growing vessel_movements table.
print("\n  [K] analysis_strait_vessel_trend  (Daily vessel counts per strait)")

if not ais.empty and "port_guess" in ais.columns and "is_strait" in ais.columns:
    strait_ais = ais[ais["is_strait"] == True].copy() if "is_strait" in ais.columns else ais[ais["port_guess"].isin([
        "Strait of Hormuz","Strait of Malacca","Suez Canal","Bab el-Mandeb",
        "Strait of Gibraltar","Bosphorus Strait","Strait of Dover","Lombok Strait"
    ])]

    if not strait_ais.empty:
        date_col = "fetch_date" if "fetch_date" in strait_ais.columns else "event_time_utc"
        if date_col == "event_time_utc":
            strait_ais["fetch_date"] = pd.to_datetime(strait_ais[date_col], errors="coerce", utc=True).dt.strftime("%Y-%m-%d")

        t12 = (
            strait_ais.groupby(["fetch_date", "port_guess"])
            .agg(
                total_vessels  =("mmsi",             "nunique"),
                tanker_count   =("vessel_category",  lambda x: (x == "Tanker").sum()),
                cargo_count    =("vessel_category",  lambda x: (x == "Cargo").sum()),
                moving_count   =("is_moving",        "sum"),
                avg_speed_knots=("sog_knots",        "mean"),
            )
            .reset_index()
            .rename(columns={"port_guess": "strait_name"})
        )
        t12["avg_speed_knots"] = t12["avg_speed_knots"].round(1)
        t12["pct_moving"] = (t12["moving_count"] / t12["total_vessels"].replace(0, 1) * 100).round(1)
        t12["last_updated"] = datetime.now().strftime("%Y-%m-%d")
        save(t12, "analysis_strait_vessel_trend.csv")
        upload(t12, "analysis_strait_vessel_trend")
        print(f"  → {len(t12)} strait-date rows | {t12['strait_name'].nunique()} straits covered")
    else:
        print("  ✗ No strait AIS data found — skipping")
else:
    print("  ✗ AIS data missing or no is_strait column — skipping")


print("\nDone.")
