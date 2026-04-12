"""
dashboard/app.py
Maritime Shipping Intelligence Dashboard — MSBA 305
Run: streamlit run dashboard/app.py

8 pages:
  1. Executive Summary     — Morning briefing across all sources
  2. Strait Monitor        — 8 chokepoints: Hormuz, Malacca, Suez, Bab el-Mandeb...
  3. Route Disruption      — 6 key trade routes: are they clear today?
  4. Baltic Dry Index      — BDI trend, signals, charter recommendations
  5. Trade Analysis        — Comtrade flows, balance, commodity breakdown
  6. Port Risk             — 20 ports: weather + trade exposure
  7. Vessel Activity       — AIS positions, vessel types, speeds (Singapore)
  8. Cross-Source Insights — BDI×Trade, China concentration, seasonal booking
"""

import os
import warnings
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import streamlit as st
from datetime import datetime

warnings.filterwarnings("ignore")

# ── PAGE CONFIG ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Maritime Shipping Intelligence",
    page_icon="⚓",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap');
  html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
  [data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0a1628 0%, #0d2137 100%);
    border-right: 1px solid #1e3a5f;
  }
  [data-testid="stSidebar"] * { color: #c8d8e8 !important; }
  .main, [data-testid="stAppViewContainer"] { background-color: #060f1a; }
  [data-testid="block-container"] { padding: 1.5rem 2rem; }
  [data-testid="metric-container"] {
    background: linear-gradient(135deg, #0d2137 0%, #112840 100%);
    border: 1px solid #1e3a5f; border-radius: 12px; padding: 1rem 1.25rem;
  }
  [data-testid="metric-container"] label { color: #8aacc8 !important; font-size: 12px !important; }
  [data-testid="metric-container"] [data-testid="stMetricValue"] {
    color: #e8f0f8 !important; font-size: 24px !important; font-weight: 600 !important;
  }
  .section-header {
    font-size: 11px; font-weight: 600; letter-spacing: 1.5px; color: #4a7fa5;
    text-transform: uppercase; margin: 1.5rem 0 0.75rem;
    padding-bottom: 6px; border-bottom: 1px solid #1e3a5f;
  }
  .page-title  { font-size: 26px; font-weight: 600; color: #e8f0f8; margin-bottom: 4px; }
  .page-sub    { font-size: 13px; color: #4a7fa5; margin-bottom: 1.5rem; }
  .badge-red   { background:#4a1a1a; color:#ff6b6b; padding:3px 10px; border-radius:20px; font-size:11px; font-weight:600; }
  .badge-amber { background:#3a2a0a; color:#ffc65c; padding:3px 10px; border-radius:20px; font-size:11px; font-weight:600; }
  .badge-green { background:#1a3a1a; color:#6bcf7f; padding:3px 10px; border-radius:20px; font-size:11px; font-weight:600; }
  .badge-blue  { background:#0d2137; color:#7eb8e8; padding:3px 10px; border-radius:20px; font-size:11px; font-weight:600; }
  .alert-card  { padding:12px 16px; border-radius:10px; margin-bottom:6px; font-size:13px; line-height:1.5; }
  .alert-red   { background:rgba(74,26,26,0.5);  border:1px solid #4a1a1a; color:#e8c0c0; }
  .alert-amber { background:rgba(74,50,10,0.5);  border:1px solid #5a3a10; color:#e8d8c0; }
  .alert-green { background:rgba(20,50,20,0.5);  border:1px solid #1e4a1e; color:#c0e8c0; }
  .alert-blue  { background:rgba(13,33,75,0.5);  border:1px solid #1e3a7f; color:#c0d8e8; }
  hr { border-color: #1e3a5f; margin: 1rem 0; }
  .stSelectbox > div > div, .stMultiSelect > div > div {
    background: #0d2137 !important; border-color: #1e3a5f !important; color: #c8d8e8 !important;
  }
</style>
""", unsafe_allow_html=True)

# ── THEME ─────────────────────────────────────────────────────────────────────
DARK = dict(
    paper_bgcolor="#0d2137", plot_bgcolor="#0a1628",
    font=dict(color="#8aacc8", family="Inter, sans-serif", size=12),
    xaxis=dict(gridcolor="#1e3a5f", linecolor="#1e3a5f", zerolinecolor="#1e3a5f"),
    yaxis=dict(gridcolor="#1e3a5f", linecolor="#1e3a5f", zerolinecolor="#1e3a5f"),
    legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="#1e3a5f"),
    margin=dict(t=40, b=40, l=50, r=20),
)
C = dict(
    blue="#378ADD", teal="#1D9E75", amber="#EF9F27",
    coral="#D85A30", purple="#7F77DD", gray="#888780",
    green="#639922", red="#E24B4A", pink="#D4537E",
)
HS_NAMES = {10:"Cereals", 26:"Ores & slag", 27:"Mineral fuels",
            72:"Iron & steel", 89:"Ships & boats"}


# ── DATA LOADING ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def load_bigquery(project: str, dataset: str, key: str) -> dict:
    from google.cloud import bigquery
    from google.oauth2 import service_account
    creds  = service_account.Credentials.from_service_account_file(key)
    client = bigquery.Client(project=project, credentials=creds)
    tables = {}
    for t in ["bdi_daily", "trade_flows", "port_weather", "vessel_movements",
              "strait_conditions",
              "analysis_bdi_trade", "analysis_port_risk_trade",
              "analysis_strait_monitor", "analysis_commodity_bdi",
              "analysis_bdi_signals", "analysis_net_exporter_risk",
              "analysis_seasonal_freight", "analysis_china_concentration",
              "analysis_vessel_port_risk", "analysis_route_disruption"]:
        try:
            df = client.query(f"SELECT * FROM `{project}.{dataset}.{t}`").to_dataframe()
            df.columns = [c.lower() for c in df.columns]
            tables[t] = df
        except Exception as e:
            tables[t] = pd.DataFrame()
    return tables


@st.cache_data(ttl=3600, show_spinner=False)
def load_csv() -> dict:
    base  = os.path.join(os.path.dirname(__file__), "..", "data", "clean")
    files = {
        "bdi_daily":                   "bdi_clean.csv",
        "trade_flows":                 "un_comtrade_clean.csv",
        "port_weather":                "port_weather_clean.csv",
        "vessel_movements":            "aisstream_clean.csv",
        "strait_conditions":           "strait_conditions.csv",
        "analysis_bdi_trade":          "analysis_bdi_trade.csv",
        "analysis_port_risk_trade":    "analysis_port_risk_trade.csv",
        "analysis_strait_monitor":     "analysis_strait_monitor.csv",
        "analysis_commodity_bdi":      "analysis_commodity_bdi.csv",
        "analysis_bdi_signals":        "analysis_bdi_signals.csv",
        "analysis_net_exporter_risk":  "analysis_net_exporter_risk.csv",
        "analysis_seasonal_freight":   "analysis_seasonal_freight.csv",
        "analysis_china_concentration":"analysis_china_concentration.csv",
        "analysis_vessel_port_risk":   "analysis_vessel_port_risk.csv",
        "analysis_route_disruption":   "analysis_route_disruption.csv",
    }
    out = {}
    for key, fname in files.items():
        path = os.path.join(base, fname)
        out[key] = pd.read_csv(path, low_memory=False) if os.path.exists(path) else pd.DataFrame()
    return out


# ── SIDEBAR ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div style="font-size:22px;font-weight:700;color:#e8f0f8;margin-bottom:2px;">⚓ Maritime Intel</div>', unsafe_allow_html=True)
    st.markdown('<div style="font-size:11px;color:#4a7fa5;margin-bottom:20px;">MSBA 305 — Shipping Pipeline</div>', unsafe_allow_html=True)

    page = st.radio("Navigation", [
        "📊 Executive Summary",
        "🚧 Strait Monitor",
        "🛳  Route Disruption",
        "📈 Baltic Dry Index",
        "🌍 Trade Analysis",
        "🌦  Port Risk",
        "🛥  Vessel Activity",
        "🔗 Cross-Source Insights",
    ], label_visibility="collapsed")

    st.markdown('<div style="font-size:10px;font-weight:600;letter-spacing:1px;color:#4a7fa5;text-transform:uppercase;margin:20px 0 8px;">Data Connection</div>', unsafe_allow_html=True)
    use_bq = st.toggle("Connect to BigQuery", value=False)
    if use_bq:
        bq_project = st.text_input("Project ID", placeholder="msba305-shipping-123")
        bq_dataset = st.text_input("Dataset",    value="shipping_data")
        key_file   = st.text_input("Key file",   value="gcp_key.json")
    else:
        bq_project = bq_dataset = key_file = ""

    st.markdown('<div style="font-size:11px;color:#2a4a6a;margin-top:24px;line-height:1.6;">Sources: UN Comtrade · BDI (investing.com, manual) · OpenWeatherMap · AISStream<br>Storage: BigQuery | Dashboard: Streamlit</div>', unsafe_allow_html=True)


# ── LOAD ──────────────────────────────────────────────────────────────────────
with st.spinner("Loading data..."):
    if use_bq and bq_project and bq_dataset and key_file and os.path.exists(key_file):
        try:
            D = load_bigquery(bq_project, bq_dataset, key_file)
            src = "BigQuery"
        except Exception:
            D   = load_csv()
            src = "Local CSV (BigQuery failed)"
    else:
        D   = load_csv()
        src = "Local CSV"

bdi   = D.get("bdi_daily",    pd.DataFrame())
trade = D.get("trade_flows",  pd.DataFrame())
wx    = D.get("port_weather", pd.DataFrame())
ais   = D.get("vessel_movements", pd.DataFrame())
straits_raw = D.get("strait_conditions", pd.DataFrame())

# Parse types
for df, col in [(bdi, "date")]:
    if not df.empty and col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")
if not bdi.empty and "year" not in bdi.columns and "date" in bdi.columns:
    bdi["year"] = bdi["date"].dt.year
if not trade.empty and "year" in trade.columns:
    trade["year"] = pd.to_numeric(trade["year"], errors="coerce")
if not trade.empty and "hs_code" in trade.columns:
    trade["hs_code"] = pd.to_numeric(trade["hs_code"], errors="coerce")
    trade["commodity_name"] = trade["hs_code"].map(HS_NAMES).fillna("Other")
for num_col in ["beaufort_number","wind_speed_ms","temp_c","humidity_pct","visibility_m","disruption_score"]:
    for df in [wx, straits_raw]:
        if not df.empty and num_col in df.columns:
            df[num_col] = pd.to_numeric(df[num_col], errors="coerce")


def h(text): st.markdown(f'<div class="section-header">{text}</div>', unsafe_allow_html=True)
def title(t, sub=""): 
    st.markdown(f'<div class="page-title">{t}</div>', unsafe_allow_html=True)
    if sub: st.markdown(f'<div class="page-sub">{sub} · {src}</div>', unsafe_allow_html=True)

def alert_box(cls, text):
    st.markdown(f'<div class="alert-card alert-{cls}">{text}</div>', unsafe_allow_html=True)

def status_badge(status: str) -> str:
    s = str(status).upper()
    if any(x in s for x in ["CRITICAL","DISRUPTED","HIGH RISK","BEARISH"]):
        return f'<span class="badge-red">{status}</span>'
    elif any(x in s for x in ["MODERATE","WATCH","WARN","BULLISH","OVERBOUGHT"]):
        return f'<span class="badge-amber">{status}</span>'
    elif any(x in s for x in ["CLEAR","NORMAL","CHEAP","OVERSOLD"]):
        return f'<span class="badge-green">{status}</span>'
    else:
        return f'<span class="badge-blue">{status}</span>'


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — EXECUTIVE SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
if page == "📊 Executive Summary":
    title("Executive Summary", "Maritime shipping intelligence — morning briefing")

    today = datetime.now().strftime("%B %d, %Y")
    st.markdown(f'<div style="font-size:12px;color:#4a7fa5;margin-bottom:1rem;">📅 {today}</div>',
                unsafe_allow_html=True)

    # ── Row 1: Top KPIs ──
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    if not bdi.empty and "bdi_value" in bdi.columns:
        latest_bdi = float(bdi.sort_values("date").iloc[-1]["bdi_value"])
        prev_bdi   = float(bdi.sort_values("date").iloc[-6]["bdi_value"]) if len(bdi) > 6 else latest_bdi
        k1.metric("BDI", f"{latest_bdi:.0f}", delta=f"{latest_bdi-prev_bdi:+.0f} (5d)")
    if not wx.empty and "port_risk_flag" in wx.columns:
        risk_ports = int(wx["port_risk_flag"].sum())
        k2.metric("Ports at risk", f"{risk_ports} / {len(wx)}", delta="Beaufort ≥ 7")
    if not straits_raw.empty and "disruption_score" in straits_raw.columns:
        st_latest = straits_raw.sort_values("fetched_at", ascending=False).drop_duplicates("strait_name") if "strait_name" in straits_raw.columns else straits_raw
        crit = int((st_latest["disruption_score"] >= 40).sum()) if "disruption_score" in st_latest.columns else 0
        k3.metric("Straits elevated risk", f"{crit} / {len(st_latest)}")
    if not trade.empty and "trade_value_usd" in trade.columns:
        total_t = trade[trade.get("flow_direction","Export") == "Export"]["trade_value_usd"].sum() / 1e12 if "flow_direction" in trade.columns else trade["trade_value_usd"].sum() / 1e12
        k4.metric("Total export value", f"${total_t:.1f}T", delta="All years, 5 HS codes")
    if not ais.empty and "mmsi" in ais.columns:
        k5.metric("Vessels tracked (AIS)", f"{ais['mmsi'].nunique():,}")
    if not bdi.empty:
        sig_df = D.get("analysis_bdi_signals", pd.DataFrame())
        if not sig_df.empty and "market_signal" in sig_df.columns:
            latest_sig = str(sig_df.sort_values("date").iloc[-1]["market_signal"])
            k6.metric("BDI Signal", latest_sig)

    # ── Route disruption alerts ──
    h("Route disruption status")
    rd = D.get("analysis_route_disruption", pd.DataFrame())
    if not rd.empty and "overall_status" in rd.columns:
        for _, row in rd.sort_values("strait_disruption_score", ascending=False).iterrows():
            s = str(row["overall_status"])
            cls = "red" if "DISRUPTED" in s else "amber" if "WATCH" in s else "green"
            notes = str(row.get("route_notes",""))[:120]
            st.markdown(
                f'<div class="alert-card alert-{cls}">'
                f'<b>{row["route"]}</b> &nbsp; {s} &nbsp; '
                f'— Strait: <b>{row.get("strait","N/A")}</b> '
                f'(score {row.get("strait_disruption_score",0)}) &nbsp;|&nbsp; '
                f'Commodities: {row.get("commodities_affected","")}<br>'
                f'<span style="font-size:11px;color:#8aacc8;">{notes}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )
    else:
        alert_box("blue", "Run update_combined.py to generate route disruption analysis.")

    # ── Port risk + BDI mini panels ──
    c1, c2 = st.columns(2)

    with c1:
        h("Port weather alerts")
        prt = D.get("analysis_port_risk_trade", pd.DataFrame())
        if not prt.empty and "business_alert" in prt.columns:
            alerted = prt[prt["business_alert"].str.startswith(("🔴","🟠","🟡"), na=False)].head(8)
            for _, row in alerted.iterrows():
                cls = "red" if str(row["business_alert"]).startswith("🔴") else "amber" if str(row["business_alert"]).startswith("🟠") else "amber"
                st.markdown(
                    f'<div class="alert-card alert-{cls}">'
                    f'<b>{row.get("port_name","")}</b> ({row.get("country_iso","")}) &nbsp;·&nbsp; '
                    f'{row.get("beaufort_desc","")} Bf{row.get("beaufort_number","")} &nbsp;·&nbsp; '
                    f'Trade exposure: <b>${row.get("total_B",0):.0f}B</b><br>'
                    f'<span style="font-size:11px;">{row.get("business_alert","")}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
        elif not wx.empty and "port_risk_flag" in wx.columns:
            risky = wx[wx["port_risk_flag"] == True]
            if risky.empty:
                alert_box("green", "✓ All 20 ports operating under normal conditions.")
            else:
                for _, row in risky.iterrows():
                    alert_box("amber", f"⚠ <b>{row.get('port_name','')} ({row.get('country_iso','')})</b> — Bf{row.get('beaufort_number','')} {row.get('beaufort_desc','')}")
        else:
            alert_box("blue", "Weather data not yet loaded.")

    with c2:
        h("Strait conditions now")
        sm = D.get("analysis_strait_monitor", pd.DataFrame())
        df_sm = sm if not sm.empty else straits_raw
        if not df_sm.empty and "strait_name" in df_sm.columns:
            for _, row in df_sm.sort_values("disruption_score", ascending=False).head(8).iterrows():
                rl  = str(row.get("risk_level","Normal"))
                geo = str(row.get("geopolitical_risk",""))
                cls = "red" if rl == "Critical" else "amber" if rl in ("High","Moderate") else "green"
                bf  = row.get("beaufort_number", "?")
                sc  = int(row.get("disruption_score", 0))
                st.markdown(
                    f'<div class="alert-card alert-{cls}">'
                    f'<b>{row.get("strait_name","")}</b> &nbsp;·&nbsp; '
                    f'Score {sc}/100 &nbsp;·&nbsp; Bf{bf} &nbsp;·&nbsp; Geo risk: {geo}<br>'
                    f'<span style="font-size:11px;color:#8aacc8;">'
                    f'{str(row.get("risk_notes",""))[:100]}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
        else:
            alert_box("blue", "Run ingest_weather.py to populate strait conditions.")

    # ── BDI sparkline ──
    h("BDI — last 90 days")
    if not bdi.empty and "bdi_value" in bdi.columns:
        bdi90 = bdi.sort_values("date").tail(90)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=bdi90["date"], y=bdi90["bdi_value"], name="BDI",
            line=dict(color=C["blue"], width=1.5),
            fill="tozeroy", fillcolor="rgba(55,138,221,0.08)",
        ))
        if "rolling_30d_avg" in bdi90.columns:
            fig.add_trace(go.Scatter(x=bdi90["date"], y=bdi90["rolling_30d_avg"],
                                     name="30d avg", line=dict(color=C["amber"], width=1.5, dash="dot")))
        fig.update_layout(**DARK, height=200, hovermode="x unified",
                          showlegend=False, margin=dict(t=10, b=20, l=40, r=10))
        st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — STRAIT MONITOR
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🚧 Strait Monitor":
    title("Strait Monitor", "8 critical maritime chokepoints — weather + geopolitical risk")

    sm = D.get("analysis_strait_monitor", pd.DataFrame())
    df = sm if not sm.empty else straits_raw

    if df.empty:
        alert_box("amber", "Run scripts/ingest_weather.py to populate strait conditions. "
                           "Straits data is generated as part of the daily weather fetch.")
        st.stop()

    # Latest per strait
    if "fetch_date" in df.columns:
        df = df.sort_values("fetch_date", ascending=False).drop_duplicates("strait_name") if "strait_name" in df.columns else df
    for col in ["beaufort_number","wind_speed_ms","disruption_score","trade_pct_global","oil_pct_global"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # ── KPIs ──
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Straits monitored", len(df))
    crit_count = int((df["disruption_score"] >= 60).sum()) if "disruption_score" in df.columns else 0
    high_count = int((df["disruption_score"] >= 40).sum()) if "disruption_score" in df.columns else 0
    k2.metric("Critical / High risk", f"{crit_count} / {high_count}")
    if "trade_pct_global" in df.columns:
        trade_at_risk = df[df["disruption_score"] >= 40]["trade_pct_global"].sum()
        k3.metric("Trade% at elevated risk", f"{trade_at_risk:.0f}%")
    if "oil_pct_global" in df.columns:
        oil_at_risk = df[df["disruption_score"] >= 40]["oil_pct_global"].sum()
        k4.metric("Oil% at elevated risk", f"{oil_at_risk:.0f}%")

    # ── Strait cards ──
    h("Chokepoint status")
    for _, row in df.sort_values("disruption_score", ascending=False).iterrows():
        rl   = str(row.get("risk_level","Normal"))
        geo  = str(row.get("geopolitical_risk",""))
        score= int(row.get("disruption_score", 0))
        bf   = int(row.get("beaufort_number", 0) or 0)
        ws   = float(row.get("wind_speed_ms", 0) or 0)
        trd  = int(row.get("trade_pct_global", 0) or 0)
        oil  = int(row.get("oil_pct_global", 0) or 0)
        cls  = "red" if rl == "Critical" else "amber" if rl in ("High","Moderate") else "green"
        icon = "🔴" if rl == "Critical" else "🟠" if rl == "High" else "🟡" if rl == "Moderate" else "🟢"

        st.markdown(f"""
        <div class="alert-card alert-{cls}">
          <div style="display:flex;justify-content:space-between;align-items:center;">
            <div>
              <b style="font-size:15px;">{icon} {row.get("strait_name","")}</b>
              <span style="color:#8aacc8;font-size:12px;margin-left:8px;">{row.get("region","")}</span>
            </div>
            <div style="text-align:right;font-size:12px;">
              Score <b>{score}/100</b> &nbsp;·&nbsp; Geo risk: <b>{geo}</b>
            </div>
          </div>
          <div style="margin-top:6px;font-size:12px;display:flex;gap:24px;flex-wrap:wrap;">
            <span>🌬 Bf{bf} — {row.get("beaufort_desc","")} ({ws:.1f} m/s)</span>
            <span>☁ {row.get("weather_main","")}</span>
            <span>🚢 {trd}% of world seaborne trade</span>
            <span>🛢 {oil}% of world oil</span>
          </div>
          <div style="margin-top:6px;font-size:11px;color:#8aacc8;">
            <b>Connects:</b> {row.get("connects","")} &nbsp;·&nbsp;
            <b>Routes:</b> {row.get("key_routes","")}
          </div>
          <div style="margin-top:4px;font-size:11px;color:#c8a060;">
            ⚠ {row.get("risk_notes","")}
          </div>
          <div style="margin-top:4px;font-size:11px;color:#8aacc8;">
            🔀 Reroute: {row.get("reroute_note","N/A") if "reroute_note" in row.index else "N/A"}
          </div>
        </div>""", unsafe_allow_html=True)

    # ── Disruption score chart ──
    h("Disruption score comparison (0 = safe, 100 = critical)")
    if "disruption_score" in df.columns and "strait_name" in df.columns:
        df_s = df.sort_values("disruption_score")
        bar_colors = [
            C["red"] if s >= 60 else C["amber"] if s >= 40 else C["teal"] if s >= 20 else C["teal"]
            for s in df_s["disruption_score"]
        ]
        fig = go.Figure(go.Bar(
            x=df_s["disruption_score"], y=df_s["strait_name"],
            orientation="h", marker_color=bar_colors,
            text=df_s["disruption_score"].astype(int).astype(str),
            textposition="outside", textfont=dict(size=11, color="#8aacc8"),
        ))
        fig.add_vline(x=40, line_dash="dash", line_color=C["amber"], line_width=1,
                      annotation_text="High risk threshold", annotation_font_color=C["amber"])
        fig.add_vline(x=60, line_dash="dash", line_color=C["red"], line_width=1,
                      annotation_text="Critical", annotation_font_color=C["red"])
        fig.update_layout(**DARK, height=380, xaxis=dict(range=[0,105], **DARK["xaxis"]),
                          margin=dict(t=20, b=40, l=10, r=80))
        st.plotly_chart(fig, use_container_width=True)

    # ── Map ──
    if "lat" in df.columns and "lon" in df.columns:
        h("Strait locations")
        fig2 = go.Figure()
        for _, row in df.iterrows():
            rl = str(row.get("risk_level","Normal"))
            color = C["red"] if rl == "Critical" else C["amber"] if rl in ("High","Moderate") else C["teal"]
            fig2.add_trace(go.Scattermap(
                lat=[row["lat"]], lon=[row["lon"]],
                mode="markers+text",
                text=[row.get("strait_name","")],
                textposition="top right",
                marker=dict(
                    size=max(10, int(row.get("disruption_score",10)) // 4),
                    color=color, opacity=0.9,
                ),
                hovertemplate=(
                    f"<b>{row.get('strait_name','')}</b><br>"
                    f"Risk: {rl} (score={int(row.get('disruption_score',0))})<br>"
                    f"Geo: {row.get('geopolitical_risk','')}<br>"
                    f"Trade: {row.get('trade_pct_global',0)}% global<extra></extra>"
                ),
                name="",
                showlegend=False,
            ))
        fig2.update_layout(
            map=dict(style="dark", center=dict(lat=20, lon=60), zoom=1.5),
            height=400, margin=dict(t=0, b=0, l=0, r=0),
            paper_bgcolor="#0d2137",
        )
        st.plotly_chart(fig2, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — ROUTE DISRUPTION
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🛳  Route Disruption":
    title("Route Disruption", "6 key global trade routes — daily status")

    rd = D.get("analysis_route_disruption", pd.DataFrame())

    if rd.empty:
        alert_box("amber", "Run scripts/update_combined.py to generate route disruption analysis.")
        st.stop()

    for col in ["strait_disruption_score","strait_beaufort"]:
        if col in rd.columns:
            rd[col] = pd.to_numeric(rd[col], errors="coerce").fillna(0)

    # ── KPIs ──
    k1, k2, k3 = st.columns(3)
    k1.metric("Routes monitored", len(rd))
    disrupted = int((rd["overall_status"].str.contains("DISRUPTED", na=False)).sum()) if "overall_status" in rd.columns else 0
    watched   = int((rd["overall_status"].str.contains("WATCH", na=False)).sum())    if "overall_status" in rd.columns else 0
    k2.metric("Disrupted / Watch", f"{disrupted} / {watched}")
    if "strait_disruption_score" in rd.columns:
        k3.metric("Avg strait score", f"{rd['strait_disruption_score'].mean():.0f}/100")

    h("Route status (sorted by severity)")
    for _, row in rd.iterrows():
        s   = str(row.get("overall_status",""))
        cls = "red" if "DISRUPTED" in s else "amber" if "WATCH" in s else "green"
        score = int(row.get("strait_disruption_score",0))
        geo   = str(row.get("geopolitical_risk","N/A"))
        alt   = str(row.get("alt_route","None"))
        origin_risk = str(row.get("origin_ports_at_risk","None"))
        dest_risk   = str(row.get("dest_ports_at_risk","None"))

        st.markdown(f"""
        <div class="alert-card alert-{cls}">
          <div style="display:flex;justify-content:space-between;">
            <b style="font-size:14px;">{s} &nbsp; {row.get("route","")}</b>
            <span style="font-size:12px;color:#8aacc8;">Strait score: {score}/100 · Geo: {geo}</span>
          </div>
          <div style="margin-top:6px;font-size:12px;display:flex;gap:20px;flex-wrap:wrap;">
            <span>🚧 Strait: <b>{row.get("strait","N/A")}</b></span>
            <span>📦 Commodities: {row.get("commodities_affected","")}</span>
            <span>🔀 Alt: {alt}</span>
          </div>
          <div style="margin-top:4px;font-size:12px;">
            <span style="color:#8aacc8;">Origin ports at risk: {origin_risk} &nbsp;·&nbsp; Destination: {dest_risk}</span>
          </div>
          <div style="margin-top:4px;font-size:11px;color:#8aacc8;">{row.get("route_notes","")}</div>
        </div>""", unsafe_allow_html=True)

    # ── Score bar chart ──
    h("Strait disruption scores by route")
    if "strait_disruption_score" in rd.columns and "route" in rd.columns:
        fig = go.Figure(go.Bar(
            x=rd["route"], y=rd["strait_disruption_score"],
            marker_color=[C["red"] if s>=60 else C["amber"] if s>=40 else C["teal"] for s in rd["strait_disruption_score"]],
            text=rd["strait_disruption_score"].astype(int).astype(str),
            textposition="outside", textfont=dict(size=11, color="#8aacc8"),
        ))
        fig.add_hline(y=40, line_dash="dash", line_color=C["amber"],
                      annotation_text="Elevated risk", annotation_font_color=C["amber"])
        fig.update_layout(**DARK, height=350, xaxis_tickangle=-20,
                          yaxis=dict(range=[0,105], **DARK["yaxis"]),
                          margin=dict(t=20, b=80, l=40, r=20))
        st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — BALTIC DRY INDEX
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📈 Baltic Dry Index":
    title("Baltic Dry Index", "Global bulk shipping cost indicator · investing.com (manual update)")

    if bdi.empty:
        alert_box("red", "BDI data not available."); st.stop()

    # ── Filters ──
    c1, c2, c3 = st.columns([3, 3, 1])
    with c1:
        years = sorted(bdi["date"].dt.year.dropna().unique().astype(int).tolist())
        yr_r  = st.select_slider("Year range", options=years, value=(years[0], years[-1]))
    with c2:
        avgs = st.multiselect("Rolling averages", ["7-day","30-day","90-day"], default=["30-day"])
    with c3:
        show_ev = st.checkbox("Events", value=True)

    df = bdi[(bdi["date"].dt.year >= yr_r[0]) & (bdi["date"].dt.year <= yr_r[1])].copy()

    # ── KPIs ──
    latest  = df.sort_values("date").iloc[-1]
    k1,k2,k3,k4,k5 = st.columns(5)
    prev5   = df.sort_values("date").iloc[-6] if len(df) > 6 else latest
    k1.metric("Current BDI",   f"{latest['bdi_value']:.0f}", delta=f"{latest['bdi_value']-prev5['bdi_value']:+.0f} vs 5d")
    k2.metric("Period high",   f"{df['bdi_value'].max():.0f}")
    k3.metric("Period low",    f"{df['bdi_value'].min():.0f}")
    k4.metric("Period avg",    f"{df['bdi_value'].mean():.0f}")
    sp = int(df["is_spike"].sum()) if "is_spike" in df.columns else 0
    dr = int(df["is_drop"].sum())  if "is_drop"  in df.columns else 0
    k5.metric("Spikes / Drops", f"{sp} / {dr}")

    # ── Signal from analysis table ──
    sig_df = D.get("analysis_bdi_signals", pd.DataFrame())
    if not sig_df.empty and "market_signal" in sig_df.columns:
        sig_df["date"] = pd.to_datetime(sig_df["date"], errors="coerce")
        latest_sig = sig_df.sort_values("date").iloc[-1]
        sig_cls = "red" if latest_sig["market_signal"] == "BEARISH" else "green" if latest_sig["market_signal"] == "BULLISH" else "blue"
        alert_box(sig_cls,
                  f"📡 <b>Market signal: {latest_sig['market_signal']}</b> "
                  f"— {latest_sig.get('charter_recommendation','')}")

    # ── Main BDI chart ──
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["date"], y=df["bdi_value"], name="BDI",
                             line=dict(color=C["blue"], width=1.2),
                             fill="tozeroy", fillcolor="rgba(55,138,221,0.06)"))
    avg_map = {"7-day":  ("rolling_7d_avg",  C["teal"]),
               "30-day": ("rolling_30d_avg", C["amber"]),
               "90-day": ("rolling_90d_avg", C["coral"])}
    for label in avgs:
        col_n, col_c = avg_map[label]
        if col_n in df.columns:
            fig.add_trace(go.Scatter(x=df["date"], y=df[col_n], name=label,
                                     line=dict(color=col_c, width=1.8, dash="dot")))
    if show_ev:
        EVENTS = {"2016-02-01":"BDI historic low","2020-03-01":"COVID-19",
                  "2021-03-23":"Suez blockage","2021-10-01":"Supply crisis",
                  "2022-03-01":"Ukraine war","2023-11-01":"Houthi attacks (Red Sea)"}
        ymax = df["bdi_value"].max()
        for d, label in EVENTS.items():
            ts = pd.Timestamp(d)
            if yr_r[0] <= ts.year <= yr_r[1]:
                fig.add_vline(x=ts, line_dash="dash", line_color="#2a4a6a", line_width=1)
                fig.add_annotation(x=ts, y=ymax * 0.9, text=label, showarrow=False,
                                   font=dict(color="#4a7fa5", size=9), textangle=-90)
    fig.update_layout(**DARK, title="Baltic Dry Index", height=380, hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    # ── Signals chart ──
    if not sig_df.empty and "market_signal" in sig_df.columns:
        h("Market signals timeline")
        sig_yr = sig_df[(sig_df["date"].dt.year >= yr_r[0]) & (sig_df["date"].dt.year <= yr_r[1])].copy()
        signal_colors = {"BULLISH": C["teal"], "BEARISH": C["coral"], "OVERBOUGHT": C["amber"],
                         "OVERSOLD": C["purple"], "NEUTRAL": C["gray"]}
        fig_s = go.Figure()
        for sig, col in signal_colors.items():
            sub = sig_yr[sig_yr["market_signal"] == sig]
            if not sub.empty:
                fig_s.add_trace(go.Scatter(
                    x=sub["date"], y=sub["bdi_value"], mode="markers",
                    name=sig, marker=dict(size=4, color=col, opacity=0.7),
                ))
        fig_s.update_layout(**DARK, height=220, hovermode="x unified",
                            title="BDI coloured by market signal",
                            margin=dict(t=40, b=30, l=40, r=10))
        st.plotly_chart(fig_s, use_container_width=True)

    # ── Volatility + monthly seasonality ──
    c1, c2 = st.columns(2)
    with c1:
        h("Spikes & drops per year")
        if "is_spike" in df.columns:
            yr_g = df.copy(); yr_g["yr"] = yr_g["date"].dt.year
            gy   = yr_g.groupby("yr").agg(spikes=("is_spike","sum"),drops=("is_drop","sum")).reset_index()
            fig2 = go.Figure()
            fig2.add_bar(x=gy["yr"].astype(str), y=gy["spikes"], name="Spikes", marker_color=C["teal"])
            fig2.add_bar(x=gy["yr"].astype(str), y=gy["drops"],  name="Drops",  marker_color=C["coral"])
            fig2.update_layout(**DARK, barmode="group", height=280, margin=dict(t=10,b=40,l=40,r=10))
            st.plotly_chart(fig2, use_container_width=True)
    with c2:
        h("Monthly BDI seasonality")
        sf = D.get("analysis_seasonal_freight", pd.DataFrame())
        if not sf.empty and "avg_bdi" in sf.columns:
            sf["month_name"] = sf["month_name"] if "month_name" in sf.columns else sf["month"].apply(
                lambda x: ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][int(x)-1])
            bar_c = [C["coral"] if fl=="Expensive (high demand)" else C["teal"] if fl=="Cheap (low demand)" else C["gray"]
                     for fl in sf.get("freight_level", ["Moderate"]*12)]
            fig3  = go.Figure(go.Bar(x=sf["month_name"], y=sf["avg_bdi"].round(0),
                                     marker_color=bar_c,
                                     text=sf["avg_bdi"].round(0), textposition="outside",
                                     textfont=dict(size=9, color="#8aacc8")))
            fig3.update_layout(**DARK, height=280, margin=dict(t=10,b=40,l=40,r=10))
            st.plotly_chart(fig3, use_container_width=True)
        else:
            df_m = df.copy(); df_m["month"] = df_m["date"].dt.month
            mnames = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
            monthly = df_m.groupby("month")["bdi_value"].mean().reset_index()
            monthly["mn"] = monthly["month"].apply(lambda x: mnames[x-1])
            fig3 = go.Figure(go.Bar(x=monthly["mn"], y=monthly["bdi_value"].round(0),
                                    marker_color=C["blue"]))
            fig3.update_layout(**DARK, height=280, margin=dict(t=10,b=40,l=40,r=10))
            st.plotly_chart(fig3, use_container_width=True)

    # ── Charter recommendation table ──
    if not sig_df.empty and "charter_recommendation" in sig_df.columns:
        h("Charter recommendations by signal type")
        rec_table = (sig_df.groupby(["market_signal","charter_recommendation"])
                    .size().reset_index(name="days"))
        st.dataframe(rec_table, use_container_width=True, height=200)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — TRADE ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🌍 Trade Analysis":
    title("Trade Analysis", "UN Comtrade — bilateral flows by commodity")

    if trade.empty:
        alert_box("red", "Trade data not available."); st.stop()

    hs_labels = {k: f"HS {k} — {v}" for k,v in HS_NAMES.items()}
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        all_yrs = sorted(trade["year"].dropna().unique().astype(int).tolist())
        sel_yr  = st.select_slider("Year range", options=all_yrs, value=(all_yrs[0], all_yrs[-1]))
    with fc2:
        all_hs  = sorted(trade["hs_code"].dropna().unique().astype(int).tolist())
        sel_hs  = st.multiselect("Commodity", all_hs, default=all_hs, format_func=lambda x: hs_labels.get(x, str(x)))
    with fc3:
        flow    = st.selectbox("Flow", ["Both","Export","Import"])

    dft = trade[(trade["year"]>=sel_yr[0]) & (trade["year"]<=sel_yr[1])].copy()
    if sel_hs: dft = dft[dft["hs_code"].isin(sel_hs)]
    if flow != "Both" and "flow_direction" in dft.columns:
        dft = dft[dft["flow_direction"]==flow]

    k1,k2,k3,k4 = st.columns(4)
    k1.metric("Total value", f"${dft['trade_value_usd'].sum()/1e12:.1f}T")
    k2.metric("Countries",   f"{dft['reporter_iso'].nunique()}" if "reporter_iso" in dft.columns else "N/A")
    k3.metric("Years",       f"{sel_yr[0]}–{sel_yr[1]}")
    if "flow_direction" in dft.columns:
        e = dft[dft["flow_direction"]=="Export"]["trade_value_usd"].sum()
        i = dft[dft["flow_direction"]=="Import"]["trade_value_usd"].sum()
        k4.metric("Export/Import ratio", f"{e/i:.2f}" if i else "N/A")

    c1, c2 = st.columns(2)
    with c1:
        h("Top 15 exporters by total value")
        if "flow_direction" in dft.columns and "reporter_country" in dft.columns:
            exp = (dft[dft["flow_direction"]=="Export"]
                   .groupby("reporter_country")["trade_value_usd"].sum().nlargest(15).reset_index())
            exp["val_T"] = exp["trade_value_usd"]/1e12
            fig = go.Figure(go.Bar(x=exp["val_T"], y=exp["reporter_country"],
                                   orientation="h", marker_color=C["blue"],
                                   text=exp["val_T"].round(1).astype(str)+"T",
                                   textposition="outside", textfont=dict(size=10,color="#8aacc8")))
            fig.update_layout(**DARK, height=420,
                              yaxis=dict(autorange="reversed",**DARK["yaxis"]),
                              xaxis_title="USD Trillion", margin=dict(t=10,b=40,l=10,r=60))
            st.plotly_chart(fig, use_container_width=True)
    with c2:
        h("Trade balance: net exporters vs importers")
        if "flow_direction" in dft.columns and "reporter_country" in dft.columns:
            piv = dft.pivot_table(index="reporter_country", columns="flow_direction",
                                  values="trade_value_usd", aggfunc="sum").reset_index()
            piv.columns.name = None
            if "Export" in piv.columns and "Import" in piv.columns:
                piv["balance"] = (piv.get("Export",0).fillna(0)-piv.get("Import",0).fillna(0))/1e12
                top_bal = pd.concat([piv.nlargest(8,"balance"), piv.nsmallest(8,"balance")])
                fig2 = go.Figure(go.Bar(x=top_bal["balance"], y=top_bal["reporter_country"],
                                        orientation="h",
                                        marker_color=[C["teal"] if v>0 else C["coral"] for v in top_bal["balance"]]))
                fig2.add_vline(x=0, line_color="#4a7fa5", line_width=1)
                fig2.update_layout(**DARK, height=420,
                                   yaxis=dict(autorange="reversed",**DARK["yaxis"]),
                                   xaxis_title="Balance (USD Trillion)", margin=dict(t=10,b=40,l=10,r=20))
                st.plotly_chart(fig2, use_container_width=True)

    h("Trade value over time by commodity")
    if "hs_code" in dft.columns and "year" in dft.columns:
        td = dft.groupby(["year","hs_code","commodity_name"])["trade_value_usd"].sum().reset_index()
        td["val_B"] = td["trade_value_usd"]/1e9
        hs_c = {10:C["teal"],26:C["coral"],27:C["amber"],72:C["purple"],89:C["blue"]}
        fig3 = go.Figure()
        for hs in td["hs_code"].dropna().unique():
            sub = td[td["hs_code"]==hs]
            fig3.add_trace(go.Scatter(x=sub["year"], y=sub["val_B"],
                                      name=hs_labels.get(int(hs),str(hs)),
                                      mode="lines+markers",
                                      line=dict(color=hs_c.get(int(hs),C["gray"]),width=2),
                                      marker=dict(size=6)))
        fig3.update_layout(**DARK, height=300, hovermode="x unified",
                           xaxis_title="Year", yaxis_title="Trade Value (USD B)")
        st.plotly_chart(fig3, use_container_width=True)

    # ── BDI vs Trade correlation ──
    bt = D.get("analysis_bdi_trade", pd.DataFrame())
    if not bt.empty and "avg_bdi" in bt.columns and "export_value_trillion" in bt.columns:
        h("BDI vs trade volume correlation (joined analysis)")
        for col in ["avg_bdi","export_value_trillion","bdi_yoy_pct","trade_yoy_pct"]:
            if col in bt.columns: bt[col] = pd.to_numeric(bt[col], errors="coerce")
        fig4 = make_subplots(specs=[[{"secondary_y": True}]])
        fig4.add_trace(go.Bar(x=bt["year"], y=bt["export_value_trillion"], name="Exports (T USD)",
                              marker_color=C["blue"], opacity=0.6), secondary_y=False)
        fig4.add_trace(go.Scatter(x=bt["year"], y=bt["avg_bdi"], name="Avg BDI",
                                  mode="lines+markers", line=dict(color=C["amber"],width=2.5)), secondary_y=True)
        fig4.update_layout(**DARK, height=320, hovermode="x unified",
                           margin=dict(t=10,b=40,l=40,r=60))
        fig4.update_yaxes(title_text="USD Trillion", secondary_y=False,
                          gridcolor="#1e3a5f", color="#8aacc8")
        fig4.update_yaxes(title_text="BDI",         secondary_y=True,  color="#8aacc8")
        st.plotly_chart(fig4, use_container_width=True)
        if "correlation_signal" in bt.columns:
            with st.expander("Signal details"):
                st.dataframe(bt[["year","avg_bdi","export_value_trillion",
                                  "bdi_yoy_pct","trade_yoy_pct","correlation_signal"]],
                             use_container_width=True)

    # ── YoY heatmap ──
    if "yoy_growth_pct" in dft.columns and "flow_direction" in dft.columns:
        h("Year-on-year export growth % — top 20 countries")
        yoy = (dft[dft["flow_direction"]=="Export"]
               .groupby(["reporter_country","year"])["yoy_growth_pct"].mean().reset_index())
        top20 = (dft[dft["flow_direction"]=="Export"]
                 .groupby("reporter_country")["trade_value_usd"].sum().nlargest(20).index.tolist())
        yoy20 = yoy[yoy["reporter_country"].isin(top20)]
        pvt   = yoy20.pivot(index="reporter_country", columns="year", values="yoy_growth_pct")
        fig5  = go.Figure(go.Heatmap(
            z=pvt.values, x=pvt.columns.astype(str), y=pvt.index,
            colorscale=[[0,C["coral"]],[0.5,"#0d2137"],[1,C["teal"]]],
            zmid=0, zmin=-30, zmax=30,
            colorbar=dict(title="%", tickfont=dict(color="#8aacc8")),
            text=pvt.values.round(1), texttemplate="%{text}", textfont=dict(size=9),
        ))
        fig5.update_layout(**DARK, height=420, margin=dict(t=10,b=40,l=140,r=20))
        st.plotly_chart(fig5, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 6 — PORT RISK
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🌦  Port Risk":
    title("Port Risk", "20 ports: real-time weather × trade exposure")

    if wx.empty:
        alert_box("red", "Port weather data not available."); st.stop()

    prt = D.get("analysis_port_risk_trade", pd.DataFrame())
    df  = prt if not prt.empty else wx.copy()

    for col in ["beaufort_number","wind_speed_ms","temp_c","humidity_pct","visibility_m","total_B"]:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors="coerce")
    if "port_risk_flag" in df.columns:
        df["port_risk_flag"] = df["port_risk_flag"].astype(bool)

    # ── Filters ──
    fc1, fc2 = st.columns(2)
    with fc1:
        rf = st.selectbox("Filter", ["All ports","At-risk only","Normal only","Top 10 by trade"])
    with fc2:
        sb = st.selectbox("Sort by", ["Risk priority","Wind speed","Trade exposure","Port rank"])

    if rf == "At-risk only"   and "port_risk_flag" in df.columns:
        df = df[df["port_risk_flag"]==True]
    elif rf == "Normal only"  and "port_risk_flag" in df.columns:
        df = df[df["port_risk_flag"]==False]
    elif rf == "Top 10 by trade" and "total_B" in df.columns:
        df = df.nlargest(10,"total_B")

    sort_map = {"Risk priority": ("alert_priority",True) if "alert_priority" in df.columns else ("wind_speed_ms",False),
                "Wind speed":     ("wind_speed_ms",False),
                "Trade exposure": ("total_B",False),
                "Port rank":      ("port_rank",True)}
    sc, sa = sort_map.get(sb, ("port_rank",True))
    if sc in df.columns: df = df.sort_values(sc, ascending=sa)

    # ── KPIs ──
    k1,k2,k3,k4,k5 = st.columns(5)
    k1.metric("Ports monitored", len(wx))
    risk_n = int(wx["port_risk_flag"].sum()) if "port_risk_flag" in wx.columns else 0
    k2.metric("At risk (Bf≥7)", risk_n)
    if "total_B" in prt.columns and not prt.empty:
        trade_at_risk = prt[prt.get("port_risk_flag",pd.Series(False,index=prt.index))==True]["total_B"].sum()
        k3.metric("Trade exposure at risk", f"${trade_at_risk:.0f}B")
    k4.metric("Avg wind", f"{wx['wind_speed_ms'].mean():.1f} m/s" if "wind_speed_ms" in wx.columns else "N/A")
    k5.metric("Avg temp",  f"{wx['temp_c'].mean():.1f}°C"        if "temp_c"        in wx.columns else "N/A")

    # ── Port cards ──
    h("Port conditions (with trade exposure)")
    BEAUFORT_DESCS = ["Calm","Light air","Light breeze","Gentle breeze","Moderate breeze",
                      "Fresh breeze","Strong breeze","Near gale","Gale","Strong gale",
                      "Storm","Violent storm","Hurricane"]
    for _, row in df.iterrows():
        risk = bool(row.get("port_risk_flag", False))
        bf   = int(row.get("beaufort_number", 0) or 0)
        t_B  = float(row.get("total_B", 0) or 0)
        cls  = "red" if risk and t_B > 200 else "amber" if risk else "green" if bf < 4 else "amber"
        alert_txt = str(row.get("business_alert","")) if "business_alert" in row.index else ""
        trade_str = f"${t_B:.0f}B" if t_B else "N/A"
        st.markdown(f"""
        <div class="alert-card alert-{cls}" style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;">
          <div style="min-width:160px;"><b>{row.get('port_name','')} ({row.get('country_iso','')})</b></div>
          <div style="color:#8aacc8;font-size:12px;">🌡 {row.get('temp_c','?'):.1f}°C</div>
          <div style="color:#8aacc8;font-size:12px;">💨 {row.get('wind_speed_ms',0):.1f} m/s — {BEAUFORT_DESCS[bf] if bf<13 else 'Extreme'}</div>
          <div style="color:#8aacc8;font-size:12px;">💧 {row.get('humidity_pct','?'):.0f}%</div>
          <div style="color:#8aacc8;font-size:12px;">☁ {row.get('weather_main','')}</div>
          <div style="color:#e8c860;font-size:12px;">💰 Trade: {trade_str}</div>
          <div style="flex:1;font-size:11px;">{alert_txt}</div>
        </div>""", unsafe_allow_html=True)

    # ── Charts ──
    c1, c2 = st.columns(2)
    with c1:
        h("Wind speed by port (Bf7+ = near gale)")
        ws_df = df.sort_values("wind_speed_ms") if "wind_speed_ms" in df.columns else df
        if "port_name" in ws_df.columns and "wind_speed_ms" in ws_df.columns:
            risk_c = [C["coral"] if r else C["blue"] for r in ws_df.get("port_risk_flag",[False]*len(ws_df))]
            fig = go.Figure(go.Bar(x=ws_df["wind_speed_ms"], y=ws_df["port_name"],
                                   orientation="h", marker_color=risk_c))
            fig.add_vline(x=13.9, line_dash="dash", line_color=C["amber"], line_width=1.5,
                          annotation_text="Near gale", annotation_font_color=C["amber"])
            fig.update_layout(**DARK, height=460, xaxis_title="m/s",
                              margin=dict(t=10,b=40,l=10,r=20))
            st.plotly_chart(fig, use_container_width=True)
    with c2:
        h("Trade exposure vs wind speed")
        if "total_B" in df.columns and "wind_speed_ms" in df.columns:
            fig2 = go.Figure(go.Scatter(
                x=df["wind_speed_ms"], y=df["total_B"],
                mode="markers+text",
                text=df.get("port_name",""),
                textposition="top center", textfont=dict(size=9, color="#4a7fa5"),
                marker=dict(
                    size=df.get("beaufort_number",pd.Series([4]*len(df))) * 3 + 6,
                    color=[C["coral"] if r else C["blue"] for r in df.get("port_risk_flag",[False]*len(df))],
                    opacity=0.8, line=dict(color="#1e3a5f",width=1),
                ),
                hovertemplate=(
                    "<b>%{text}</b><br>"
                    "Wind: %{x:.1f} m/s<br>"
                    "Trade: $%{y:.0f}B<extra></extra>"
                ),
            ))
            fig2.update_layout(**DARK, height=460,
                               xaxis_title="Wind speed (m/s)",
                               yaxis_title="Total trade exposure ($B)",
                               margin=dict(t=10,b=40,l=60,r=10))
            st.plotly_chart(fig2, use_container_width=True)

    # ── Net exporter risk table ──
    ner = D.get("analysis_net_exporter_risk", pd.DataFrame())
    if not ner.empty:
        h("Net exporters ranked by value + current weather impact")
        show_c = ["reporter_country","trade_balance_B","is_net_exporter","top_export_commodity",
                  "port_name","beaufort_number","weather_impact"]
        show_c = [c for c in show_c if c in ner.columns]
        st.dataframe(ner[show_c].head(20), use_container_width=True, height=320)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 7 — VESSEL ACTIVITY
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🛥  Vessel Activity":
    title("Vessel Activity", "AIS Stream — vessel tracking at Singapore port")

    if ais.empty:
        alert_box("red", "AIS data not available."); st.stop()

    for col in ["sog_knots","latitude","longitude"]:
        if col in ais.columns: ais[col] = pd.to_numeric(ais[col], errors="coerce")

    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        cats    = ["All"] + sorted(ais["vessel_category"].dropna().unique().tolist()) if "vessel_category" in ais.columns else ["All"]
        sel_cat = st.selectbox("Vessel category", cats)
    with fc2:
        spds    = ["All"] + sorted(ais["speed_category"].dropna().unique().tolist()) if "speed_category" in ais.columns else ["All"]
        sel_spd = st.selectbox("Speed", spds)
    with fc3:
        min_sog = st.slider("Min speed (knots)", 0.0, 25.0, 0.0, 0.5)

    df_a = ais.copy()
    if sel_cat != "All" and "vessel_category" in df_a.columns: df_a = df_a[df_a["vessel_category"]==sel_cat]
    if sel_spd != "All" and "speed_category"  in df_a.columns: df_a = df_a[df_a["speed_category"]==sel_spd]
    if "sog_knots" in df_a.columns: df_a = df_a[df_a["sog_knots"].fillna(0) >= min_sog]

    k1,k2,k3,k4,k5 = st.columns(5)
    k1.metric("Total messages",  f"{len(df_a):,}")
    k2.metric("Unique vessels",  f"{df_a['mmsi'].nunique():,}" if "mmsi" in df_a.columns else "N/A")
    moving = int((ais["is_moving"]==True).sum()) if "is_moving" in ais.columns else 0
    k3.metric("Moving vessels",  f"{moving}")
    k4.metric("Avg speed",       f"{df_a['sog_knots'].mean():.1f} kn" if "sog_knots" in df_a.columns else "N/A")
    tankers = int((ais["vessel_category"]=="Tanker").sum()) if "vessel_category" in ais.columns else 0
    k5.metric("Tankers",         f"{tankers}")

    # Vessel port risk
    vpr = D.get("analysis_vessel_port_risk", pd.DataFrame())
    if not vpr.empty and "congestion_weather_risk" in vpr.columns:
        h("Congestion + weather risk by port")
        for _, row in vpr.sort_values("vessel_count",ascending=False).head(6).iterrows():
            risk = str(row.get("congestion_weather_risk",""))
            cls  = "red" if "HIGH" in risk else "amber" if "MODERATE" in risk or "WEATHER" in risk else "green"
            st.markdown(
                f'<div class="alert-card alert-{cls}">'
                f'<b>{row.get("port_name","")}</b> — {risk}<br>'
                f'<span style="font-size:11px;color:#8aacc8;">'
                f'Vessels: {row.get("vessel_count",0)} · Tankers: {row.get("tanker_count",0)} · '
                f'Cargo: {row.get("cargo_count",0)} · '
                f'Wind Bf{row.get("beaufort_number","?")}'
                f'</span></div>',
                unsafe_allow_html=True,
            )

    c1, c2 = st.columns([3, 2])
    with c1:
        h("Vessel positions — Singapore")
        pos = df_a.dropna(subset=["latitude","longitude"]) if "latitude" in df_a.columns else pd.DataFrame()
        if not pos.empty:
            CAT_COLOR = {"Tanker":C["coral"],"Cargo":C["blue"],"Tug / Support":C["amber"],
                         "Fishing":C["teal"],"Passenger":C["purple"],"Unknown":C["gray"],"Other":C["gray"]}
            fig = go.Figure()
            for cat in (pos["vessel_category"].unique() if "vessel_category" in pos.columns else ["Unknown"]):
                sub = pos[pos["vessel_category"]==cat] if "vessel_category" in pos.columns else pos
                mv  = sub[sub["is_moving"]==True]  if "is_moving" in sub.columns else sub
                st_ = sub[sub["is_moving"]!=True]  if "is_moving" in sub.columns else pd.DataFrame()
                if not mv.empty:
                    fig.add_trace(go.Scattergl(
                        x=mv["longitude"], y=mv["latitude"], mode="markers",
                        name=f"{cat} (moving)",
                        marker=dict(size=7,color=CAT_COLOR.get(cat,C["gray"]),symbol="arrow",opacity=0.85),
                    ))
                if not st_.empty:
                    fig.add_trace(go.Scattergl(
                        x=st_["longitude"], y=st_["latitude"], mode="markers",
                        name=f"{cat} (stationary)",
                        marker=dict(size=4,color=CAT_COLOR.get(cat,C["gray"]),symbol="circle",opacity=0.4),
                    ))
            fig.update_layout(**DARK, height=440,
                              xaxis=dict(range=[103.4,104.4],**DARK["xaxis"]),
                              yaxis=dict(range=[1.0,1.6],**DARK["yaxis"]),
                              xaxis_title="Longitude", yaxis_title="Latitude",
                              margin=dict(t=10,b=40,l=50,r=10))
            st.plotly_chart(fig, use_container_width=True)
    with c2:
        if "vessel_category" in df_a.columns:
            h("Vessel types")
            vc = df_a[df_a["vessel_category"]!="Unknown"]["vessel_category"].value_counts().reset_index()
            vc.columns = ["category","count"]
            if not vc.empty:
                fig2 = go.Figure(go.Pie(labels=vc["category"], values=vc["count"],
                                        marker_colors=[CAT_COLOR.get(c,C["gray"]) for c in vc["category"]],
                                        hole=0.55, textinfo="label+value",
                                        textfont=dict(color="#e8f0f8",size=11)))
                fig2.update_layout(**DARK, height=220, showlegend=False, margin=dict(t=10,b=10,l=10,r=10))
                st.plotly_chart(fig2, use_container_width=True)
        if "speed_category" in df_a.columns:
            h("Speed categories")
            SC_ORDER = ["Stationary","Slow / manoeuvring","Transit","Cruising","Unknown"]
            SC_C     = {"Stationary":C["purple"],"Slow / manoeuvring":C["amber"],
                        "Transit":C["teal"],"Cruising":C["blue"],"Unknown":C["gray"]}
            sc = df_a["speed_category"].value_counts().reindex(SC_ORDER).dropna().reset_index()
            sc.columns = ["category","count"]
            fig3 = go.Figure(go.Bar(x=sc["category"], y=sc["count"],
                                    marker_color=[SC_C.get(c,C["gray"]) for c in sc["category"]]))
            fig3.update_layout(**DARK, height=220, showlegend=False,
                               margin=dict(t=10,b=50,l=30,r=10), xaxis_tickangle=-30)
            st.plotly_chart(fig3, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 8 — CROSS-SOURCE INSIGHTS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🔗 Cross-Source Insights":
    title("Cross-Source Insights", "Joined analytical tables — BDI × Trade × Weather × AIS")

    tab1, tab2, tab3, tab4 = st.tabs([
        "📦 Commodity Freight Cost",
        "🇨🇳 China Concentration",
        "📅 Booking Calendar",
        "🔗 All Analytical Tables",
    ])

    # ── TAB 1: Commodity × BDI ──────────────────────────────────────────────
    with tab1:
        cb = D.get("analysis_commodity_bdi", pd.DataFrame())
        if cb.empty:
            alert_box("blue","Run update_combined.py to generate this table.")
        else:
            for col in ["avg_bdi","export_B","freight_burden_index","bdi_volatility"]:
                if col in cb.columns: cb[col] = pd.to_numeric(cb[col], errors="coerce")

            st.markdown("""
            **Business question:** For each shipping commodity, how does the freight cost (BDI proxy)
            compare to trade value? High-BDI years eat into margins for bulk commodities like cereals
            and ores. Useful for cargo booking timing and charter rate negotiation.
            """)

            # Freight burden by commodity over time
            if "commodity_name" in cb.columns and "year" in cb.columns and "freight_burden_index" in cb.columns:
                h("Freight burden index by commodity (higher = freight is more expensive relative to trade value)")
                comm_pivot = cb.pivot_table(index="year", columns="commodity_name",
                                            values="freight_burden_index")
                fig = go.Figure()
                comm_colors = {"Cereals":C["teal"],"Ores & slag":C["coral"],"Mineral fuels":C["amber"],
                               "Iron & steel":C["purple"],"Ships & boats":C["blue"]}
                for col in comm_pivot.columns:
                    fig.add_trace(go.Scatter(
                        x=comm_pivot.index, y=comm_pivot[col], name=col,
                        line=dict(color=comm_colors.get(col,C["gray"]),width=2),
                        mode="lines+markers", marker=dict(size=5),
                    ))
                fig.update_layout(**DARK, height=320, hovermode="x unified",
                                  yaxis_title="BDI / Trade value ratio")
                st.plotly_chart(fig, use_container_width=True)

            h("Export value vs BDI by commodity")
            if "export_B" in cb.columns and "avg_bdi" in cb.columns and "commodity_name" in cb.columns:
                fig2 = px.scatter(
                    cb, x="avg_bdi", y="export_B", color="commodity_name",
                    size="freight_burden_index",
                    color_discrete_map=comm_colors,
                    hover_data=["year","bdi_sensitivity"],
                    labels={"avg_bdi":"Average BDI","export_B":"Export Value (B USD)"},
                )
                fig2.update_layout(**DARK, height=350)
                st.plotly_chart(fig2, use_container_width=True)

            if "bdi_sensitivity" in cb.columns:
                h("BDI sensitivity by commodity")
                sens = cb.groupby(["commodity_name","bdi_sensitivity"]).size().reset_index(name="n")
                st.dataframe(sens.drop(columns="n").drop_duplicates(), use_container_width=True)

    # ── TAB 2: China concentration ───────────────────────────────────────────
    with tab2:
        cc = D.get("analysis_china_concentration", pd.DataFrame())
        if cc.empty:
            alert_box("blue","Run update_combined.py to generate this table.")
        else:
            for col in ["china_share_pct","world_export_usd","china_export_usd"]:
                if col in cc.columns: cc[col] = pd.to_numeric(cc[col], errors="coerce")

            st.markdown("""
            **Business question:** How dominant is China in each HS code category?
            If Chinese ports are disrupted (weather, geopolitical event), what is the
            global supply chain exposure? Current port weather risk is overlaid.
            """)

            # China port risk today
            china_risk = bool(cc["china_port_risk_today"].iloc[0]) if "china_port_risk_today" in cc.columns else False
            china_bf   = int(cc["china_max_beaufort"].iloc[0])      if "china_max_beaufort"   in cc.columns else 0
            if china_risk:
                alert_box("red", f"⚠ Chinese ports currently at weather risk (max Beaufort {china_bf}). "
                                 f"Supply chain disruption risk elevated for high-concentration commodities.")
            else:
                alert_box("green", f"✓ Chinese port weather conditions normal (max Beaufort {china_bf}).")

            h("China export share % by commodity and year")
            if "year" in cc.columns and "commodity_name" in cc.columns and "china_share_pct" in cc.columns:
                china_piv = cc.pivot_table(index="year", columns="commodity_name", values="china_share_pct")
                fig = go.Figure()
                for col in china_piv.columns:
                    fig.add_trace(go.Bar(x=china_piv.index, y=china_piv[col], name=col))
                fig.update_layout(**DARK, barmode="group", height=340,
                                  yaxis_title="China share (%)", xaxis_title="Year",
                                  hovermode="x unified")
                st.plotly_chart(fig, use_container_width=True)

            h("Supply concentration risk")
            if "supply_concentration" in cc.columns:
                sc_latest = cc.sort_values("year",ascending=False).drop_duplicates("commodity_name")
                disp_cols = ["commodity_name","china_share_pct","supply_concentration","disruption_exposure_B"]
                disp_cols = [c for c in disp_cols if c in sc_latest.columns]
                st.dataframe(sc_latest[disp_cols], use_container_width=True)

    # ── TAB 3: Booking calendar ───────────────────────────────────────────────
    with tab3:
        sf = D.get("analysis_seasonal_freight", pd.DataFrame())
        if sf.empty:
            alert_box("blue","Run update_combined.py to generate this table.")
        else:
            for col in ["avg_bdi","min_bdi","max_bdi","std_bdi"]:
                if col in sf.columns: sf[col] = pd.to_numeric(sf[col], errors="coerce")

            st.markdown("""
            **Business question:** When is freight cheapest? When should you book cargo early vs
            use spot market? This table combines historical BDI seasonality with shipping demand patterns.
            """)

            h("Average BDI by month (all years combined)")
            if "avg_bdi" in sf.columns and "month_name" in sf.columns:
                fl_col = [C["coral"] if fl=="Expensive (high demand)" else C["teal"] if fl=="Cheap (low demand)" else C["gray"]
                          for fl in sf.get("freight_level",["Moderate"]*12)]
                fig = go.Figure()
                fig.add_trace(go.Bar(x=sf["month_name"], y=sf["avg_bdi"], name="Avg BDI",
                                     marker_color=fl_col, text=sf["avg_bdi"].round(0),
                                     textposition="outside", textfont=dict(size=10,color="#8aacc8")))
                if "min_bdi" in sf.columns and "max_bdi" in sf.columns:
                    fig.add_trace(go.Scatter(x=sf["month_name"],
                                             y=sf["max_bdi"], mode="lines",
                                             name="Max BDI", line=dict(color=C["coral"],width=1,dash="dot")))
                    fig.add_trace(go.Scatter(x=sf["month_name"],
                                             y=sf["min_bdi"], mode="lines",
                                             name="Min BDI", line=dict(color=C["teal"],width=1,dash="dot")))
                fig.update_layout(**DARK, height=320, xaxis_title="Month", yaxis_title="BDI",
                                  margin=dict(t=20,b=40,l=40,r=10))
                st.plotly_chart(fig, use_container_width=True)

            h("Booking advice by month")
            if "booking_advice" in sf.columns:
                disp = sf[["month_name","avg_bdi","freight_level","booking_advice"]].copy() if "month_name" in sf.columns else sf
                st.dataframe(disp, use_container_width=True, height=380)

    # ── TAB 4: All analytical tables ─────────────────────────────────────────
    with tab4:
        h("All analytical tables available")
        tables_meta = {
            "analysis_bdi_trade":          "BDI × annual trade (JOIN by year)",
            "analysis_port_risk_trade":    "Port weather × trade exposure (JOIN by country_iso)",
            "analysis_strait_monitor":     "8 strait chokepoints + disruption scoring",
            "analysis_commodity_bdi":      "HS commodity × BDI freight cost burden",
            "analysis_bdi_signals":        "Daily market signals + charter recommendations",
            "analysis_net_exporter_risk":  "Net exporter rank + current port weather",
            "analysis_seasonal_freight":   "Monthly BDI seasonality + booking calendar",
            "analysis_china_concentration":"China export share per HS code + supply risk",
            "analysis_vessel_port_risk":   "AIS vessel density × port weather",
            "analysis_route_disruption":   "Daily route disruption overview",
        }
        sel_table = st.selectbox("Select table to preview", list(tables_meta.keys()),
                                 format_func=lambda k: f"{k} — {tables_meta[k]}")
        tbl_df = D.get(sel_table, pd.DataFrame())
        if tbl_df.empty:
            alert_box("amber", f"Table {sel_table} not yet generated. Run update_combined.py.")
        else:
            st.info(f"**{sel_table}** — {tables_meta[sel_table]} | {len(tbl_df):,} rows × {tbl_df.shape[1]} cols")
            st.dataframe(tbl_df, use_container_width=True, height=420)
            csv = tbl_df.to_csv(index=False).encode("utf-8")
            st.download_button(f"⬇ Download {sel_table}.csv", csv,
                               file_name=f"{sel_table}.csv", mime="text/csv")


# ── FOOTER ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="margin-top:3rem;padding:1rem;border-top:1px solid #1e3a5f;
            text-align:center;font-size:11px;color:#2a4a6a;">
  MSBA 305 — Maritime Shipping Intelligence Pipeline &nbsp;·&nbsp;
  Sources: UN Comtrade · BDI (investing.com, manual) · OpenWeatherMap · AISStream &nbsp;·&nbsp;
  Storage: Google BigQuery &nbsp;·&nbsp; 10 analytical insight tables
</div>
""", unsafe_allow_html=True)
