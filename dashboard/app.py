"""
dashboard/app.py
Maritime Shipping Intelligence Dashboard
MSBA 305 | Run: streamlit run dashboard/app.py
"""

import os
import json
import warnings
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import streamlit as st
from datetime import datetime, date

warnings.filterwarnings("ignore")

# ── PAGE CONFIG ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Maritime Shipping Intelligence",
    page_icon="🚢",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap');

  html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

  /* Sidebar */
  [data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0a1628 0%, #0d2137 100%);
    border-right: 1px solid #1e3a5f;
  }
  [data-testid="stSidebar"] * { color: #c8d8e8 !important; }
  [data-testid="stSidebar"] .stSelectbox label,
  [data-testid="stSidebar"] .stSlider label,
  [data-testid="stSidebar"] .stMultiSelect label { color: #8aacc8 !important; font-size: 12px; }

  /* Main background */
  .main { background-color: #060f1a; }
  [data-testid="stAppViewContainer"] { background-color: #060f1a; }
  [data-testid="block-container"] { padding: 1.5rem 2rem; }

  /* Metric cards */
  [data-testid="metric-container"] {
    background: linear-gradient(135deg, #0d2137 0%, #112840 100%);
    border: 1px solid #1e3a5f;
    border-radius: 12px;
    padding: 1rem 1.25rem;
  }
  [data-testid="metric-container"] label { color: #8aacc8 !important; font-size: 12px !important; }
  [data-testid="metric-container"] [data-testid="stMetricValue"] {
    color: #e8f0f8 !important; font-size: 24px !important; font-weight: 600 !important;
  }
  [data-testid="metric-container"] [data-testid="stMetricDelta"] { font-size: 12px !important; }

  /* Section headers */
  .section-header {
    font-size: 11px; font-weight: 600; letter-spacing: 1.5px;
    color: #4a7fa5; text-transform: uppercase; margin: 1.5rem 0 0.75rem;
    padding-bottom: 6px; border-bottom: 1px solid #1e3a5f;
  }

  /* Nav pills in sidebar */
  .nav-item {
    display: block; padding: 10px 14px; margin: 4px 0;
    border-radius: 8px; cursor: pointer; font-size: 13px; font-weight: 500;
    color: #8aacc8; transition: all 0.15s; text-decoration: none;
  }
  .nav-item:hover, .nav-item.active {
    background: rgba(56, 122, 221, 0.15); color: #7eb8e8;
    border-left: 3px solid #378ADD;
  }

  /* Info boxes */
  .info-box {
    background: rgba(56,122,221,0.08); border: 1px solid rgba(56,122,221,0.25);
    border-radius: 10px; padding: 12px 16px; margin: 8px 0;
    font-size: 13px; color: #8aacc8; line-height: 1.5;
  }

  /* Risk badges */
  .badge-risk { background:#4a1a1a; color:#ff6b6b; padding:3px 10px; border-radius:20px; font-size:11px; font-weight:600; }
  .badge-ok   { background:#1a3a1a; color:#6bcf7f; padding:3px 10px; border-radius:20px; font-size:11px; font-weight:600; }
  .badge-warn { background:#3a2a0a; color:#ffc65c; padding:3px 10px; border-radius:20px; font-size:11px; font-weight:600; }

  /* Page title */
  .page-title {
    font-size: 26px; font-weight: 600; color: #e8f0f8; margin-bottom: 4px;
  }
  .page-sub { font-size: 13px; color: #4a7fa5; margin-bottom: 1.5rem; }

  /* Plotly charts dark */
  .js-plotly-plot { border-radius: 12px; }
  [data-testid="stDataFrame"] { border-radius: 10px; }

  /* Divider */
  hr { border-color: #1e3a5f; margin: 1rem 0; }

  /* Selectbox, slider */
  .stSelectbox > div > div, .stMultiSelect > div > div {
    background: #0d2137 !important; border-color: #1e3a5f !important; color: #c8d8e8 !important;
  }
  .stSlider [data-testid="stTickBarMin"], .stSlider [data-testid="stTickBarMax"] { color: #4a7fa5 !important; }
</style>
""", unsafe_allow_html=True)

# ── PLOTLY THEME ───────────────────────────────────────────────────────────────
DARK_LAYOUT = dict(
    paper_bgcolor="#0d2137",
    plot_bgcolor="#0a1628",
    font=dict(color="#8aacc8", family="Inter, sans-serif", size=12),
    xaxis=dict(gridcolor="#1e3a5f", linecolor="#1e3a5f", zerolinecolor="#1e3a5f"),
    yaxis=dict(gridcolor="#1e3a5f", linecolor="#1e3a5f", zerolinecolor="#1e3a5f"),
    legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="#1e3a5f"),
    margin=dict(t=40, b=40, l=50, r=20),
)

COLORS = dict(
    blue="#378ADD", teal="#1D9E75", amber="#EF9F27",
    coral="#D85A30", purple="#7F77DD", gray="#888780",
    green="#639922", red="#E24B4A",
)

# ── DATA LOADING ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def load_from_bigquery(project_id: str, dataset: str, key_path: str) -> dict:
    """Load all tables from BigQuery."""
    from google.cloud import bigquery
    from google.oauth2 import service_account

    creds = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(project=project_id, credentials=creds)

    tables = {}
    for tbl in ["bdi_daily", "trade_flows", "port_weather", "vessel_movements"]:
        try:
            q = f"SELECT * FROM `{project_id}.{dataset}.{tbl}`"
            tables[tbl] = client.query(q).to_dataframe()
            tables[tbl].columns = [c.lower() for c in tables[tbl].columns]
        except Exception as e:
            st.warning(f"Could not load {tbl}: {e}")
            tables[tbl] = pd.DataFrame()
    return tables


@st.cache_data(ttl=3600, show_spinner=False)
def load_from_csv() -> dict:
    """Fallback: load from local clean CSVs."""
    base = os.path.join(os.path.dirname(__file__), "..", "data", "clean")
    return {
        "bdi_daily":        pd.read_csv(os.path.join(base, "bdi_clean.csv"),           low_memory=False),
        "trade_flows":      pd.read_csv(os.path.join(base, "un_comtrade_clean.csv"),    low_memory=False),
        "port_weather":     pd.read_csv(os.path.join(base, "port_weather_clean.csv"),   low_memory=False),
        "vessel_movements": pd.read_csv(os.path.join(base, "aisstream_clean.csv"),      low_memory=False),
    }


def get_data(project_id, dataset, key_path):
    if project_id and dataset and key_path and os.path.exists(key_path):
        try:
            return load_from_bigquery(project_id, dataset, key_path), "BigQuery"
        except Exception:
            pass
    return load_from_csv(), "Local CSV"


# ── SIDEBAR ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div style="font-size:22px;font-weight:700;color:#e8f0f8;margin-bottom:4px;">⚓ Maritime Intel</div>', unsafe_allow_html=True)
    st.markdown('<div style="font-size:11px;color:#4a7fa5;margin-bottom:20px;">MSBA 305 | Shipping Pipeline</div>', unsafe_allow_html=True)

    page = st.radio(
        "Navigation",
        ["Baltic Dry Index", "Trade Analysis", "Port Weather", "Vessel Activity"],
        label_visibility="collapsed",
    )

    st.markdown('<div class="section-header">Data Connection</div>', unsafe_allow_html=True)
    use_bq = st.toggle("Connect to BigQuery", value=False)

    if use_bq:
        bq_project = st.text_input("Project ID", placeholder="msba305-shipping-123")
        bq_dataset = st.text_input("Dataset", value="shipping_data")
        key_file   = st.text_input("Key file path", value="gcp_key.json")
    else:
        bq_project = bq_dataset = key_file = ""

    st.markdown('<div class="section-header">About</div>', unsafe_allow_html=True)
    st.markdown('<div style="font-size:11px;color:#4a7fa5;line-height:1.6;">4 data sources: UN Comtrade, Baltic Dry Index, OpenWeatherMap, AIS Stream.<br><br>Pipeline: Colab notebooks → BigQuery → This dashboard.</div>', unsafe_allow_html=True)

# ── LOAD DATA ──────────────────────────────────────────────────────────────────
with st.spinner("Loading data..."):
    data, data_source = get_data(
        bq_project if use_bq else "",
        bq_dataset if use_bq else "",
        key_file   if use_bq else "",
    )

bdi   = data.get("bdi_daily", pd.DataFrame())
trade = data.get("trade_flows", pd.DataFrame())
wx    = data.get("port_weather", pd.DataFrame())
ais   = data.get("vessel_movements", pd.DataFrame())

if not bdi.empty and "date" in bdi.columns:
    bdi["date"] = pd.to_datetime(bdi["date"])
if not trade.empty and "year" in trade.columns:
    trade["year"] = pd.to_numeric(trade["year"], errors="coerce")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — BALTIC DRY INDEX
# ══════════════════════════════════════════════════════════════════════════════
if page == "Baltic Dry Index":
    st.markdown('<div class="page-title">Baltic Dry Index</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Global bulk shipping cost indicator · Source: investing.com · Loaded from {data_source}</div>', unsafe_allow_html=True)

    if bdi.empty:
        st.error("BDI data not available."); st.stop()

    # ── Filters ──
    col_f1, col_f2, col_f3 = st.columns([2, 2, 1])
    with col_f1:
        years = sorted(bdi["date"].dt.year.unique().tolist())
        yr_range = st.select_slider("Year range", options=years, value=(years[0], years[-1]))
    with col_f2:
        show_avg = st.multiselect(
            "Rolling averages",
            ["7-day", "30-day", "90-day"],
            default=["30-day"],
        )
    with col_f3:
        show_events = st.checkbox("Show events", value=True)

    df = bdi[(bdi["date"].dt.year >= yr_range[0]) & (bdi["date"].dt.year <= yr_range[1])].copy()

    # ── KPI row ──
    latest    = df.iloc[-1]
    prev_week = df.iloc[-6] if len(df) > 6 else df.iloc[0]
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Current BDI",    f"{latest['bdi_value']:.0f}",
              delta=f"{latest['bdi_value'] - prev_week['bdi_value']:+.0f} vs 5d ago")
    k2.metric("Period high",    f"{df['bdi_value'].max():.0f}")
    k3.metric("Period low",     f"{df['bdi_value'].min():.0f}")
    k4.metric("Avg BDI",        f"{df['bdi_value'].mean():.0f}")
    spikes = int(df["is_spike"].sum()) if "is_spike" in df.columns else 0
    drops  = int(df["is_drop"].sum())  if "is_drop"  in df.columns else 0
    k5.metric("Spikes / Drops", f"{spikes} / {drops}")

    # ── Main chart ──
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["bdi_value"], name="BDI",
        line=dict(color=COLORS["blue"], width=1.2),
        fill="tozeroy", fillcolor="rgba(55,138,221,0.07)",
    ))
    avg_map = {"7-day": ("rolling_7d_avg", COLORS["teal"]),
               "30-day": ("rolling_30d_avg", COLORS["amber"]),
               "90-day": ("rolling_90d_avg", COLORS["coral"])}
    for label in show_avg:
        col_name, color = avg_map[label]
        if col_name in df.columns:
            fig.add_trace(go.Scatter(
                x=df["date"], y=df[col_name], name=label,
                line=dict(color=color, width=1.8, dash="dot"),
            ))

    if show_events:
        events = {
            "2016-02-01": "BDI low",
            "2020-03-01": "COVID",
            "2021-03-23": "Suez blockage",
            "2021-10-01": "Supply crisis",
            "2022-03-01": "Ukraine war",
        }
        for d, label in events.items():
            ts = pd.Timestamp(d)
            if yr_range[0] <= ts.year <= yr_range[1]:
                fig.add_vline(x=ts, line_dash="dash", line_color="#2a4a6a", line_width=1)
                fig.add_annotation(x=ts, y=df["bdi_value"].max() * 0.92,
                                   text=label, showarrow=False,
                                   font=dict(color="#4a7fa5", size=10),
                                   textangle=-90, yanchor="top")

    fig.update_layout(**DARK_LAYOUT, title="Baltic Dry Index over time",
                      height=400, hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    # ── Volatility + YoY ──
    c1, c2 = st.columns(2)

    with c1:
        st.markdown('<div class="section-header">Spikes & drops per year</div>', unsafe_allow_html=True)
        if "is_spike" in df.columns and "is_drop" in df.columns:
            yr_grp = df.copy()
            yr_grp["yr"] = yr_grp["date"].dt.year
            gy = yr_grp.groupby("yr").agg(spikes=("is_spike","sum"), drops=("is_drop","sum")).reset_index()
            fig2 = go.Figure()
            fig2.add_bar(x=gy["yr"].astype(str), y=gy["spikes"], name="Spikes", marker_color=COLORS["teal"])
            fig2.add_bar(x=gy["yr"].astype(str), y=gy["drops"],  name="Drops",  marker_color=COLORS["coral"])
            fig2.update_layout(**DARK_LAYOUT, barmode="group", height=280,
                               xaxis_title="Year", yaxis_title="Days", margin=dict(t=20,b=40,l=40,r=10))
            st.plotly_chart(fig2, use_container_width=True)

    with c2:
        st.markdown('<div class="section-header">Monthly average BDI (all years)</div>', unsafe_allow_html=True)
        df_m = df.copy()
        df_m["month"] = df_m["date"].dt.month
        monthly = df_m.groupby("month")["bdi_value"].mean().reset_index()
        mnames  = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        monthly["month_name"] = monthly["month"].apply(lambda x: mnames[x-1])
        fig3 = go.Figure(go.Bar(
            x=monthly["month_name"], y=monthly["bdi_value"].round(0),
            marker_color=[COLORS["blue"] if v >= monthly["bdi_value"].mean() else COLORS["gray"]
                          for v in monthly["bdi_value"]],
            text=monthly["bdi_value"].round(0), textposition="outside",
            textfont=dict(size=10, color="#8aacc8"),
        ))
        fig3.update_layout(**DARK_LAYOUT, height=280, xaxis_title="Month",
                           yaxis_title="Avg BDI", margin=dict(t=20,b=40,l=40,r=10))
        st.plotly_chart(fig3, use_container_width=True)

    # ── Raw data toggle ──
    with st.expander("View raw BDI data"):
        show_cols = ["date","bdi_value","daily_change_pct","rolling_30d_avg","is_spike","is_drop"]
        show_cols = [c for c in show_cols if c in df.columns]
        st.dataframe(df[show_cols].sort_values("date", ascending=False).head(100),
                     use_container_width=True, height=300)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — TRADE ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Trade Analysis":
    st.markdown('<div class="page-title">Global Trade Analysis</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">UN Comtrade — bilateral trade flows by commodity · Loaded from {data_source}</div>', unsafe_allow_html=True)

    if trade.empty:
        st.error("Trade data not available."); st.stop()

    HS_NAMES = {10:"Cereals", 26:"Ores & slag", 27:"Mineral fuels",
                72:"Iron & steel", 89:"Ships & boats"}

    # ── Filters ──
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        all_years = sorted(trade["year"].dropna().unique().astype(int).tolist())
        sel_years = st.select_slider("Year range", options=all_years, value=(all_years[0], all_years[-1]))
    with fc2:
        all_hs = sorted(trade["hs_code"].dropna().unique().astype(int).tolist())
        hs_labels = {k: f"HS {k} — {HS_NAMES.get(k,'Other')}" for k in all_hs}
        sel_hs = st.multiselect("Commodity (HS code)",
                                options=all_hs, default=all_hs,
                                format_func=lambda x: hs_labels[x])
    with fc3:
        flow = st.selectbox("Trade flow", ["Both", "Export", "Import"])

    df_t = trade[(trade["year"] >= sel_years[0]) & (trade["year"] <= sel_years[1])].copy()
    if sel_hs:
        df_t = df_t[df_t["hs_code"].isin(sel_hs)]
    if flow != "Both":
        df_t = df_t[df_t["flow_direction"] == flow]

    # ── KPIs ──
    k1, k2, k3, k4 = st.columns(4)
    total_val = df_t["trade_value_usd"].sum() / 1e12
    k1.metric("Total trade value", f"${total_val:.1f}T")
    k2.metric("Countries", f"{df_t['reporter_iso'].nunique()}")
    k3.metric("Years covered", f"{sel_years[0]}–{sel_years[1]}")
    exp_only = df_t[df_t["flow_direction"]=="Export"]["trade_value_usd"].sum()
    imp_only = df_t[df_t["flow_direction"]=="Import"]["trade_value_usd"].sum()
    k4.metric("Export / Import ratio", f"{exp_only/imp_only:.2f}" if imp_only else "N/A")

    # ── Top exporters & importers ──
    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="section-header">Top 15 exporters</div>', unsafe_allow_html=True)
        exp = (df_t[df_t["flow_direction"]=="Export"]
               .groupby("reporter_country")["trade_value_usd"]
               .sum().nlargest(15).reset_index())
        exp["val_T"] = exp["trade_value_usd"] / 1e12
        fig = go.Figure(go.Bar(
            x=exp["val_T"], y=exp["reporter_country"],
            orientation="h", marker_color=COLORS["blue"],
            text=exp["val_T"].round(1).astype(str) + "T",
            textposition="outside", textfont=dict(size=10, color="#8aacc8"),
        ))
        fig.update_layout(**DARK_LAYOUT, height=420, xaxis_title="USD Trillion",
                          yaxis=dict(autorange="reversed", **DARK_LAYOUT["yaxis"]),
                          margin=dict(t=10,b=40,l=10,r=60))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown('<div class="section-header">Trade balance: net exporters vs importers</div>', unsafe_allow_html=True)
        piv = df_t.pivot_table(
            index="reporter_country", columns="flow_direction",
            values="trade_value_usd", aggfunc="sum"
        ).reset_index()
        piv.columns.name = None
        if "Export" in piv.columns and "Import" in piv.columns:
            piv["balance"] = (piv.get("Export",0).fillna(0) - piv.get("Import",0).fillna(0)) / 1e12
            top_bal = pd.concat([piv.nlargest(8,"balance"), piv.nsmallest(8,"balance")])
            colors_bal = [COLORS["teal"] if v > 0 else COLORS["coral"] for v in top_bal["balance"]]
            fig2 = go.Figure(go.Bar(
                x=top_bal["balance"], y=top_bal["reporter_country"],
                orientation="h", marker_color=colors_bal,
            ))
            fig2.add_vline(x=0, line_color="#4a7fa5", line_width=1)
            fig2.update_layout(**DARK_LAYOUT, height=420, xaxis_title="Balance (USD Trillion)",
                               yaxis=dict(autorange="reversed", **DARK_LAYOUT["yaxis"]),
                               margin=dict(t=10,b=40,l=10,r=20))
            st.plotly_chart(fig2, use_container_width=True)

    # ── Trade over time by commodity ──
    st.markdown('<div class="section-header">Trade value over time by commodity</div>', unsafe_allow_html=True)
    time_df = df_t.groupby(["year","hs_code"])["trade_value_usd"].sum().reset_index()
    time_df["val_B"] = time_df["trade_value_usd"] / 1e9
    time_df["commodity"] = time_df["hs_code"].map(HS_NAMES).fillna("Other")
    hs_colors = {10:COLORS["teal"], 26:COLORS["coral"], 27:COLORS["amber"],
                 72:COLORS["purple"], 89:COLORS["blue"]}

    fig3 = go.Figure()
    for hs in time_df["hs_code"].unique():
        sub = time_df[time_df["hs_code"]==hs]
        fig3.add_trace(go.Scatter(
            x=sub["year"], y=sub["val_B"],
            name=hs_labels.get(int(hs), str(hs)),
            mode="lines+markers",
            line=dict(color=hs_colors.get(int(hs), COLORS["gray"]), width=2),
            marker=dict(size=6),
        ))
    fig3.update_layout(**DARK_LAYOUT, height=320, hovermode="x unified",
                       xaxis_title="Year", yaxis_title="Trade Value (USD Billion)")
    st.plotly_chart(fig3, use_container_width=True)

    # ── YoY growth heatmap ──
    if "yoy_growth_pct" in df_t.columns:
        st.markdown('<div class="section-header">Year-over-year export growth % — top 20 countries</div>', unsafe_allow_html=True)
        yoy = (df_t[df_t["flow_direction"]=="Export"]
               .groupby(["reporter_country","year"])["yoy_growth_pct"]
               .mean().reset_index())
        top20 = (df_t[df_t["flow_direction"]=="Export"]
                 .groupby("reporter_country")["trade_value_usd"]
                 .sum().nlargest(20).index.tolist())
        yoy20 = yoy[yoy["reporter_country"].isin(top20)]
        pivot_yoy = yoy20.pivot(index="reporter_country", columns="year", values="yoy_growth_pct")
        fig4 = go.Figure(go.Heatmap(
            z=pivot_yoy.values,
            x=pivot_yoy.columns.astype(str),
            y=pivot_yoy.index,
            colorscale=[[0,"#D85A30"],[0.5,"#0d2137"],[1,"#1D9E75"]],
            zmid=0, zmin=-30, zmax=30,
            colorbar=dict(title="%", tickfont=dict(color="#8aacc8")),
            text=pivot_yoy.values.round(1),
            texttemplate="%{text}",
            textfont=dict(size=9),
        ))
        fig4.update_layout(**DARK_LAYOUT, height=420,
                           xaxis_title="Year", margin=dict(t=10,b=40,l=140,r=20))
        st.plotly_chart(fig4, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — PORT WEATHER
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Port Weather":
    st.markdown('<div class="page-title">Port Weather Monitor</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Real-time conditions at top 20 global ports · OpenWeatherMap API · Loaded from {data_source}</div>', unsafe_allow_html=True)

    if wx.empty:
        st.error("Weather data not available."); st.stop()

    # ensure numeric
    for col in ["temp_c","wind_speed_ms","humidity_pct","beaufort_number","visibility_m"]:
        if col in wx.columns:
            wx[col] = pd.to_numeric(wx[col], errors="coerce")

    if "port_risk_flag" in wx.columns:
        wx["port_risk_flag"] = wx["port_risk_flag"].astype(bool)

    # ── Filters ──
    fc1, fc2 = st.columns(2)
    with fc1:
        risk_filter = st.selectbox("Show ports", ["All ports", "Risk ports only", "Normal only"])
    with fc2:
        sort_by = st.selectbox("Sort by", ["Wind speed (high→low)", "Temperature (high→low)",
                                           "Port rank", "Humidity"])

    df_wx = wx.copy()
    if risk_filter == "Risk ports only":
        df_wx = df_wx[df_wx["port_risk_flag"] == True]
    elif risk_filter == "Normal only":
        df_wx = df_wx[df_wx["port_risk_flag"] == False]

    sort_map = {
        "Wind speed (high→low)": ("wind_speed_ms", False),
        "Temperature (high→low)": ("temp_c", False),
        "Port rank": ("port_rank", True),
        "Humidity": ("humidity_pct", False),
    }
    sc, sa = sort_map[sort_by]
    df_wx = df_wx.sort_values(sc, ascending=sa)

    # ── KPIs ──
    risk_count = int(wx["port_risk_flag"].sum()) if "port_risk_flag" in wx.columns else 0
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Ports monitored", len(wx))
    k2.metric("Ports at risk", risk_count, delta=f"Beaufort ≥ 7")
    k3.metric("Avg wind speed", f"{wx['wind_speed_ms'].mean():.1f} m/s" if "wind_speed_ms" in wx.columns else "N/A")
    k4.metric("Avg temperature", f"{wx['temp_c'].mean():.1f}°C" if "temp_c" in wx.columns else "N/A")
    k5.metric("Avg humidity", f"{wx['humidity_pct'].mean():.0f}%" if "humidity_pct" in wx.columns else "N/A")

    # ── Port status table ──
    st.markdown('<div class="section-header">Live port conditions</div>', unsafe_allow_html=True)

    BEAUFORT_DESC = {0:"Calm",1:"Light air",2:"Light breeze",3:"Gentle breeze",
                    4:"Moderate breeze",5:"Fresh breeze",6:"Strong breeze",
                    7:"Near gale",8:"Gale",9:"Strong gale",10:"Storm",11:"Violent storm",12:"Hurricane"}

    for _, row in df_wx.iterrows():
        risk = bool(row.get("port_risk_flag", False)) if "port_risk_flag" in row.index else False
        bf   = int(row.get("beaufort_number", 0))
        temp = float(row.get("temp_c", 0))

        badge = f'<span class="badge-risk">⚠ Near gale+</span>' if risk else \
                f'<span class="badge-warn">● Moderate</span>' if bf >= 4 else \
                f'<span class="badge-ok">● Normal</span>'

        temp_str = f"{temp:.1f}°C"
        wind_str = f"{row.get('wind_speed_ms',0):.1f} m/s"
        desc_str = BEAUFORT_DESC.get(bf, f"Bf {bf}")
        hum_str  = f"{row.get('humidity_pct',0):.0f}%"
        vis_str  = f"{int(row.get('visibility_m',10000)/1000)}km"
        wx_main  = str(row.get("weather_main",""))
        country  = str(row.get("country_iso",""))

        st.markdown(f"""
        <div style="display:flex;align-items:center;gap:16px;padding:9px 14px;margin-bottom:5px;
                    background:{'rgba(74,26,26,0.3)' if risk else 'rgba(13,33,55,0.5)'};
                    border:1px solid {'#4a1a1a' if risk else '#1e3a5f'};border-radius:10px;">
          <div style="font-size:12px;font-weight:600;color:#e8f0f8;min-width:160px;">{row.get('port_name','')} <span style="color:#4a7fa5;font-weight:400;">({country})</span></div>
          <div style="font-size:12px;color:#8aacc8;min-width:60px;">🌡 {temp_str}</div>
          <div style="font-size:12px;color:#8aacc8;min-width:80px;">💨 {wind_str}</div>
          <div style="font-size:12px;color:#8aacc8;min-width:100px;">{desc_str}</div>
          <div style="font-size:12px;color:#8aacc8;min-width:50px;">💧 {hum_str}</div>
          <div style="font-size:12px;color:#8aacc8;min-width:50px;">👁 {vis_str}</div>
          <div style="font-size:12px;color:#8aacc8;flex:1;">{wx_main}</div>
          <div>{badge}</div>
        </div>""", unsafe_allow_html=True)

    # ── Charts ──
    c1, c2 = st.columns(2)
    with c1:
        st.markdown('<div class="section-header">Wind speed by port</div>', unsafe_allow_html=True)
        ws = df_wx.sort_values("wind_speed_ms", ascending=True)
        risk_colors = [COLORS["coral"] if r else COLORS["blue"] for r in ws.get("port_risk_flag", [False]*len(ws))]
        fig = go.Figure(go.Bar(
            x=ws["wind_speed_ms"], y=ws["port_name"],
            orientation="h", marker_color=risk_colors,
        ))
        fig.add_vline(x=13.9, line_dash="dash", line_color=COLORS["amber"], line_width=1.5,
                      annotation_text="Near gale", annotation_font_color=COLORS["amber"])
        fig.update_layout(**DARK_LAYOUT, height=480, xaxis_title="m/s",
                          margin=dict(t=10,b=40,l=10,r=20))
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown('<div class="section-header">Temperature & humidity scatter</div>', unsafe_allow_html=True)
        fig2 = go.Figure(go.Scatter(
            x=wx["temp_c"], y=wx["humidity_pct"],
            mode="markers+text",
            text=wx["port_name"],
            textposition="top center",
            textfont=dict(size=9, color="#4a7fa5"),
            marker=dict(
                size=wx["wind_speed_ms"] * 2.5 + 8 if "wind_speed_ms" in wx.columns else 12,
                color=wx["beaufort_number"] if "beaufort_number" in wx.columns else COLORS["blue"],
                colorscale=[[0,COLORS["teal"]], [0.5,COLORS["amber"]], [1,COLORS["coral"]]],
                showscale=True,
                colorbar=dict(title="Beaufort", tickfont=dict(color="#8aacc8")),
                line=dict(color="#1e3a5f", width=1),
            ),
        ))
        fig2.update_layout(**DARK_LAYOUT, height=480,
                           xaxis_title="Temperature (°C)", yaxis_title="Humidity (%)",
                           margin=dict(t=10,b=40,l=50,r=10))
        st.plotly_chart(fig2, use_container_width=True)

    # ── Weather breakdown ──
    st.markdown('<div class="section-header">Weather condition breakdown</div>', unsafe_allow_html=True)
    wc = wx["weather_main"].value_counts().reset_index()
    wc.columns = ["condition","count"]
    pal = [COLORS["blue"],COLORS["teal"],COLORS["purple"],COLORS["amber"],
           COLORS["coral"],COLORS["gray"],COLORS["green"]]
    fig3 = go.Figure(go.Pie(
        labels=wc["condition"], values=wc["count"],
        marker_colors=pal[:len(wc)],
        hole=0.5, textinfo="label+percent",
        textfont=dict(color="#e8f0f8", size=12),
    ))
    fig3.update_layout(**DARK_LAYOUT, height=280, showlegend=False,
                       margin=dict(t=10,b=10,l=10,r=10))
    st.plotly_chart(fig3, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — VESSEL ACTIVITY
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Vessel Activity":
    st.markdown('<div class="page-title">Vessel Activity — Singapore Port</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">AIS Stream — real-time vessel tracking · World\'s 2nd busiest port · Loaded from {data_source}</div>', unsafe_allow_html=True)

    if ais.empty:
        st.error("AIS data not available."); st.stop()

    for col in ["sog_knots","latitude","longitude"]:
        if col in ais.columns:
            ais[col] = pd.to_numeric(ais[col], errors="coerce")

    # ── Filters ──
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        if "vessel_category" in ais.columns:
            cats = ["All"] + sorted(ais["vessel_category"].dropna().unique().tolist())
            sel_cat = st.selectbox("Vessel category", cats)
        else:
            sel_cat = "All"
    with fc2:
        if "speed_category" in ais.columns:
            spds = ["All"] + sorted(ais["speed_category"].dropna().unique().tolist())
            sel_spd = st.selectbox("Speed category", spds)
        else:
            sel_spd = "All"
    with fc3:
        min_sog = st.slider("Min speed (knots)", 0.0, 25.0, 0.0, 0.5)

    df_a = ais.copy()
    if sel_cat != "All" and "vessel_category" in df_a.columns:
        df_a = df_a[df_a["vessel_category"] == sel_cat]
    if sel_spd != "All" and "speed_category" in df_a.columns:
        df_a = df_a[df_a["speed_category"] == sel_spd]
    if "sog_knots" in df_a.columns:
        df_a = df_a[df_a["sog_knots"].fillna(0) >= min_sog]

    # ── KPIs ──
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total messages",  f"{len(df_a):,}")
    k2.metric("Unique vessels",  f"{df_a['mmsi'].nunique():,}" if "mmsi" in df_a.columns else "N/A")
    moving = int((ais["is_moving"]==True).sum()) if "is_moving" in ais.columns else 0
    k3.metric("Moving vessels",  f"{moving}")
    avg_sog = df_a["sog_knots"].mean() if "sog_knots" in df_a.columns else 0
    k4.metric("Avg speed",       f"{avg_sog:.1f} kn")
    tankers = int((ais["vessel_category"]=="Tanker").sum()) if "vessel_category" in ais.columns else 0
    k5.metric("Tankers",         f"{tankers}")

    # ── Map + breakdown ──
    c1, c2 = st.columns([3, 2])

    with c1:
        st.markdown('<div class="section-header">Vessel positions — Singapore</div>', unsafe_allow_html=True)
        pos = df_a.dropna(subset=["latitude","longitude"])
        if not pos.empty:
            cat_color_map = {
                "Tanker": COLORS["coral"], "Cargo": COLORS["blue"],
                "Tug / Support": COLORS["amber"], "Fishing": COLORS["teal"],
                "Passenger": COLORS["purple"], "Unknown": COLORS["gray"], "Other": COLORS["gray"],
            }
            fig = go.Figure()
            for cat in pos.get("vessel_category", pd.Series(dtype=str)).unique() if "vessel_category" in pos.columns else ["Unknown"]:
                sub = pos[pos["vessel_category"]==cat] if "vessel_category" in pos.columns else pos
                moving_mask = sub["is_moving"] == True if "is_moving" in sub.columns else pd.Series([True]*len(sub))
                fig.add_trace(go.Scattergl(
                    x=sub.loc[moving_mask,"longitude"], y=sub.loc[moving_mask,"latitude"],
                    mode="markers", name=f"{cat} (moving)",
                    marker=dict(size=7, color=cat_color_map.get(cat, COLORS["gray"]),
                                symbol="arrow", opacity=0.85),
                    hovertemplate=(
                        "<b>%{customdata[0]}</b><br>"
                        "Speed: %{customdata[1]:.1f} kn<br>"
                        "Status: %{customdata[2]}<extra></extra>"
                    ),
                    customdata=sub.loc[moving_mask, [
                        "ship_name" if "ship_name" in sub.columns else "mmsi",
                        "sog_knots" if "sog_knots" in sub.columns else "mmsi",
                        "nav_status_name" if "nav_status_name" in sub.columns else "mmsi",
                    ]].fillna("Unknown"),
                ))
                fig.add_trace(go.Scattergl(
                    x=sub.loc[~moving_mask,"longitude"], y=sub.loc[~moving_mask,"latitude"],
                    mode="markers", name=f"{cat} (stationary)",
                    marker=dict(size=5, color=cat_color_map.get(cat, COLORS["gray"]),
                                symbol="circle", opacity=0.5),
                ))

            fig.update_layout(
                **DARK_LAYOUT, height=460,
                xaxis_title="Longitude", yaxis_title="Latitude",
                xaxis=dict(range=[103.4, 104.4], **DARK_LAYOUT["xaxis"]),
                yaxis=dict(range=[1.0, 1.6], **DARK_LAYOUT["yaxis"]),
                margin=dict(t=10, b=40, l=50, r=10),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No position data with current filters.")

    with c2:
        # Vessel type pie
        st.markdown('<div class="section-header">Vessel types (classified)</div>', unsafe_allow_html=True)
        if "vessel_category" in df_a.columns:
            vc = df_a[df_a["vessel_category"] != "Unknown"]["vessel_category"].value_counts().reset_index()
            vc.columns = ["category","count"]
            if not vc.empty:
                pal = [cat_color_map.get(c, COLORS["gray"]) for c in vc["category"]]
                fig2 = go.Figure(go.Pie(
                    labels=vc["category"], values=vc["count"],
                    marker_colors=pal, hole=0.55,
                    textinfo="label+value", textfont=dict(color="#e8f0f8", size=11),
                ))
                fig2.update_layout(**DARK_LAYOUT, height=230, showlegend=False,
                                   margin=dict(t=10,b=10,l=10,r=10))
                st.plotly_chart(fig2, use_container_width=True)

        # Speed category bar
        st.markdown('<div class="section-header">Speed categories</div>', unsafe_allow_html=True)
        if "speed_category" in df_a.columns:
            sc_order = ["Stationary","Slow / manoeuvring","Transit","Cruising","Unknown"]
            sc = df_a["speed_category"].value_counts().reindex(sc_order).dropna().reset_index()
            sc.columns = ["category","count"]
            sc_colors = {
                "Stationary": COLORS["purple"], "Slow / manoeuvring": COLORS["amber"],
                "Transit": COLORS["teal"], "Cruising": COLORS["blue"], "Unknown": COLORS["gray"],
            }
            fig3 = go.Figure(go.Bar(
                x=sc["category"], y=sc["count"],
                marker_color=[sc_colors.get(c, COLORS["gray"]) for c in sc["category"]],
                text=sc["count"], textposition="outside", textfont=dict(size=10, color="#8aacc8"),
            ))
            fig3.update_layout(**DARK_LAYOUT, height=230, showlegend=False,
                               margin=dict(t=10,b=50,l=30,r=10),
                               xaxis_tickangle=-30)
            st.plotly_chart(fig3, use_container_width=True)

    # ── Navigational status ──
    st.markdown('<div class="section-header">Navigational status breakdown</div>', unsafe_allow_html=True)
    if "nav_status_name" in df_a.columns:
        ns = df_a["nav_status_name"].value_counts().head(8).reset_index()
        ns.columns = ["status","count"]
        fig4 = go.Figure(go.Bar(
            x=ns["count"], y=ns["status"], orientation="h",
            marker_color=COLORS["blue"],
            text=ns["count"], textposition="outside", textfont=dict(size=10, color="#8aacc8"),
        ))
        fig4.update_layout(**DARK_LAYOUT, height=280,
                           yaxis=dict(autorange="reversed", **DARK_LAYOUT["yaxis"]),
                           xaxis_title="Vessel count", margin=dict(t=10,b=40,l=10,r=60))
        st.plotly_chart(fig4, use_container_width=True)

    # ── Raw data table ──
    with st.expander("View vessel data table"):
        show = ["mmsi","ship_name","vessel_type_name","vessel_category","sog_knots",
                "speed_category","nav_status_name","latitude","longitude","destination","port_guess"]
        show = [c for c in show if c in df_a.columns]
        st.dataframe(df_a[show].sort_values("sog_knots", ascending=False).head(200),
                     use_container_width=True, height=350)

# ── Footer ──
st.markdown("""
<div style="margin-top:3rem;padding:1rem;border-top:1px solid #1e3a5f;
            text-align:center;font-size:11px;color:#2a4a6a;">
  MSBA 305 — Maritime Shipping Intelligence Pipeline &nbsp;·&nbsp;
  Sources: UN Comtrade · Nasdaq BDI · OpenWeatherMap · AISStream &nbsp;·&nbsp;
  Storage: Google BigQuery
</div>
""", unsafe_allow_html=True)
