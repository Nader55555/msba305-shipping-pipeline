"""
scripts/build_route_analytics.py
Joins AIS + fuel + weather + news + BDI to produce route-level analytics.
Compares current vessel activity vs historical baseline per route.
Run: python scripts/build_route_analytics.py
"""
import os
import pathlib
import pandas as pd
import yaml

BQ_PROJECT = os.getenv("BQ_PROJECT", "msba305-shipping")
BQ_DATASET = os.getenv("BQ_DATASET", "shipping_data")
GCP_KEY    = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "gcp_key.json")

ROOT  = pathlib.Path(__file__).resolve().parents[1]
CLEAN = ROOT / "data" / "clean"
ROUTES_PATH = ROOT / "config" / "routes.yaml"

# Port → route mapping
PORT_TO_ROUTE = {
    "Singapore":        "Asia-Europe via Suez",
    "Shanghai":         "Asia-Europe via Suez",
    "Shenzhen":         "Asia-Europe via Suez",
    "Hong Kong":        "Asia-Europe via Suez",
    "Busan":            "Asia-Europe via Suez",
    "Rotterdam":        "Asia-Europe via Suez",
    "Antwerp":          "Asia-Europe via Suez",
    "Hamburg":          "North Sea / Dover",
    "Strait of Dover":  "North Sea / Dover",
    "Strait of Gibraltar": "Asia-Europe via Suez",
    "Suez Canal":       "Asia-Europe via Suez",
    "Bab el-Mandeb":    "Asia-Europe via Suez",
    "Strait of Malacca":"Asia-Europe via Suez",
    "Strait of Hormuz": "Strait of Hormuz",
    "Dubai":            "Strait of Hormuz",
    "Bosphorus Strait": "Black Sea",
    "Los Angeles":      "Trans-Pacific",
    "New York":         "Trans-Pacific",
}

STRAIT_TO_ROUTE = {
    "Strait of Hormuz":    "Strait of Hormuz",
    "Strait of Malacca":   "Asia-Europe via Suez",
    "Suez Canal":          "Asia-Europe via Suez",
    "Bab el-Mandeb":       "Asia-Europe via Suez",
    "Bosphorus Strait":    "Black Sea",
    "Strait of Dover":     "North Sea / Dover",
    "Strait of Gibraltar": "Asia-Europe via Suez",
    "Lombok Strait":       "Asia-Europe via Suez",
}

ALL_ROUTES = [
    "Asia-Europe via Suez",
    "Strait of Hormuz",
    "Black Sea",
    "Trans-Pacific",
    "North Sea / Dover",
    "Unmapped / Other",
]


def load(name):
    path = CLEAN / name
    return pd.read_csv(path, low_memory=False) if path.exists() else pd.DataFrame()


def save(df, name):
    out = CLEAN / name
    df.to_csv(out, index=False)
    print(f"  ✓ Saved {name}: {len(df):,} rows")


def map_to_route(port_guess):
    if not isinstance(port_guess, str):
        return "Unmapped / Other"
    for key, route in PORT_TO_ROUTE.items():
        if key.lower() in port_guess.lower():
            return route
    return "Unmapped / Other"


def upload_to_bq(df: pd.DataFrame, table_name: str) -> None:
    """Upload DataFrame to BigQuery — WRITE_TRUNCATE."""
    if df.empty:
        print(f"  ⚠ Skipping {table_name} — empty DataFrame")
        return
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
        creds  = service_account.Credentials.from_service_account_file(GCP_KEY)
        client = bigquery.Client(project=BQ_PROJECT, credentials=creds)
        table_ref  = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"  ✓ BigQuery: {table_name} ({len(df):,} rows)")
    except Exception as e:
        print(f"  ✗ BigQuery upload failed for {table_name}: {e}")


def main():
    print("\n=== BUILD ROUTE ANALYTICS ===")

    # ── LOAD ALL SOURCES ──────────────────────────────────────────────────────
    ais     = load("aisstream_clean.csv")
    fuel    = load("fuel_prices_daily.csv")
    news    = load("shipping_news.csv")
    bdi     = load("bdi_clean.csv")
    weather = load("port_weather_clean.csv")

    # ── 1. TAG AIS WITH ROUTE ─────────────────────────────────────────────────
    if not ais.empty and "port_guess" in ais.columns:
        ais["route"] = ais["port_guess"].apply(map_to_route)
        ais["fetch_date"] = pd.to_datetime(ais.get("fetch_date", pd.Timestamp.now()), errors="coerce")

    # ── 2. ROUTE BASELINES (historical averages per route) ────────────────────
    baselines = []
    if not ais.empty and "route" in ais.columns:
        hist = ais.groupby(["route", "fetch_date"]).agg(
            daily_vessels=("mmsi", "nunique"),
            avg_speed=("sog_knots", "mean"),
            tanker_count=("vessel_category", lambda x: (x == "Tanker").sum()),
            cargo_count=("vessel_category", lambda x: (x == "Cargo").sum()),
        ).reset_index()

        for route in ALL_ROUTES:
            sub = hist[hist["route"] == route]
            baselines.append({
                "route":                    route,
                "baseline_avg_vessels":     round(sub["daily_vessels"].mean(), 1) if not sub.empty else 0,
                "baseline_avg_speed":       round(sub["avg_speed"].mean(), 2)     if not sub.empty else 0,
                "baseline_tanker_count":    round(sub["tanker_count"].mean(), 1)  if not sub.empty else 0,
                "baseline_cargo_count":     round(sub["cargo_count"].mean(), 1)   if not sub.empty else 0,
                "days_of_history":          len(sub["fetch_date"].unique())        if not sub.empty else 0,
            })
    baseline_df = pd.DataFrame(baselines)
    save(baseline_df, "route_baselines.csv")
    upload_to_bq(baseline_df, "route_baselines")

    # ── 3. CURRENT VS HISTORICAL (latest day per route) ───────────────────────
    current_rows = []
    if not ais.empty and "route" in ais.columns:
        latest_date = ais["fetch_date"].max()
        latest_ais  = ais[ais["fetch_date"] == latest_date]

        for route in ALL_ROUTES:
            sub = latest_ais[latest_ais["route"] == route]
            bl  = baseline_df[baseline_df["route"] == route]
            bl_vessels = float(bl["baseline_avg_vessels"].iloc[0]) if not bl.empty else 0
            bl_speed   = float(bl["baseline_avg_speed"].iloc[0])   if not bl.empty else 0

            cur_vessels = sub["mmsi"].nunique() if not sub.empty else 0
            cur_speed   = float(sub["sog_knots"].mean()) if not sub.empty and "sog_knots" in sub.columns else 0
            cur_tankers = int((sub["vessel_category"] == "Tanker").sum()) if not sub.empty else 0

            # Fuel pressure today
            fuel_pressure = 0
            fuel_signal   = "NORMAL"
            if not fuel.empty and "fuel_pressure_score" in fuel.columns:
                fuel["date"] = pd.to_datetime(fuel["date"], errors="coerce")
                latest_fuel  = fuel.sort_values("date").iloc[-1]
                fuel_pressure = float(latest_fuel["fuel_pressure_score"])
                fuel_signal   = str(latest_fuel.get("fuel_signal", "NORMAL"))

            # News risk for this route
            news_risk = 0
            news_count = 0
            if not news.empty and "relevant_routes" in news.columns:
                route_news = news[news["relevant_routes"].str.contains(route.split(" ")[0], na=False, case=False)]
                if not route_news.empty:
                    news_risk  = float(route_news["risk_score"].max())
                    news_count = len(route_news)

            # Composite impact score
            traffic_gap  = ((cur_vessels - bl_vessels) / max(bl_vessels, 1)) * 100
            speed_gap    = cur_speed - bl_speed
            impact_score = round(
                max(0, -traffic_gap * 0.4) +   # traffic drop → pressure
                max(0, -speed_gap * 5) +         # speed drop → congestion
                fuel_pressure * 0.3 +            # fuel cost pressure
                news_risk * 0.3,                 # news risk signal
                1
            )

            current_rows.append({
                "event_date":              str(latest_date.date() if hasattr(latest_date, "date") else latest_date),
                "route":                   route,
                "current_vessels":         cur_vessels,
                "current_avg_speed":       round(cur_speed, 2),
                "current_tankers":         cur_tankers,
                "baseline_avg_vessels":    round(bl_vessels, 1),
                "baseline_avg_speed":      round(bl_speed, 2),
                "traffic_vs_history_pct":  round(traffic_gap, 1),
                "speed_gap_knots":         round(speed_gap, 2),
                "fuel_pressure_score":     round(fuel_pressure, 2),
                "fuel_signal":             fuel_signal,
                "news_risk_score":         round(news_risk, 1),
                "news_article_count":      news_count,
                "route_impact_score":      impact_score,
                "status":                  (
                    "CRITICAL" if impact_score >= 60 else
                    "ELEVATED" if impact_score >= 30 else
                    "NORMAL"
                ),
            })

    current_df = pd.DataFrame(current_rows)
    save(current_df, "analysis_current_vs_historical.csv")
    upload_to_bq(current_df, "analysis_current_vs_historical")

    # ── 4. DEVIATION ALERTS ───────────────────────────────────────────────────
    if not current_df.empty:
        alerts = current_df[
            (current_df["traffic_vs_history_pct"] < -20) |   # >20% traffic drop
            (current_df["speed_gap_knots"] < -2) |            # >2 knot speed drop
            (current_df["news_risk_score"] >= 70) |           # high news risk
            (current_df["fuel_pressure_score"] > 10)          # fuel spike
        ].copy()
        alerts["deviation_flag"] = True
        alerts["deviation_reason"] = alerts.apply(lambda r:
            "Traffic drop" if r["traffic_vs_history_pct"] < -20 else
            "Speed drop"   if r["speed_gap_knots"] < -2 else
            "News risk"    if r["news_risk_score"] >= 70 else
            "Fuel spike",  axis=1
        )
        save(alerts, "route_deviation_alerts.csv")
        upload_to_bq(alerts, "route_deviation_alerts")
    else:
        save(pd.DataFrame(), "route_deviation_alerts.csv")

    print("=== ROUTE ANALYTICS COMPLETE ===\n")


if __name__ == "__main__":
    main()
