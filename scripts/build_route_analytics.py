"""
scripts/build_route_analytics.py
Reads from BigQuery (live data) — NOT from static CSV files.
Run: python scripts/build_route_analytics.py
"""
import os
import pathlib
import pandas as pd

BQ_PROJECT = os.getenv("BQ_PROJECT", "msba305-shipping")
BQ_DATASET = os.getenv("BQ_DATASET", "shipping_data")
GCP_KEY    = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "gcp_key.json")

ROOT  = pathlib.Path(__file__).resolve().parents[1]
CLEAN = ROOT / "data" / "clean"

PORT_TO_ROUTE = {
    "Singapore":"Asia-Europe via Suez","Shanghai":"Asia-Europe via Suez",
    "Shenzhen":"Asia-Europe via Suez","Hong Kong":"Asia-Europe via Suez",
    "Busan":"Asia-Europe via Suez","Rotterdam":"Asia-Europe via Suez",
    "Antwerp":"Asia-Europe via Suez","Hamburg":"North Sea / Dover",
    "Strait of Dover":"North Sea / Dover","Strait of Gibraltar":"Asia-Europe via Suez",
    "Suez Canal":"Asia-Europe via Suez","Bab el-Mandeb":"Asia-Europe via Suez",
    "Strait of Malacca":"Asia-Europe via Suez","Strait of Hormuz":"Strait of Hormuz",
    "Dubai":"Strait of Hormuz","Bosphorus Strait":"Black Sea",
    "Los Angeles":"Trans-Pacific","New York":"Trans-Pacific","Tokyo":"Trans-Pacific",
}
ALL_ROUTES = ["Asia-Europe via Suez","Strait of Hormuz","Black Sea","Trans-Pacific","North Sea / Dover"]

def get_client():
    from google.cloud import bigquery
    from google.oauth2 import service_account
    kp = pathlib.Path(GCP_KEY)
    if kp.exists():
        creds = service_account.Credentials.from_service_account_file(str(kp))
        return bigquery.Client(project=BQ_PROJECT, credentials=creds)
    return bigquery.Client(project=BQ_PROJECT)

def read_bq(table, client):
    try:
        df = client.query(f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{table}`").to_dataframe()
        df.columns = [c.lower() for c in df.columns]
        print(f"  ✓ {table}: {len(df):,} rows")
        return df
    except Exception as e:
        print(f"  ⚠ {table}: {e}")
        return pd.DataFrame()

def upload(df, name, client):
    if df.empty:
        print(f"  ⚠ Skip {name} — empty"); return
    from google.cloud import bigquery
    jc = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, autodetect=True)
    client.load_table_from_dataframe(df, f"{BQ_PROJECT}.{BQ_DATASET}.{name}", job_config=jc).result()
    df.to_csv(CLEAN / f"{name}.csv", index=False)
    print(f"  ✓ {name}: {len(df):,} rows → BigQuery + CSV")

def map_route(pg):
    if not isinstance(pg, str): return "Unmapped / Other"
    for k, v in PORT_TO_ROUTE.items():
        if k.lower() in pg.lower(): return v
    return "Unmapped / Other"

def main():
    print("\n=== BUILD ROUTE ANALYTICS ===")
    try:
        client = get_client()
    except Exception as e:
        print(f"✗ BQ connection failed: {e}"); return

    ais  = read_bq("vessel_movements",  client)
    fuel = read_bq("fuel_prices_daily", client)
    news = read_bq("shipping_news",     client)

    if ais.empty:
        print("⚠ No AIS data — cannot build analytics"); return

    if "port_guess" in ais.columns:
        ais["route"] = ais["port_guess"].apply(map_route)
    if "fetch_date" in ais.columns:
        ais["fetch_date"] = pd.to_datetime(ais["fetch_date"], errors="coerce")
    if "sog_knots" in ais.columns:
        ais["sog_knots"] = pd.to_numeric(ais["sog_knots"], errors="coerce")

    # Baselines
    baselines = []
    if "fetch_date" in ais.columns and "route" in ais.columns:
        hist = (ais.groupby(["route", ais["fetch_date"].dt.date])
                .agg(daily_vessels=("mmsi","nunique"),
                     avg_speed=("sog_knots","mean"),
                     tanker_count=("vessel_category", lambda x: (x=="Tanker").sum()),
                     cargo_count=("vessel_category", lambda x: (x=="Cargo").sum()))
                .reset_index())
        for r in ALL_ROUTES:
            sub = hist[hist["route"]==r]
            baselines.append({"route":r,
                "baseline_avg_vessels":round(sub["daily_vessels"].mean(),1) if not sub.empty else 0,
                "baseline_avg_speed":round(sub["avg_speed"].mean(),2) if not sub.empty else 0,
                "days_of_history":len(sub) if not sub.empty else 0})
    bl_df = pd.DataFrame(baselines)
    upload(bl_df, "route_baselines", client)

    # Current vs historical
    rows = []
    if "fetch_date" in ais.columns and "route" in ais.columns:
        latest = ais["fetch_date"].dt.date.max()
        la = ais[ais["fetch_date"].dt.date == latest]

        fuel_pressure, fuel_signal = 0.0, "NORMAL"
        if not fuel.empty and "fuel_pressure_score" in fuel.columns:
            fuel["date"] = pd.to_datetime(fuel["date"], errors="coerce")
            lf = fuel.sort_values("date").iloc[-1]
            fuel_pressure = float(lf.get("fuel_pressure_score",0) or 0)
            fuel_signal   = str(lf.get("fuel_signal","NORMAL"))

        for r in ALL_ROUTES:
            sub = la[la["route"]==r]
            bl  = bl_df[bl_df["route"]==r]
            bl_v = float(bl["baseline_avg_vessels"].iloc[0]) if not bl.empty else 0
            bl_s = float(bl["baseline_avg_speed"].iloc[0])   if not bl.empty else 0
            cv = int(sub["mmsi"].nunique()) if not sub.empty else 0
            cs = float(sub["sog_knots"].dropna().mean()) if not sub.empty and sub["sog_knots"].notna().any() else 0
            ct = int((sub["vessel_category"]=="Tanker").sum()) if not sub.empty and "vessel_category" in sub.columns else 0

            nr, nc = 0.0, 0
            if not news.empty and "relevant_routes" in news.columns and "risk_score" in news.columns:
                rn = news[news["relevant_routes"].str.contains(r.split(" ")[0], na=False, case=False)]
                if not rn.empty:
                    nr = float(rn["risk_score"].max())
                    nc = len(rn)

            tg = ((cv - bl_v) / max(bl_v, 1)) * 100
            sg = cs - bl_s
            impact = round(max(0,-tg*0.4) + max(0,-sg*5) + fuel_pressure*0.3 + nr*0.3, 1)

            rows.append({"event_date":str(latest),"route":r,
                "current_vessels":cv,"current_avg_speed":round(cs,2),"current_tankers":ct,
                "baseline_avg_vessels":round(bl_v,1),"baseline_avg_speed":round(bl_s,2),
                "traffic_vs_history_pct":round(tg,1),"speed_gap_knots":round(sg,2),
                "fuel_pressure_score":round(fuel_pressure,2),"fuel_signal":fuel_signal,
                "news_risk_score":round(nr,1),"news_article_count":nc,
                "route_impact_score":impact,
                "status":"CRITICAL" if impact>=60 else "ELEVATED" if impact>=30 else "NORMAL"})

    cur_df = pd.DataFrame(rows)
    upload(cur_df, "analysis_current_vs_historical", client)

    if not cur_df.empty:
        al = cur_df[(cur_df["traffic_vs_history_pct"]<-20)|(cur_df["speed_gap_knots"]<-2)|
                    (cur_df["news_risk_score"]>=70)|(cur_df["fuel_pressure_score"]>10)].copy()
        if not al.empty:
            al["deviation_flag"] = True
            al["deviation_reason"] = al.apply(lambda r2:
                "Traffic drop" if r2["traffic_vs_history_pct"]<-20 else
                "Speed drop"   if r2["speed_gap_knots"]<-2 else
                "News risk"    if r2["news_risk_score"]>=70 else "Fuel spike", axis=1)
            upload(al, "route_deviation_alerts", client)
        else:
            print("  ✓ No deviation alerts today")

    print("=== ROUTE ANALYTICS COMPLETE ===\n")

if __name__ == "__main__":
    main()
