"""
scripts/ingest_ais.py
Daily AIS vessel tracking — 8 critical straits + 20 major ports.

Connects to AISStream WebSocket for COLLECT_SECONDS (default 240 = 4 minutes),
captures vessel positions at defined bounding boxes, then disconnects cleanly.

GitHub Actions secret required: AISSTREAM_API_KEY
Get your free key at: https://aisstream.io

What it collects:
  - PositionReport  : lat, lon, speed, heading, nav status
  - ShipStaticData  : ship name, type, destination, ETA

Uploads to BigQuery:
  - vessel_movements (WRITE_APPEND) — grows daily, full history
Saves locally:
  - data/clean/ais_latest.csv — overwritten each run, used by update_combined.py

Run order in GitHub Actions:
  1. ingest_weather.py
  2. ingest_ais.py      ← this script
  3. update_combined.py
"""

import os
import asyncio
import json
import time
import pandas as pd
import numpy as np
import websockets
from datetime import datetime, timezone
from google.cloud import bigquery

BQ_PROJECT       = os.environ["BQ_PROJECT"]
BQ_DATASET       = os.environ["BQ_DATASET"]
AISSTREAM_API_KEY = os.environ["AISSTREAM_API_KEY"]

CLEAN_DIR   = "data/clean"
LATEST_PATH = f"{CLEAN_DIR}/ais_latest.csv"
RAW_DIR     = "data/raw"

os.makedirs(CLEAN_DIR, exist_ok=True)
os.makedirs(RAW_DIR,   exist_ok=True)

# How long to collect (seconds). 240 = 4 minutes.
COLLECT_SECONDS = int(os.environ.get("AIS_COLLECT_SECONDS", "240"))

# ── BOUNDING BOXES ─────────────────────────────────────────────────────────────
# Each box: [[lat_min, lon_min], [lat_max, lon_max]]
# Covers all 8 straits + top 10 ports
# Kept at 18 boxes — AISStream free tier can close connection with too many boxes

BOUNDING_BOXES = [
    # ── 8 Critical Straits ──────────────────────────────────────────────────
    [[25.0,  55.0], [28.0,  59.0]],   # Strait of Hormuz
    [[-1.0,  99.0], [ 4.0, 105.0]],   # Strait of Malacca
    [[29.0,  31.0], [32.0,  34.0]],   # Suez Canal
    [[11.0,  42.0], [14.0,  45.0]],   # Bab el-Mandeb
    [[35.0,  -6.5], [37.0,  -4.0]],   # Strait of Gibraltar
    [[40.5,  27.5], [42.0,  30.0]],   # Bosphorus Strait
    [[50.5,  -0.5], [52.0,   2.5]],   # Strait of Dover
    [[-9.5, 115.0], [-7.0, 117.5]],   # Lombok Strait

    # ── Top 10 Ports by traffic volume (±0.8° bounding box) ─────────────────
    # Kept at 10 to stay within AISStream free tier limits (18 boxes total)
    [[30.4, 120.7], [32.1, 122.2]],   # Shanghai    (rank 1)
    [[ 0.5, 103.1], [ 2.1, 104.5]],   # Singapore   (rank 2)
    [[29.1, 120.7], [30.6, 122.3]],   # Ningbo      (rank 3)
    [[21.7, 113.3], [23.3, 114.9]],   # Shenzhen    (rank 4)
    [[34.4, 128.3], [35.9, 129.9]],   # Busan       (rank 6)
    [[21.5, 113.4], [23.0, 114.9]],   # Hong Kong   (rank 8)
    [[51.1,   3.7], [52.7,   5.3]],   # Rotterdam   (rank 9)
    [[24.2,  54.2], [25.7,  55.8]],   # Dubai       (rank 10)
    [[32.9,-119.1], [34.5,-117.6]],   # Los Angeles (rank 14)
    [[39.9,  -75.0], [41.5, -73.4]],  # New York    (rank 20)
]

# Port/strait name lookup for the port_guess column
LOCATION_LABELS = [
    "Strait of Hormuz", "Strait of Malacca", "Suez Canal",
    "Bab el-Mandeb", "Strait of Gibraltar", "Bosphorus Strait",
    "Strait of Dover", "Lombok Strait",
    "Shanghai", "Singapore", "Ningbo-Zhoushan", "Shenzhen",
    "Busan", "Hong Kong", "Rotterdam", "Dubai",
    "Los Angeles", "New York",
]

# Is this location a strait (vs a port)?
STRAIT_NAMES = {
    "Strait of Hormuz", "Strait of Malacca", "Suez Canal",
    "Bab el-Mandeb", "Strait of Gibraltar", "Bosphorus Strait",
    "Strait of Dover", "Lombok Strait",
}


def guess_location(lat: float, lon: float) -> str:
    """Return the name of the bounding box this position falls inside."""
    for i, box in enumerate(BOUNDING_BOXES):
        lat_min = min(box[0][0], box[1][0])
        lat_max = max(box[0][0], box[1][0])
        lon_min = min(box[0][1], box[1][1])
        lon_max = max(box[0][1], box[1][1])
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return LOCATION_LABELS[i]
    return "Unknown"


# Vessel type code → human-readable category
VESSEL_CATEGORIES = {
    range(70, 80): "Cargo",
    range(80, 90): "Tanker",
    range(60, 70): "Passenger",
    range(30, 36): "Fishing",
    range(50, 60): "Tug / Support",
}

NAV_STATUS = {
    0: "Under way using engine",
    1: "At anchor",
    2: "Not under command",
    3: "Restricted manoeuvrability",
    4: "Constrained by draught",
    5: "Moored",
    6: "Aground",
    7: "Engaged in fishing",
    8: "Under way sailing",
    15: "Not reported",
}


def vessel_category(type_code) -> str:
    if type_code is None or np.isnan(float(type_code if type_code else "nan")):
        return "Unknown"
    tc = int(type_code)
    for r, cat in VESSEL_CATEGORIES.items():
        if tc in r:
            return cat
    return "Other"


def speed_cat(sog) -> str:
    if sog is None or np.isnan(float(sog if sog is not None else "nan")):
        return "Unknown"
    s = float(sog)
    if s < 0.5:  return "Stationary"
    if s < 3.0:  return "Slow / manoeuvring"
    if s < 10.0: return "Transit"
    return "Cruising"


# ── AIS COLLECTION ─────────────────────────────────────────────────────────────
records = []
vessel_static: dict = {}   # mmsi → static data (name, type, destination…)


async def collect():
    """Connect to AISStream, subscribe, collect for COLLECT_SECONDS, then stop."""
    url = "wss://stream.aisstream.io/v0/stream"

    subscribe = {
        "APIKey":         AISSTREAM_API_KEY,
        "BoundingBoxes":  BOUNDING_BOXES,
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }

    start_time  = time.time()
    msg_count   = 0
    error_count = 0

    print(f"  Connecting to AISStream...")
    print(f"  Collecting for {COLLECT_SECONDS}s across {len(BOUNDING_BOXES)} bounding boxes...")

    try:
        async with websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            await ws.send(json.dumps(subscribe))
            print("  Subscribed — receiving messages...")

            async for raw_msg in ws:
                elapsed = time.time() - start_time
                if elapsed >= COLLECT_SECONDS:
                    print(f"  Collection window complete ({elapsed:.0f}s) — disconnecting")
                    break

                try:
                    msg   = json.loads(raw_msg)
                    mtype = msg.get("MessageType", "")
                    meta  = msg.get("MetaData", {})
                    mmsi  = meta.get("MMSI") or meta.get("UserID")

                    now_utc = datetime.now(timezone.utc)

                    if mtype == "ShipStaticData":
                        body = msg.get("Message", {}).get("ShipStaticData", {})
                        vessel_static[mmsi] = {
                            "ship_name":    meta.get("ShipName", body.get("Name", "")),
                            "imo":          body.get("ImoNumber"),
                            "call_sign":    body.get("CallSign", ""),
                            "destination":  body.get("Destination", ""),
                            "vessel_type":  body.get("Type"),
                            "eta":          str(body.get("Eta", "")),
                        }

                    elif mtype == "PositionReport":
                        body = msg.get("Message", {}).get("PositionReport", {})
                        lat  = meta.get("latitude",  body.get("Latitude"))
                        lon  = meta.get("longitude", body.get("Longitude"))
                        sog  = body.get("Sog")
                        nav  = body.get("NavigationalStatus", 15)

                        if lat is None or lon is None:
                            continue

                        static = vessel_static.get(mmsi, {})
                        vtype  = static.get("vessel_type")
                        sog_f  = float(sog) if sog is not None else None
                        loc    = guess_location(float(lat), float(lon))

                        records.append({
                            "event_time_utc":      now_utc.isoformat(),
                            "capture_utc":         now_utc.isoformat(),
                            "fetch_date":          now_utc.strftime("%Y-%m-%d"),
                            "message_type":        mtype,
                            "mmsi":                mmsi,
                            "ship_name":           static.get("ship_name") or meta.get("ShipName", ""),
                            "imo":                 static.get("imo"),
                            "call_sign":           static.get("call_sign", ""),
                            "latitude":            float(lat),
                            "longitude":           float(lon),
                            "sog_knots":           sog_f,
                            "speed_category":      speed_cat(sog_f),
                            "is_moving":           (sog_f or 0) >= 0.5,
                            "cog_degrees":         body.get("Cog"),
                            "true_heading":        body.get("TrueHeading"),
                            "navigational_status": nav,
                            "nav_status_name":     NAV_STATUS.get(nav, "Unknown"),
                            "destination":         static.get("destination", ""),
                            "eta":                 static.get("eta", ""),
                            "vessel_type":         vtype,
                            "vessel_type_name":    str(vtype) if vtype else "Unknown",
                            "vessel_category":     vessel_category(vtype),
                            "port_guess":          loc,
                            "is_strait":           loc in STRAIT_NAMES,
                            "source":              "aisstream",
                            "data_type":           "vessel_movement",
                            "granularity":         "event",
                            "year":                now_utc.year,
                            "month":               now_utc.month,
                            "day":                 now_utc.day,
                            "hour":                now_utc.hour,
                        })
                        msg_count += 1

                        if msg_count % 500 == 0:
                            elapsed = time.time() - start_time
                            print(f"  {msg_count:,} position messages | "
                                  f"{len(records):,} records | {elapsed:.0f}s elapsed")

                except Exception as e:
                    error_count += 1
                    if error_count < 5:
                        print(f"  Parse error: {e}")

    except websockets.exceptions.ConnectionClosedError as e:
        # AISStream (BETA) often closes without a proper close frame — treat as normal end
        print(f"  AISStream closed connection ({e}) — treating as normal end of stream")
    except websockets.exceptions.ConnectionClosedOK:
        print("  AISStream closed connection cleanly")
    except Exception as e:
        print(f"  WebSocket error: {e}")
        if not records:
            raise

    print(f"  Collection done: {len(records):,} records, {msg_count:,} position msgs")


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print("=" * 60)
    print(f"ingest_ais.py  —  {now_str}")
    print(f"Collect window: {COLLECT_SECONDS}s")
    print("=" * 60)

    # ── 1. Collect ────────────────────────────────────────────────────────────
    print("\n[1/3] Collecting AIS data...")
    asyncio.run(collect())

    if not records:
        print("  ✗ No records collected — AISStream may be unavailable or connection was too brief")
        print("    Pipeline continues — existing vessel_movements data unchanged")
        print("    Check your AISSTREAM_API_KEY secret and AISStream status at aisstream.io")
        import sys
        sys.exit(0)   # exit 0 so GitHub Actions does not fail the workflow

    df = pd.DataFrame(records)

    # De-duplicate: one position record per vessel per day
    df = df.sort_values("event_time_utc", ascending=False)
    df_dedup = df.drop_duplicates(subset=["mmsi", "fetch_date"])
    print(f"  {len(df):,} raw records → {len(df_dedup):,} after de-dup "
          f"(one per vessel per day)")

    # Location breakdown
    loc_counts = df["port_guess"].value_counts()
    straits_seen = [l for l in loc_counts.index if l in STRAIT_NAMES]
    ports_seen   = [l for l in loc_counts.index if l not in STRAIT_NAMES and l != "Unknown"]
    print(f"  Straits with vessels: {len(straits_seen)} — {straits_seen}")
    print(f"  Ports with vessels:   {len(ports_seen)}")

    # ── 2. Save ───────────────────────────────────────────────────────────────
    print("\n[2/3] Saving...")
    # Latest snapshot (used by update_combined.py)
    df_dedup.to_csv(LATEST_PATH, index=False)
    print(f"  ✎ ais_latest.csv → {len(df_dedup):,} rows")

    # Raw backup with date in filename
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    raw_path = f"{RAW_DIR}/ais_raw_{date_str}.csv"
    df.to_csv(raw_path, index=False)

    # ── 3. Upload to BigQuery ─────────────────────────────────────────────────
    print("\n[3/3] Uploading to BigQuery (vessel_movements, WRITE_APPEND)...")
    try:
        client   = bigquery.Client(project=BQ_PROJECT)
        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.vessel_movements"
        job_cfg  = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,
        )
        job = client.load_table_from_dataframe(df_dedup, table_id, job_config=job_cfg)
        job.result()
        tbl = client.get_table(table_id)
        print(f"  ✓ {len(df_dedup):,} rows → {table_id} "
              f"(table total: {tbl.num_rows:,})")
    except Exception as e:
        print(f"  ✗ BigQuery upload failed: {e}")
        print("    ais_latest.csv saved locally — pipeline continues")
        raise

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"Records collected:   {len(df):,}")
    print(f"Unique vessels:      {df['mmsi'].nunique():,}")
    print(f"Moving vessels:      {df['is_moving'].sum():,}")
    print(f"Vessels at straits:  {df[df['is_strait']==True]['mmsi'].nunique():,}")
    tankers = (df["vessel_category"] == "Tanker").sum()
    cargo   = (df["vessel_category"] == "Cargo").sum()
    print(f"Tankers: {tankers:,}  |  Cargo: {cargo:,}")
    print("Done.")
