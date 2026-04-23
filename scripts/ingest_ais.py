"""
scripts/ingest_ais.py
Daily AIS vessel tracking — 8 critical straits + 100 major ports.

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

# One global bounding box — receives everything AISStream has worldwide.
# This captures any coverage in the Persian Gulf, Red Sea, Arabian Sea,
# and any other region AISStream receivers exist in.
# AISStream sends up to ~300 messages/second at global scale.
BOUNDING_BOXES = [
    [[-90.0, -180.0], [90.0, 180.0]],   # Entire world
]

# Port/strait name lookup for the port_guess column
# These boxes are used ONLY for labeling vessels by location (guess_location function)
# They are NOT sent to AISStream — only BOUNDING_BOXES above is sent
KNOWN_LOCATIONS = [
    # (label, lat_min, lat_max, lon_min, lon_max)
    ("Strait of Hormuz",   22.0, 30.0,  50.0,  60.0),
    ("Strait of Malacca",  -2.0,  5.0,  98.0, 106.0),
    ("Suez Canal",         27.0, 33.0,  29.0,  37.0),
    ("Bab el-Mandeb",       8.0, 16.0,  40.0,  50.0),
    ("Strait of Gibraltar", 34.0, 38.0,  -8.0,  -1.0),
    ("Bosphorus Strait",   39.0, 43.0,  26.0,  32.0),
    ("Strait of Dover",    49.5, 52.5,  -2.0,   3.5),
    ("Lombok Strait",      -9.5, -5.0, 114.0, 118.5),
    ("Red Sea",            12.0, 28.0,  32.0,  44.0),
    ("Arabian Sea",         5.0, 25.0,  50.0,  75.0),
    ("Shanghai",           29.5, 32.5, 120.0, 123.0),
    ("Singapore",           0.0,  2.5, 102.5, 105.0),
    ("Shenzhen",           21.0, 24.0, 112.5, 115.5),
    ("Busan",              33.5, 36.5, 127.5, 130.5),
    ("Hong Kong",          21.0, 23.5, 113.0, 115.5),
    ("Rotterdam",          50.5, 53.0,   3.0,   6.0),
    ("Dubai",              23.5, 26.5,  53.5,  57.0),
    ("Los Angeles",        32.0, 35.0,-120.0,-117.0),
    ("New York",           39.0, 41.5, -75.5, -73.0),
    ("Tokyo",              34.5, 36.5, 139.0, 141.5),
    ("Mumbai",             18.0, 20.5,  72.0,  74.0),
]

STRAIT_NAMES = {
    "Strait of Hormuz", "Strait of Malacca", "Suez Canal",
    "Bab el-Mandeb", "Strait of Gibraltar", "Bosphorus Strait",
    "Strait of Dover", "Lombok Strait", "Red Sea", "Arabian Sea",
}

# Is this location a strait (vs a port)?
# STRAIT_NAMES now defined in KNOWN_LOCATIONS block above


def guess_location(lat: float, lon: float) -> str:
    """Return the name of the known location this position falls inside."""
    for label, lat_min, lat_max, lon_min, lon_max in KNOWN_LOCATIONS:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return label
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
    client   = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.vessel_movements"

    # Try WRITE_APPEND first (normal daily operation)
    # If schema mismatch (e.g. new columns added), recreate table with WRITE_TRUNCATE
    for attempt, disposition in enumerate([
        bigquery.WriteDisposition.WRITE_APPEND,
        bigquery.WriteDisposition.WRITE_TRUNCATE,
    ]):
        try:
            job_cfg = bigquery.LoadJobConfig(
                write_disposition=disposition,
                autodetect=True,
            )
            job = client.load_table_from_dataframe(
                df_dedup, table_id, job_config=job_cfg
            )
            job.result()
            tbl = client.get_table(table_id)
            mode = "APPEND" if disposition == bigquery.WriteDisposition.WRITE_APPEND else "RECREATED (schema changed)"
            print(f"  ✓ {len(df_dedup):,} rows → {table_id} "
                  f"[{mode}] (table total: {tbl.num_rows:,})")
            break
        except Exception as e:
            if attempt == 0 and ("schema" in str(e).lower() or "field" in str(e).lower()):
                print(f"  Schema mismatch on APPEND — recreating table with new schema...")
            elif attempt == 1:
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
