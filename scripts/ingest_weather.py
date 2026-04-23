"""
scripts/ingest_weather.py
Fetch weather at:
  - Top 20 global ports (by container throughput)
  - 8 critical maritime straits / chokepoints

Scheduled: GitHub Actions daily 20:00 UTC weekdays.
Uploads to BigQuery tables:
  - port_weather      (WRITE_APPEND — historical accumulation)
  - strait_conditions (WRITE_APPEND — historical accumulation)

SCHEMA FIX: now matches the notebook's port_weather_clean.csv schema exactly.
ADDED: strait monitoring for Hormuz, Malacca, Suez, Gibraltar,
       Bosphorus, Bab el-Mandeb, Dover, Lombok.
"""

import os, time, json, requests, math
import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery

WEATHER_API_KEY = os.environ["WEATHER_API_KEY"]
BQ_PROJECT      = os.environ["BQ_PROJECT"]
BQ_DATASET      = os.environ["BQ_DATASET"]

os.makedirs("data/raw",   exist_ok=True)
os.makedirs("data/clean", exist_ok=True)

# ── Top 100 ports by container throughput ─────────────────────────────────────
TOP_PORTS = [
    {"name": "Shanghai",          "country": "CN", "lat": 31.2304,  "lon": 121.4737, "rank": 1},
    {"name": "Singapore",         "country": "SG", "lat":  1.2966,  "lon": 103.8006, "rank": 2},
    {"name": "Ningbo-Zhoushan",   "country": "CN", "lat": 29.8683,  "lon": 121.5440, "rank": 3},
    {"name": "Shenzhen",          "country": "CN", "lat": 22.5431,  "lon": 114.0579, "rank": 4},
    {"name": "Guangzhou",         "country": "CN", "lat": 23.1291,  "lon": 113.2644, "rank": 5},
    {"name": "Busan",             "country": "KR", "lat": 35.1796,  "lon": 129.0756, "rank": 6},
    {"name": "Tianjin",           "country": "CN", "lat": 39.3434,  "lon": 117.3616, "rank": 7},
    {"name": "Hong Kong",         "country": "HK", "lat": 22.3193,  "lon": 114.1694, "rank": 8},
    {"name": "Rotterdam",         "country": "NL", "lat": 51.9225,  "lon":   4.4792, "rank": 9},
    {"name": "Dubai",             "country": "AE", "lat": 24.9965,  "lon":  55.0272, "rank": 10},
    {"name": "Port Klang",        "country": "MY", "lat":  3.0000,  "lon": 101.4000, "rank": 11},
    {"name": "Antwerp",           "country": "BE", "lat": 51.2194,  "lon":   4.4025, "rank": 12},
    {"name": "Xiamen",            "country": "CN", "lat": 24.4798,  "lon": 118.0894, "rank": 13},
    {"name": "Los Angeles",       "country": "US", "lat": 33.7290,  "lon": -118.262, "rank": 14},
    {"name": "Hamburg",           "country": "DE", "lat": 53.5753,  "lon":  10.0153, "rank": 15},
    {"name": "Long Beach",        "country": "US", "lat": 33.7548,  "lon": -118.2164,"rank": 16},
    {"name": "Tanjung Pelepas",   "country": "MY", "lat":  1.3634,  "lon": 103.5521, "rank": 17},
    {"name": "Kaohsiung",         "country": "TW", "lat": 22.6273,  "lon": 120.3014, "rank": 18},
    {"name": "Dalian",            "country": "CN", "lat": 38.9140,  "lon": 121.6147, "rank": 19},
    {"name": "New York",          "country": "US", "lat": 40.6943,  "lon":  -74.1239,"rank": 20},
    {"name": "Tanjung Priok",     "country": "ID", "lat": -6.1000,  "lon": 106.8800, "rank": 21},
    {"name": "Colombo",           "country": "LK", "lat":  6.9271,  "lon":  79.8612, "rank": 22},
    {"name": "Valencia",          "country": "ES", "lat": 39.4561,  "lon":  -0.3230, "rank": 23},
    {"name": "Laem Chabang",      "country": "TH", "lat": 13.0800,  "lon": 100.8800, "rank": 24},
    {"name": "Algeciras",         "country": "ES", "lat": 36.1408,  "lon":  -5.4531, "rank": 25},
    {"name": "Ho Chi Minh City",  "country": "VN", "lat": 10.7769,  "lon": 106.7009, "rank": 26},
    {"name": "Bremen/Bremerhaven","country": "DE", "lat": 53.5396,  "lon":   8.5809, "rank": 27},
    {"name": "Jawaharlal Nehru",  "country": "IN", "lat": 18.9500,  "lon":  72.9500, "rank": 28},
    {"name": "Qingdao",           "country": "CN", "lat": 36.0671,  "lon": 120.3826, "rank": 29},
    {"name": "Felixstowe",        "country": "GB", "lat": 51.9500,  "lon":   1.3500, "rank": 30},
    {"name": "Tanjung Pelepas 2", "country": "MY", "lat":  1.3500,  "lon": 103.5500, "rank": 31},
    {"name": "Manila",            "country": "PH", "lat": 14.5764,  "lon": 120.9653, "rank": 32},
    {"name": "Lianyungang",       "country": "CN", "lat": 34.7500,  "lon": 119.4400, "rank": 33},
    {"name": "Tokyo",             "country": "JP", "lat": 35.6262,  "lon": 139.7736, "rank": 34},
    {"name": "Yokohama",          "country": "JP", "lat": 35.4437,  "lon": 139.6380, "rank": 35},
    {"name": "Osaka",             "country": "JP", "lat": 34.6540,  "lon": 135.4300, "rank": 36},
    {"name": "Nagoya",            "country": "JP", "lat": 35.0500,  "lon": 136.8833, "rank": 37},
    {"name": "Kobe",              "country": "JP", "lat": 34.6901,  "lon": 135.1956, "rank": 38},
    {"name": "Suzhou",            "country": "CN", "lat": 31.3100,  "lon": 120.6200, "rank": 39},
    {"name": "Wuhan",             "country": "CN", "lat": 30.5928,  "lon": 114.3055, "rank": 40},
    {"name": "Jebel Ali",         "country": "AE", "lat": 24.9994,  "lon":  55.0540, "rank": 41},
    {"name": "Salalah",           "country": "OM", "lat": 16.9400,  "lon":  54.0000, "rank": 42},
    {"name": "Piraeus",           "country": "GR", "lat": 37.9417,  "lon":  23.6464, "rank": 43},
    {"name": "Genoa",             "country": "IT", "lat": 44.4056,  "lon":   8.9463, "rank": 44},
    {"name": "Barcelona",         "country": "ES", "lat": 41.3700,  "lon":   2.1600, "rank": 45},
    {"name": "Marseille",         "country": "FR", "lat": 43.2965,  "lon":   5.3698, "rank": 46},
    {"name": "Le Havre",          "country": "FR", "lat": 49.4938,  "lon":   0.1077, "rank": 47},
    {"name": "Gioia Tauro",       "country": "IT", "lat": 38.4300,  "lon":  15.8900, "rank": 48},
    {"name": "Ambarli",           "country": "TR", "lat": 40.9500,  "lon":  28.6900, "rank": 49},
    {"name": "Istanbul",          "country": "TR", "lat": 41.0082,  "lon":  28.9784, "rank": 50},
    {"name": "Mersin",            "country": "TR", "lat": 36.8000,  "lon":  34.6333, "rank": 51},
    {"name": "Haifa",             "country": "IL", "lat": 32.8191,  "lon":  34.9983, "rank": 52},
    {"name": "Alexandria",        "country": "EG", "lat": 31.2001,  "lon":  29.9187, "rank": 53},
    {"name": "Damietta",          "country": "EG", "lat": 31.4167,  "lon":  31.8167, "rank": 54},
    {"name": "Casablanca",        "country": "MA", "lat": 33.5731,  "lon":  -7.5898, "rank": 55},
    {"name": "Mombasa",           "country": "KE", "lat": -4.0500,  "lon":  39.6700, "rank": 56},
    {"name": "Durban",            "country": "ZA", "lat": -29.8587, "lon":  31.0218, "rank": 57},
    {"name": "Lagos",             "country": "NG", "lat":  6.4500,  "lon":   3.4000, "rank": 58},
    {"name": "Abidjan",           "country": "CI", "lat":  5.3167,  "lon":  -4.0333, "rank": 59},
    {"name": "Dakar",             "country": "SN", "lat": 14.6928,  "lon": -17.4467, "rank": 60},
    {"name": "Santos",            "country": "BR", "lat": -23.9608, "lon":  -46.3336,"rank": 61},
    {"name": "Buenos Aires",      "country": "AR", "lat": -34.6037, "lon":  -58.3816,"rank": 62},
    {"name": "Cartagena",         "country": "CO", "lat": 10.3910,  "lon":  -75.4794,"rank": 63},
    {"name": "Colon",             "country": "PA", "lat":  9.3500,  "lon":  -79.9000,"rank": 64},
    {"name": "Callao",            "country": "PE", "lat": -12.0500, "lon":  -77.1500,"rank": 65},
    {"name": "Vancouver",         "country": "CA", "lat": 49.2827,  "lon": -123.1207,"rank": 66},
    {"name": "Montreal",          "country": "CA", "lat": 45.5017,  "lon":  -73.5673,"rank": 67},
    {"name": "Baltimore",         "country": "US", "lat": 39.2904,  "lon":  -76.6122,"rank": 68},
    {"name": "Savannah",          "country": "US", "lat": 32.0835,  "lon":  -81.0998,"rank": 69},
    {"name": "Norfolk",           "country": "US", "lat": 36.8508,  "lon":  -76.2859,"rank": 70},
    {"name": "Seattle",           "country": "US", "lat": 47.6062,  "lon": -122.3321,"rank": 71},
    {"name": "Tacoma",            "country": "US", "lat": 47.2529,  "lon": -122.4443,"rank": 72},
    {"name": "Houston",           "country": "US", "lat": 29.7604,  "lon":  -95.3698,"rank": 73},
    {"name": "Miami",             "country": "US", "lat": 25.7617,  "lon":  -80.1918,"rank": 74},
    {"name": "Incheon",           "country": "KR", "lat": 37.4563,  "lon": 126.7052, "rank": 75},
    {"name": "Gwangyang",         "country": "KR", "lat": 34.9000,  "lon": 127.7000, "rank": 76},
    {"name": "Chittagong",        "country": "BD", "lat": 22.3569,  "lon":  91.7832, "rank": 77},
    {"name": "Karachi",           "country": "PK", "lat": 24.8607,  "lon":  67.0011, "rank": 78},
    {"name": "Mumbai",            "country": "IN", "lat": 18.9667,  "lon":  72.8333, "rank": 79},
    {"name": "Chennai",           "country": "IN", "lat": 13.0827,  "lon":  80.2707, "rank": 80},
    {"name": "Mundra",            "country": "IN", "lat": 22.8390,  "lon":  69.7220, "rank": 81},
    {"name": "Vizag",             "country": "IN", "lat": 17.6868,  "lon":  83.2185, "rank": 82},
    {"name": "Colombo 2",         "country": "LK", "lat":  6.9500,  "lon":  79.8700, "rank": 83},
    {"name": "Klang 2",           "country": "MY", "lat":  2.9900,  "lon": 101.3900, "rank": 84},
    {"name": "Penang",            "country": "MY", "lat":  5.4164,  "lon": 100.3327, "rank": 85},
    {"name": "Batam",             "country": "ID", "lat":  1.0456,  "lon": 104.0305, "rank": 86},
    {"name": "Surabaya",          "country": "ID", "lat": -7.2575,  "lon": 112.7521, "rank": 87},
    {"name": "Bangkok",           "country": "TH", "lat": 13.7563,  "lon": 100.5018, "rank": 88},
    {"name": "Haiphong",          "country": "VN", "lat": 20.8449,  "lon": 106.6881, "rank": 89},
    {"name": "Da Nang",           "country": "VN", "lat": 16.0544,  "lon": 108.2022, "rank": 90},
    {"name": "Guangzhou Nansha",  "country": "CN", "lat": 22.7600,  "lon": 113.5800, "rank": 91},
    {"name": "Yingkou",           "country": "CN", "lat": 40.6600,  "lon": 122.2300, "rank": 92},
    {"name": "Rizhao",            "country": "CN", "lat": 35.4200,  "lon": 119.5300, "rank": 93},
    {"name": "Nanjing",           "country": "CN", "lat": 32.0603,  "lon": 118.7969, "rank": 94},
    {"name": "Chongqing",         "country": "CN", "lat": 29.5630,  "lon": 106.5516, "rank": 95},
    {"name": "Taichung",          "country": "TW", "lat": 24.1477,  "lon": 120.6736, "rank": 96},
    {"name": "Keelung",           "country": "TW", "lat": 25.1276,  "lon": 121.7392, "rank": 97},
    {"name": "Tauranga",          "country": "NZ", "lat": -37.6878, "lon": 176.1651, "rank": 98},
    {"name": "Melbourne",         "country": "AU", "lat": -37.8136, "lon": 144.9631, "rank": 99},
    {"name": "Sydney",            "country": "AU", "lat": -33.8688, "lon": 151.2093, "rank": 100},
]

# ── Critical maritime straits / chokepoints ───────────────────────────────────
# trade_pct = estimated % of world seaborne trade passing through
# oil_pct   = estimated % of world seaborne oil passing through
# key_routes = main trade routes using this strait
STRAITS = [
    {
        "name":        "Strait of Hormuz",
        "lat":          26.5679,
        "lon":          56.2580,
        "region":       "Middle East",
        "connects":     "Persian Gulf → Arabian Sea",
        "trade_pct":    20,
        "oil_pct":      20,
        "key_routes":   "Persian Gulf oil → Asia/Europe",
        "geopolitical_risk": "High",  # Iran tensions
        "risk_notes":   "~20% global oil trade; Iran-Saudi tensions; US-Iran friction",
    },
    {
        "name":        "Strait of Malacca",
        "lat":          1.2500,
        "lon":         103.8200,
        "region":       "Southeast Asia",
        "connects":     "Indian Ocean → South China Sea",
        "trade_pct":    25,
        "oil_pct":      15,
        "key_routes":   "Middle East/Europe → China/Japan/Korea",
        "geopolitical_risk": "Medium",
        "risk_notes":   "Busiest strait globally; piracy risk; Singapore alternative",
    },
    {
        "name":        "Suez Canal",
        "lat":          30.5234,
        "lon":          32.3511,
        "region":       "Middle East / Africa",
        "connects":     "Red Sea → Mediterranean",
        "trade_pct":    12,
        "oil_pct":       8,
        "key_routes":   "Asia/Middle East → Europe",
        "geopolitical_risk": "High",  # Houthi attacks since 2023
        "risk_notes":   "Houthi attacks forcing Cape of Good Hope rerouting since late 2023",
    },
    {
        "name":        "Bab el-Mandeb",
        "lat":          12.5842,
        "lon":          43.4298,
        "region":       "Red Sea / Yemen",
        "connects":     "Red Sea → Gulf of Aden",
        "trade_pct":    10,
        "oil_pct":       6,
        "key_routes":   "Suez Canal approach from south",
        "geopolitical_risk": "Very High",  # Yemen / Houthi attacks active
        "risk_notes":   "Houthi missile/drone attacks on commercial vessels; major rerouting",
    },
    {
        "name":        "Strait of Gibraltar",
        "lat":          35.9727,
        "lon":          -5.5715,
        "region":       "Europe / Africa",
        "connects":     "Atlantic Ocean → Mediterranean",
        "trade_pct":    10,
        "oil_pct":       5,
        "key_routes":   "Atlantic ↔ Mediterranean / Northern Europe ↔ Asia via Suez",
        "geopolitical_risk": "Low",
        "risk_notes":   "Weather risk in winter; high traffic density",
    },
    {
        "name":        "Bosphorus Strait",
        "lat":          41.1237,
        "lon":          29.0532,
        "region":       "Turkey / Black Sea",
        "connects":     "Black Sea → Mediterranean",
        "trade_pct":     4,
        "oil_pct":       3,
        "key_routes":   "Russia/Ukraine grain and oil → Europe",
        "geopolitical_risk": "High",  # Russia-Ukraine war impact
        "risk_notes":   "Russia-Ukraine war; Black Sea grain exports; Turkish transit control",
    },
    {
        "name":        "Strait of Dover",
        "lat":          51.0960,
        "lon":           1.4220,
        "region":       "Northern Europe",
        "connects":     "English Channel → North Sea",
        "trade_pct":     8,
        "oil_pct":       2,
        "key_routes":   "UK/North Europe ↔ Atlantic/Mediterranean",
        "geopolitical_risk": "Low",
        "risk_notes":   "Busiest shipping lane by vessel count; fog and winter weather risk",
    },
    {
        "name":        "Lombok Strait",
        "lat":          -8.7717,
        "lon":         115.7542,
        "region":       "Southeast Asia",
        "connects":     "Indian Ocean → Java Sea",
        "trade_pct":     3,
        "oil_pct":       2,
        "key_routes":   "Malacca alternative for deep-draft vessels (VLCCs)",
        "geopolitical_risk": "Low",
        "risk_notes":   "VLCC alternative to Malacca; deeper water allows fully laden supertankers",
    },
]


def wind_to_beaufort(ws: float) -> int:
    """Convert wind speed (m/s) to Beaufort scale number."""
    thresholds = [0.3, 1.6, 3.4, 5.5, 8.0, 10.8, 13.9, 17.2, 20.8, 24.5, 28.5, 32.7]
    for bf, t in enumerate(thresholds):
        if ws < t:
            return bf
    return 12


BEAUFORT_DESCS = [
    "Calm", "Light air", "Light breeze", "Gentle breeze", "Moderate breeze",
    "Fresh breeze", "Strong breeze", "Near gale", "Gale", "Strong gale",
    "Storm", "Violent storm", "Hurricane",
]


def fetch_weather(lat: float, lon: float, api_key: str, retries: int = 3) -> dict | None:
    """Fetch current weather from OpenWeatherMap for a lat/lon point."""
    url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    )
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            print(f"  Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(2)
    return None


def build_port_record(port: dict, raw: dict) -> dict:
    """Build a port weather record matching the notebook schema exactly."""
    ws  = raw["wind"]["speed"]
    bf  = wind_to_beaufort(ws)
    now = datetime.now(timezone.utc)

    # sunrise / sunset — OpenWeatherMap returns Unix timestamps
    sunrise_ts = raw.get("sys", {}).get("sunrise")
    sunset_ts  = raw.get("sys", {}).get("sunset")
    sunrise_str = (
        datetime.fromtimestamp(sunrise_ts, tz=timezone.utc).strftime("%H:%M")
        if sunrise_ts else None
    )
    sunset_str = (
        datetime.fromtimestamp(sunset_ts, tz=timezone.utc).strftime("%H:%M")
        if sunset_ts else None
    )

    return {
        "port_name":      port["name"],
        "country_iso":    port["country"],
        "port_rank":      port["rank"],
        "lat":            port["lat"],
        "lon":            port["lon"],
        "fetched_at":     now.strftime("%Y-%m-%d %H:%M"),
        "temp_c":         raw["main"]["temp"],
        "feels_like_c":   raw["main"]["feels_like"],       # ← was missing in old script
        "humidity_pct":   raw["main"]["humidity"],
        "pressure_hpa":   raw["main"]["pressure"],
        "wind_speed_ms":  ws,
        "wind_gust_ms":   raw["wind"].get("gust", ws),
        "wind_deg":       raw["wind"].get("deg"),
        "visibility_m":   raw.get("visibility", 10000),
        "weather_main":   raw["weather"][0]["main"],
        "weather_desc":   raw["weather"][0]["description"],
        "cloudiness_pct": raw["clouds"]["all"],
        "rain_1h_mm":     raw.get("rain", {}).get("1h", 0.0),
        "snow_1h_mm":     raw.get("snow", {}).get("1h", 0.0),
        "sunrise_utc":    sunrise_str,                      # ← was missing in old script
        "sunset_utc":     sunset_str,                       # ← was missing in old script
        "beaufort_number": bf,
        "beaufort_desc":  BEAUFORT_DESCS[bf],               # ← was missing in old script
        "port_risk_flag": bf >= 7,
        "low_visibility": raw.get("visibility", 10000) < 1000,
        "fetch_date":     now.strftime("%Y-%m-%d"),
        "fetch_year":     now.year,                         # ← was missing in old script
        "fetch_month":    now.month,                        # ← was missing in old script
        "source":         "openweathermap",
        "data_type":      "port_weather",
        "granularity":    "daily",
    }


def build_strait_record(strait: dict, raw: dict) -> dict:
    """Build a strait conditions record with disruption risk scoring."""
    ws  = raw["wind"]["speed"]
    bf  = wind_to_beaufort(ws)
    vis = raw.get("visibility", 10000)
    now = datetime.now(timezone.utc)

    # Disruption risk score (0–100):
    #   Beaufort 7+ = severe weather risk
    #   Low visibility = navigation risk
    #   Geopolitical risk adds a baseline
    geo_baseline = {"Very High": 40, "High": 25, "Medium": 10, "Low": 0}
    geo_score = geo_baseline.get(strait["geopolitical_risk"], 0)
    weather_score = min(bf * 6, 60)   # Beaufort 10 = 60 pts
    vis_score = 10 if vis < 1000 else 5 if vis < 5000 else 0
    disruption_score = min(100, geo_score + weather_score + vis_score)

    # Risk level label
    if disruption_score >= 60:
        risk_level = "Critical"
    elif disruption_score >= 40:
        risk_level = "High"
    elif disruption_score >= 20:
        risk_level = "Moderate"
    else:
        risk_level = "Normal"

    return {
        "strait_name":        strait["name"],
        "lat":                strait["lat"],
        "lon":                strait["lon"],
        "region":             strait["region"],
        "connects":           strait["connects"],
        "trade_pct_global":   strait["trade_pct"],
        "oil_pct_global":     strait["oil_pct"],
        "key_routes":         strait["key_routes"],
        "geopolitical_risk":  strait["geopolitical_risk"],
        "risk_notes":         strait["risk_notes"],
        "fetch_date":         now.strftime("%Y-%m-%d"),
        "fetched_at":         now.strftime("%Y-%m-%d %H:%M"),
        "temp_c":             raw["main"]["temp"],
        "humidity_pct":       raw["main"]["humidity"],
        "pressure_hpa":       raw["main"]["pressure"],
        "wind_speed_ms":      ws,
        "wind_gust_ms":       raw["wind"].get("gust", ws),
        "wind_deg":           raw["wind"].get("deg"),
        "visibility_m":       vis,
        "weather_main":       raw["weather"][0]["main"],
        "weather_desc":       raw["weather"][0]["description"],
        "cloudiness_pct":     raw["clouds"]["all"],
        "rain_1h_mm":         raw.get("rain", {}).get("1h", 0.0),
        "beaufort_number":    bf,
        "beaufort_desc":      BEAUFORT_DESCS[bf],
        "weather_risk_flag":  bf >= 7,
        "low_visibility":     vis < 1000,
        "disruption_score":   disruption_score,   # 0-100 composite
        "risk_level":         risk_level,          # Normal/Moderate/High/Critical
        "source":             "openweathermap",
        "data_type":          "strait_conditions",
        "granularity":        "daily",
    }


def upload_to_bigquery(df: pd.DataFrame, table_name: str, disposition: str = "WRITE_APPEND") -> None:
    """Upload a DataFrame to BigQuery."""
    client   = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"
    job_cfg  = bigquery.LoadJobConfig(
        write_disposition=getattr(bigquery.WriteDisposition, disposition),
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_cfg)
    job.result()
    tbl = client.get_table(table_id)
    print(f"  ✓ Uploaded {len(df):,} rows → {table_id} (total: {tbl.num_rows:,})")


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print("=" * 60)
    print(f"ingest_weather.py  —  {now_str}")
    print("=" * 60)

    # ── 1. Fetch port weather ─────────────────────────────────────────────────
    print(f"\n[1/4] Fetching port weather ({len(TOP_PORTS)} ports)...")
    port_records = []
    for port in TOP_PORTS:
        raw = fetch_weather(port["lat"], port["lon"], WEATHER_API_KEY)
        if raw:
            rec = build_port_record(port, raw)
            port_records.append(rec)
            bf  = rec["beaufort_number"]
            print(f"  {port['name']:20s} {rec['temp_c']:5.1f}°C  "
                  f"wind {rec['wind_speed_ms']:4.1f} m/s  Bf{bf}"
                  f"{'  ⚠ RISK' if rec['port_risk_flag'] else ''}")
        else:
            print(f"  {port['name']:20s} FAILED")
        time.sleep(0.5)

    # ── 2. Fetch strait conditions ────────────────────────────────────────────
    print("\n[2/4] Fetching strait conditions (8 straits)...")
    strait_records = []
    for strait in STRAITS:
        raw = fetch_weather(strait["lat"], strait["lon"], WEATHER_API_KEY)
        if raw:
            rec = build_strait_record(strait, raw)
            strait_records.append(rec)
            print(f"  {strait['name']:30s}  Bf{rec['beaufort_number']}  "
                  f"score={rec['disruption_score']:3d}  [{rec['risk_level']}]"
                  f"{'  ⚠ GEO:' + strait['geopolitical_risk'] if strait['geopolitical_risk'] in ('High','Very High') else ''}")
        else:
            print(f"  {strait['name']:30s} FAILED")
        time.sleep(0.5)

    # ── 3. Save to disk ───────────────────────────────────────────────────────
    print("\n[3/4] Saving to disk...")

    # Raw JSON backup
    with open("data/raw/port_weather_raw.json", "w") as f:
        json.dump(port_records, f, indent=2, default=str)
    with open("data/raw/strait_conditions_raw.json", "w") as f:
        json.dump(strait_records, f, indent=2, default=str)

    # Clean CSVs
    df_ports   = pd.DataFrame(port_records)
    df_straits = pd.DataFrame(strait_records)

    df_ports.to_csv("data/clean/port_weather_clean.csv",    index=False)
    df_straits.to_csv("data/clean/strait_conditions.csv",   index=False)

    print(f"  Saved {len(df_ports)} port records  → data/clean/port_weather_clean.csv")
    print(f"  Saved {len(df_straits)} strait records → data/clean/strait_conditions.csv")

    # ── 4. Upload to BigQuery ─────────────────────────────────────────────────
    print("\n[4/4] Uploading to BigQuery...")
    try:
        upload_to_bigquery(df_ports,   "port_weather",      "WRITE_APPEND")
        upload_to_bigquery(df_straits, "strait_conditions", "WRITE_APPEND")
    except Exception as e:
        print(f"  BigQuery upload failed: {e}")
        print("  CSVs saved locally — upload manually if needed.")
        raise

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    risk_ports   = sum(1 for r in port_records   if r.get("port_risk_flag"))
    crit_straits = sum(1 for r in strait_records if r.get("risk_level") in ("High", "Critical"))
    print(f"Ports at risk (Bf≥7):        {risk_ports} / {len(port_records)}")
    print(f"Straits elevated risk:        {crit_straits} / {len(strait_records)}")
    if crit_straits:
        for r in strait_records:
            if r["risk_level"] in ("High", "Critical"):
                print(f"  ⚠  {r['strait_name']} — {r['risk_level']} "
                      f"(score={r['disruption_score']}, geo={r['geopolitical_risk']})")
    print("Done.")
