import os
import json
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
import requests

# ------------------------------
# Config
# ------------------------------
RAW_DIR = Path("data/raw/")
RAW_DIR.mkdir(parents=True, exist_ok=True)

CITIES = {
    "Delhi": {"lat": 28.7041, "lon": 77.1025},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777},
    "Bengaluru": {"lat": 12.9716, "lon": 77.5946},
    "Hyderabad": {"lat": 17.3850, "lon": 78.4867},
    "Kolkata": {"lat": 22.5726, "lon": 88.3639},
}

API_BASE = "https://air-quality-api.open-meteo.com/v1/air-quality"
MAX_RETRIES = 3
TIMEOUT = 10
SLEEP_BETWEEN_CALLS = 1.0  # seconds

# ------------------------------
# Helper functions
# ------------------------------

def now_ts() -> str:
    """Return UTC timestamp for filenames."""
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def save_raw(city: str, payload: dict) -> str:
    """Save JSON payload to raw directory with timestamp."""
    filename = f"{city.lower()}_raw_{now_ts()}.json"
    path = RAW_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return str(path.resolve())


def fetch_city(city: str, lat: float, lon: float) -> Dict[str, Optional[str]]:
    """Fetch hourly pollutant data for a city with retry logic."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide,uv_index"
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(API_BASE, params=params, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            path = save_raw(city, data)
            print(f"✅ [{city}] Data saved to {path}")
            return {"city": city, "success": True, "raw_path": path}
        except requests.RequestException as e:
            print(f"⚠️ [{city}] Attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                backoff = 2 ** (attempt - 1)
                print(f"⏳ Retrying in {backoff}s ...")
                time.sleep(backoff)
    return {"city": city, "success": False, "error": f"Failed after {MAX_RETRIES} attempts"}


def fetch_all_cities() -> List[Dict[str, Optional[str]]]:
    results = []
    for city, coords in CITIES.items():
        res = fetch_city(city, coords["lat"], coords["lon"])
        results.append(res)
        time.sleep(SLEEP_BETWEEN_CALLS)
    return results


if __name__ == "__main__":
    print("Starting extraction of hourly AQI data...")
    output = fetch_all_cities()
    print("Extraction complete. Summary:")
    for r in output:
        if r.get("success"):
            print(f" - {r['city']}: saved -> {r['raw_path']}")
        else:
            print(f" - {r['city']}: ERROR -> {r.get('error')}")
