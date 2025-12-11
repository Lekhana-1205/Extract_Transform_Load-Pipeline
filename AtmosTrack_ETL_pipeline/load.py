import os
import time
import math
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# Config (can be overridden via env)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
LOAD_MAX_RETRIES = int(os.getenv("LOAD_MAX_RETRIES", "2"))
LOAD_BACKOFF_SECONDS = int(os.getenv("LOAD_BACKOFF_SECONDS", "3"))

TRANSFORMED_FILE = os.getenv("TRANSFORMED_FILE", "data/staged/air_quality_transformed.csv")
TABLE_NAME = os.getenv("TABLE_NAME", "air_quality_data")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Please set SUPABASE_URL and SUPABASE_KEY in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def normalize_and_prepare(df: pd.DataFrame) -> list:
    """
    1) Normalize column names to match DB
    2) Convert NaN -> None
    3) Convert datetimes to ISO strings
    Returns list of dict records ready for JSON insert.
    """
    # Standardize column names from transform.py to DB schema
    rename_map = {
        # if transform used different names, map them here
        "AQI_Category": "aqi_category",
        "AQI_Category".lower(): "aqi_category",
        "Risk_Level": "risk_flag",
        "risk": "risk_flag",
        "severity": "severity_score",
        "Severity": "severity_score",
        "pm2_5": "pm2_5",
        "pm25": "pm2_5",
        "pm10": "pm10",
        "carbon_monoxide": "carbon_monoxide",
        "nitrogen_dioxide": "nitrogen_dioxide",
        "sulphur_dioxide": "sulphur_dioxide",
        "ozone": "ozone",
        "uv_index": "uv_index",
        "hour": "hour",
        "city": "city",
        "time": "time",
    }

    # Apply rename for columns that exist
    cols_to_rename = {c: rename_map[c] for c in df.columns if c in rename_map}
    if cols_to_rename:
        df = df.rename(columns=cols_to_rename)

    # Ensure all DB columns present (if missing, add with None)
    expected = ["city","time","pm10","pm2_5","carbon_monoxide","nitrogen_dioxide",
                "sulphur_dioxide","ozone","uv_index","aqi_category","severity_score",
                "risk_flag","hour"]
    for c in expected:
        if c not in df.columns:
            df[c] = None

    # Convert time to ISO formatted strings (or None)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["time"] = df["time"].apply(lambda t: t.isoformat() if not pd.isna(t) else None)

    # Convert numeric columns to native python types and NaN->None
    numeric_cols = ["pm10","pm2_5","carbon_monoxide","nitrogen_dioxide",
                    "sulphur_dioxide","ozone","uv_index","severity_score","hour"]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
            df[c] = df[c].where(df[c].notna(), None)

    # Text columns: convert NaN → None
    text_cols = ["city","aqi_category","risk_flag"]
    for c in text_cols:
        df[c] = df[c].where(df[c].notna(), None)

    # Convert DataFrame to list of dicts
    records = df[expected].to_dict(orient="records")
    return records

def insert_batches(records: list):
    total = len(records)
    inserted = 0
    if total == 0:
        print("No records to insert.")
        return inserted

    batches = math.ceil(total / BATCH_SIZE)
    for idx in range(0, total, BATCH_SIZE):
        batch = records[idx: idx + BATCH_SIZE]
        batch_no = idx // BATCH_SIZE + 1

        attempt = 0
        while attempt <= LOAD_MAX_RETRIES:
            try:
                # supabase-py insertion
                res = supabase.table(TABLE_NAME).insert(batch).execute()
                # Check for errors in response (library version dependent)
                if hasattr(res, "error") and res.error:
                    raise RuntimeError(res.error)
                # Some versions return dict-like: res.get("error")
                if isinstance(res, dict) and res.get("error"):
                    raise RuntimeError(res.get("error"))
                print(f"✅ Inserted batch {batch_no}/{batches} ({len(batch)} rows)")
                inserted += len(batch)
                break
            except Exception as e:
                attempt += 1
                print(f"⚠️ Insert failed for batch {batch_no} (attempt {attempt}/{LOAD_MAX_RETRIES}): {e}")
                if attempt > LOAD_MAX_RETRIES:
                    print(f"❌ Skipping batch {batch_no} after repeated failures.")
                    break
                time.sleep(LOAD_BACKOFF_SECONDS)
    return inserted

def load_data():
    if not os.path.exists(TRANSFORMED_FILE):
        print("❌ No transformed file found. Run transform.py first!")
        return

    df = pd.read_csv(TRANSFORMED_FILE)
    print(f"Loaded {len(df)} rows from staged file.")

    records = normalize_and_prepare(df)
    inserted = insert_batches(records)

    print(f"\n⭐ Load completed: {inserted}/{len(records)} rows inserted.")

if __name__ == "__main__":
    load_data()