"""
transform.py

Transform step for AtmosTrack Multi-City AQI ETL Pipeline.

- Flattens hourly JSON into tabular format
- Computes AQI category, severity score, and risk classification
- Saves combined CSV to data/staged/air_quality_transformed.csv
"""

import json
from pathlib import Path
import pandas as pd

# ------------------------------
# Directories
# ------------------------------
RAW_DIR = Path("data/raw/")
STAGED_DIR = Path("data/staged/")
STAGED_DIR.mkdir(parents=True, exist_ok=True)
STAGED_FILE = STAGED_DIR / "air_quality_transformed.csv"

# ------------------------------
# Feature Engineering
# ------------------------------
def compute_aqi(pm2_5):
    if pm2_5 <= 50:
        return "Good"
    elif pm2_5 <= 100:
        return "Moderate"
    elif pm2_5 <= 200:
        return "Unhealthy"
    elif pm2_5 <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"

def compute_severity(row):
    return (
        row.get("pm2_5", 0) * 5 +
        row.get("pm10", 0) * 3 +
        row.get("nitrogen_dioxide", 0) * 4 +
        row.get("sulphur_dioxide", 0) * 4 +
        row.get("carbon_monoxide", 0) * 2 +
        row.get("ozone", 0) * 3
    )

def classify_risk(severity):
    if severity > 400:
        return "High Risk"
    elif severity > 200:
        return "Moderate Risk"
    else:
        return "Low Risk"

# ------------------------------
# Transform single city file
# ------------------------------
def transform_city_file(file_path: Path) -> pd.DataFrame:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    city_name = file_path.stem.split("_raw_")[0]
    hourly = data.get("hourly", {})

    if not hourly:
        return pd.DataFrame()

    df = pd.DataFrame(hourly)
    if df.empty:
        return pd.DataFrame()

    df["city"] = city_name

    # Convert numeric columns
    for col in ["pm10","pm2_5","carbon_monoxide","nitrogen_dioxide","ozone","sulphur_dioxide","uv_index"]:
        df[col] = pd.to_numeric(df.get(col, 0), errors="coerce")

    # Drop rows where all pollutants are missing
    df = df.dropna(subset=["pm10","pm2_5","carbon_monoxide","nitrogen_dioxide","ozone","sulphur_dioxide"], how="all")

    # Convert time to datetime and extract hour
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["hour"] = df["time"].dt.hour

    # Feature engineering
    df["AQI_category"] = df["pm2_5"].apply(compute_aqi)
    df["severity"] = df.apply(compute_severity, axis=1)
    df["risk"] = df["severity"].apply(classify_risk)

    return df

# ------------------------------
# Transform all files
# ------------------------------
def transform_all():
    files = list(RAW_DIR.glob("*.json"))
    all_dfs = []

    for f in files:
        df = transform_city_file(f)
        if not df.empty:
            all_dfs.append(df)

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df.to_csv(STAGED_FILE, index=False)
        print(f"✅ Transformed data saved to {STAGED_FILE}")
        return final_df
    else:
        print("⚠️ No data transformed. Check raw files.")
        return pd.DataFrame()

# ------------------------------
# Main
# ------------------------------
if __name__ == "__main__":
    print("Starting transform step...")
    transform_all()
    print("Transform step completed.")



import os
import json
import pandas as pd
from datetime import datetime
from pathlib import Path

RAW_DIR = "data/raw"
STAGED_DIR = "data/staged"

os.makedirs(STAGED_DIR, exist_ok=True)

OUTPUT_FILE = os.path.join(STAGED_DIR, "air_quality_transformed.csv")

# ---------------------------
# AQI Category (PM2.5 Based)
# ---------------------------
def aqi_category(pm25):
    if pd.isna(pm25):
        return "Unknown"
    if pm25 <= 50:
        return "Good"
    elif pm25 <= 100:
        return "Moderate"
    elif pm25 <= 200:
        return "Unhealthy"
    elif pm25 <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"


# ---------------------------
# Risk Classification
# ---------------------------
def classify_risk(severity):
    if severity > 400:
        return "High Risk"
    elif severity > 200:
        return "Moderate Risk"
    else:
        return "Low Risk"


# ---------------------------
# Load and Transform
# ---------------------------
def transform_data():

    all_rows = []

    raw_files = list(Path(RAW_DIR).glob("*.json"))

    if not raw_files:
        print("❌ No raw files found in data/raw/. Run extract.py first!")
        return

    for file in raw_files:
        city = file.name.split("_")[0]

        with open(file, "r") as f:
            data = json.load(f)

        if "hourly" not in data:
            print(f" Skipping {file} — No 'hourly' field")
            continue

        hourly = data["hourly"]

        times = hourly.get("time", [])
        pm10 = hourly.get("pm10", [])
        pm25 = hourly.get("pm2_5", [])
        co = hourly.get("carbon_monoxide", [])
        no2 = hourly.get("nitrogen_dioxide", [])
        so2 = hourly.get("sulphur_dioxide", [])
        ozone = hourly.get("ozone", [])
        uv = hourly.get("uv_index", [])

        rows = zip(times, pm10, pm25, co, no2, so2, ozone, uv)

        for t, p10, p25, co_v, no2_v, so2_v, oz_v, uv_v in rows:
            row = {
                "city": city,
                "time": t,
                "pm10": p10,
                "pm2_5": p25,
                "carbon_monoxide": co_v,
                "nitrogen_dioxide": no2_v,
                "sulphur_dioxide": so2_v,
                "ozone": oz_v,
                "uv_index": uv_v,
            }
            all_rows.append(row)

    df = pd.DataFrame(all_rows)

    # Convert time → datetime
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    # Convert numeric columns
    num_cols = [
        "pm10", "pm2_5", "carbon_monoxide",
        "nitrogen_dioxide", "sulphur_dioxide",
        "ozone", "uv_index"
    ]

    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Remove rows where all pollutants are missing
    df.dropna(subset=num_cols, how="all", inplace=True)

    # Feature Engineering
    df["AQI_Category"] = df["pm2_5"].apply(aqi_category)

    df["severity"] = (
        (df["pm2_5"] * 5) +
        (df["pm10"] * 3) +
        (df["nitrogen_dioxide"] * 4) +
        (df["sulphur_dioxide"] * 4) +
        (df["carbon_monoxide"] * 2) +
        (df["ozone"] * 3)
    )

    df["Risk_Level"] = df["severity"].apply(classify_risk)

    df["hour"] = df["time"].dt.hour

    # Save final staged file
    df.to_csv(OUTPUT_FILE, index=False)

    print(f" Transform completed! File saved to:\n{OUTPUT_FILE}")


if __name__ == "__main__":
    transform_data()