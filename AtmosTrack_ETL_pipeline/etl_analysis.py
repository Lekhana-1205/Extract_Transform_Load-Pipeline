"""
etl_analysis.py

Analysis step for AtmosTrack Multi-City AQI ETL Pipeline.

- Reads transformed CSV
- Generates plots: AQI trends, severity trends, risk distribution
- Saves plots to data/analysis/
"""
'''
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# ------------------------------
# Paths
# ------------------------------
STAGED_FILE = Path("data/staged/air_quality_transformed.csv")
ANALYSIS_DIR = Path("data/analysis")
ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

# ------------------------------
# Load data
# ------------------------------
if not STAGED_FILE.exists():
    raise FileNotFoundError(f"Transformed CSV not found: {STAGED_FILE}")

df = pd.read_csv(STAGED_FILE, parse_dates=["time"])
print(f"Loaded {len(df)} rows for analysis.")

# ------------------------------
# 1️⃣ City-wise AQI trend plots
# ------------------------------
for city in df["city"].unique():
    city_df = df[df["city"] == city].sort_values("time")

    plt.figure(figsize=(12,5))
    plt.plot(city_df["time"], city_df["pm2_5"], label="PM2.5", color="red")
    plt.title(f"{city} PM2.5 Trend")
    plt.xlabel("Time")
    plt.ylabel("PM2.5 (µg/m³)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(ANALYSIS_DIR / f"{city}_pm2_5_trend.png")
    plt.close()

    plt.figure(figsize=(12,5))
    # Risk counts over time (numerical mapping for visualization)
    risk_map = {"Low Risk": 1, "Moderate Risk": 2, "High Risk": 3}
    plt.plot(city_df["time"], city_df["severity"], label="Severity Score", color="blue")
    plt.title(f"{city} Pollution Severity Trend")
    plt.xlabel("Time")
    plt.ylabel("Severity Score")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(ANALYSIS_DIR / f"{city}_severity_trend.png")
    plt.close()

# ------------------------------
# 2️⃣ Risk classification distribution
# ------------------------------
for city in df["city"].unique():
    city_df = df[df["city"] == city]
    risk_counts = city_df["risk"].value_counts()
    
    plt.figure(figsize=(6,4))
    risk_counts.plot(kind="bar", color=["green","orange","red"])
    plt.title(f"{city} Risk Classification Distribution")
    plt.xlabel("Risk Level")
    plt.ylabel("Count")
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(ANALYSIS_DIR / f"{city}_risk_distribution.png")
    plt.close()

print(f"✅ Analysis completed. Plots saved to {ANALYSIS_DIR}")


'''

import os
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from supabase import create_client

# -------------------------------------------------------------------
# Load environment and connect
# -------------------------------------------------------------------
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("❌ Please set SUPABASE_URL and SUPABASE_KEY in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Output directories
os.makedirs("data/processed", exist_ok=True)
os.makedirs("plots", exist_ok=True)

# -------------------------------------------------------------------
# 1. Fetch data from Supabase
# -------------------------------------------------------------------
print("Fetching data from Supabase...")

res = supabase.table("air_quality_data").select("*").execute()
df = pd.DataFrame(res.data)

print(f"Loaded {len(df)} rows for analysis.")

# Ensure correct dtypes
df["time"] = pd.to_datetime(df["time"], errors="coerce")
numeric_cols = [
    "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide",
    "sulphur_dioxide", "ozone", "uv_index", "severity_score", "hour"
]

for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# -------------------------------------------------------------------
# 2. KPI METRICS
# -------------------------------------------------------------------
print("\nGenerating KPI metrics...")

kpi_metrics = {}

# A: City with highest average PM2.5
kpi_metrics["city_highest_pm25"] = (
    df.groupby("city")["pm2_5"].mean().idxmax()
    if not df.empty else None
)

# B: City with highest severity score
kpi_metrics["city_highest_severity"] = (
    df.groupby("city")["severity_score"].mean().idxmax()
    if not df.empty else None
)

# C: Risk percentage distribution
risk_counts = df["risk_flag"].value_counts(normalize=True) * 100
risk_distribution = risk_counts.to_dict()

# D: Hour of day with worst AQI (pm2_5)
kpi_metrics["worst_hour"] = (
    df.groupby("hour")["pm2_5"].mean().idxmax()
    if not df.empty else None
)

# Convert KPI to DataFrame
kpi_df = pd.DataFrame([kpi_metrics])
kpi_df.to_csv("data/processed/summary_metrics.csv", index=False)
print("✔ summary_metrics.csv saved")

# -------------------------------------------------------------------
# 3. CITY RISK DISTRIBUTION CSV
# -------------------------------------------------------------------
print("Generating risk distribution per city...")

risk_city_df = df.groupby(["city", "risk_flag"]).size().reset_index(name="count")
risk_city_df.to_csv("data/processed/city_risk_distribution.csv", index=False)
print("✔ city_risk_distribution.csv saved")

# -------------------------------------------------------------------
# 4. POLLUTION TRENDS CSV
# -------------------------------------------------------------------
print("Generating pollution trend report...")

trend_df = df[["time", "city", "pm2_5", "pm10", "ozone"]].copy()
trend_df.to_csv("data/processed/pollution_trends.csv", index=False)

print("✔ pollution_trends.csv saved")

# -------------------------------------------------------------------
# 5. VISUALIZATIONS
# -------------------------------------------------------------------
print("Generating plots...")

# A. Histogram of PM2.5
plt.figure(figsize=(8,5))
plt.hist(df["pm2_5"].dropna(), bins=30)
plt.title("PM2.5 Distribution")
plt.xlabel("PM2.5")
plt.ylabel("Frequency")
plt.savefig("plots/pm25_histogram.png", dpi=300)
plt.close()

# B. Bar chart of risk flags per city
plt.figure(figsize=(10,6))
risk_city_df.pivot(index="city", columns="risk_flag", values="count").plot(kind="bar")
plt.title("Risk Levels Per City")
plt.ylabel("Count")
plt.tight_layout()
plt.savefig("plots/risk_per_city.png", dpi=300)
plt.close()

# C. Line chart of hourly PM2.5 trend for each city
plt.figure(figsize=(12,6))
for city in df["city"].unique():
    city_df = df[df["city"] == city].sort_values("time")
    plt.plot(city_df["time"], city_df["pm2_5"], label=city)

plt.legend()
plt.title("Hourly PM2.5 Trend")
plt.xlabel("Time")
plt.ylabel("PM2.5")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("plots/pm25_trend.png", dpi=300)
plt.close()

# D. Scatter plot: severity_score vs pm2_5
plt.figure(figsize=(8,5))
plt.scatter(df["pm2_5"], df["severity_score"])
plt.title("Severity Score vs PM2.5")
plt.xlabel("PM2.5")
plt.ylabel("Severity Score")
plt.savefig("plots/severity_vs_pm25.png", dpi=300)
plt.close()

print("\n✨ ANALYSIS COMPLETE")
print("CSV files saved to: data/processed/")
print("Plots saved to: plots/")
