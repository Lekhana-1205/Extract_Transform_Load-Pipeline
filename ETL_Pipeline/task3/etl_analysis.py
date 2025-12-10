import os
import pandas as pd
import numpy as np
from supabase import create_client, Client
from dotenv import load_dotenv
import matplotlib.pyplot as plt


# ---------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY environment variables.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ---------------------------------------------------------
# Fetch Data
# ---------------------------------------------------------
def fetch_data():
    print("[INFO] Fetching data from Supabase...")
    response = supabase.table("churn_data").select("*").limit(10000).execute()
    df = pd.DataFrame(response.data)

    if df.empty:
        raise ValueError("No data retrieved from Supabase.")

    df.columns = df.columns.str.lower()
    print(f"[INFO] Retrieved {len(df)} rows.")
    return df


# ---------------------------------------------------------
# Metrics Calculation
# ---------------------------------------------------------
def calculate_metrics(df):
    print("[INFO] Calculating metrics...")

    churn_percentage = (
        df["churn"].value_counts(normalize=True).get("yes", 0) * 100
    )

    avg_monthly_by_contract = (
        df.groupby("contract")["monthlycharges"]
        .mean()
        .round(2)
        .to_dict()
    )

    customer_type_dist = df["tenure_group"].value_counts().to_dict()
    internet_dist = df["internetservice"].value_counts().to_dict()

    return {
        "churn_percentage": round(churn_percentage, 2),
        "avg_monthly_by_contract": avg_monthly_by_contract,
        "customer_type_distribution": customer_type_dist,
        "internet_distribution": internet_dist,
    }


# ---------------------------------------------------------
# Pivot Table
# ---------------------------------------------------------
def churn_tenure_pivot(df):
    print("[INFO] Creating pivot table...")
    pivot = pd.pivot_table(
        df,
        index="tenure_group",
        columns="churn",
        values="monthlycharges",
        aggfunc="count",
        fill_value=0,
    )
    return pivot


# ---------------------------------------------------------
# Visualizations
# ---------------------------------------------------------
def generate_visuals(df):
    print("[INFO] Generating plots...")
    os.makedirs("data/processed", exist_ok=True)

    # Histogram of TotalCharges
    plt.figure(figsize=(6, 4))
    plt.hist(df["totalcharges"].dropna())
    plt.title("Distribution of Total Charges")
    plt.xlabel("Total Charges")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig("data/processed/totalcharges_histogram.png")
    plt.close()

    # Churn by Monthly Charge Segment
    plt.figure(figsize=(6, 4))
    df.groupby("monthly_charge_segment")["churn"].value_counts().unstack().plot(kind="bar")
    plt.title("Churn by Monthly Charge Segment")
    plt.tight_layout()
    plt.savefig("data/processed/churn_by_charge_segment.png")
    plt.close()

    # Contract Type Distribution
    plt.figure(figsize=(6, 4))
    df["contract"].value_counts().plot(kind="bar")
    plt.title("Contract Type Distribution")
    plt.tight_layout()
    plt.savefig("data/processed/contract_type_distribution.png")
    plt.close()


# ---------------------------------------------------------
# Save Output
# ---------------------------------------------------------
def save_summary(metrics, pivot):
    print("[INFO] Saving analysis summary...")
    output_path = "data/processed/analysis_summary.csv"

    summary_rows = []
    summary_rows.append(["Churn Percentage", metrics["churn_percentage"]])

    for k, v in metrics["avg_monthly_by_contract"].items():
        summary_rows.append([f"Avg Monthly Charges ({k})", v])

    for k, v in metrics["customer_type_distribution"].items():
        summary_rows.append([f"Customer Type Count ({k})", v])

    for k, v in metrics["internet_distribution"].items():
        summary_rows.append([f"Internet Service Count ({k})", v])

    summary_df = pd.DataFrame(summary_rows, columns=["Metric", "Value"])

    pivot_df = pivot.reset_index()
    pivot_df.insert(0, "Metric", "Churn vs Tenure Group")

    with open(output_path, "w", encoding="utf-8") as f:
        summary_df.to_csv(f, index=False)
        f.write("\n")
        pivot_df.to_csv(f, index=False)

    print(f"[INFO] Summary saved: {output_path}")


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
if __name__ == "__main__":
    df = fetch_data()
    metrics = calculate_metrics(df)
    pivot = churn_tenure_pivot(df)

    generate_visuals(df)
    save_summary(metrics, pivot)

    print("[INFO] ETL Analysis Completed Successfully.")
