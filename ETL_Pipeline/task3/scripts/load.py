# ===========================
# load.py
# ===========================

import os
import time
import pandas as pd
import numpy as np
from supabase import create_client, Client
from dotenv import load_dotenv

# ---------------------------------------------------------
# 1️ LOAD ENVIRONMENT VARIABLES
# ---------------------------------------------------------
def get_supabase_client():
    load_dotenv()

    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")  

    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError(" Missing SUPABASE_URL or SUPABASE_KEY in .env file.")

    return create_client(SUPABASE_URL, SUPABASE_KEY)

supabase = get_supabase_client()


# ---------------------------------------------------------
# 2️ CREATE TABLE IF DOES NOT EXIST (SAFE METHOD)
# ---------------------------------------------------------
def create_table():
    print(" Checking if table exists...")

    try:
        # Try selecting 1 row → if table does not exist, APIError will occur.
        supabase.table("churn_data").select("*").limit(1).execute()
        print(" Table already exists: churn_data")
        return

    except Exception:
        print(" Table not found — creating table...")

    create_sql = """
    CREATE TABLE IF NOT EXISTS churn_data (
        id BIGSERIAL PRIMARY KEY,
        tenure INTEGER,
        monthlycharges FLOAT,
        totalcharges FLOAT,
        churn TEXT,
        internetservice TEXT,
        contract TEXT,
        paymentmethod TEXT,
        tenure_group TEXT,
        monthly_charge_segment TEXT,
        has_internet_service INTEGER,
        is_multi_line_user INTEGER,
        contract_type_code INTEGER
    );
    """

    # Safe table creation using Supabase SQL endpoint
    try:
        supabase.postgrest._client.request(
            "POST",
            "/rpc/execute",
            json={"query": create_sql}
        )
        print(" Table created successfully!")
    except:
        print(" SQL RPC function not available — using fallback method.")
        print("➡ Please create table manually inside Supabase SQL Editor:")
        print(create_sql)
        exit()


# ---------------------------------------------------------
# 3️ LOAD DATA INTO SUPABASE (Batch Upload)
# ---------------------------------------------------------
def load_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_path = os.path.join(base_dir, "data", "staged", "churn_transformed.csv")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(" Staged dataset not found! Run transform.py first.")

    df = pd.read_csv(csv_path)

    # Convert column names → lowercase
    df.columns = df.columns.str.lower()

    # Allowed columns only
    allowed_cols = [
        "tenure", "monthlycharges", "totalcharges", "churn",
        "internetservice", "contract", "paymentmethod",
        "tenure_group", "monthly_charge_segment",
        "has_internet_service", "is_multi_line_user",
        "contract_type_code"
    ]
    df = df[allowed_cols]

    # Convert NaN → None
    df = df.replace({np.nan: None})

    data_records = df.to_dict(orient="records")
    total_rows = len(data_records)
    batch_size = 200

    print(f" Uploading {total_rows} records to Supabase (Batch size = 200)...")

    # Upload in batches
    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        batch = data_records[start:end]

        attempts = 0
        uploaded = False

        while attempts < 3 and not uploaded:
            try:
                supabase.table("churn_data").insert(batch).execute()
                print(f" Uploaded rows {start} → {end}")
                uploaded = True

            except Exception as e:
                attempts += 1
                print(f" Upload failed (Attempt {attempts}/3) for rows {start}–{end}")
                print("   Error:", e)
                time.sleep(2)

        if not uploaded:
            print(f" Failed to upload rows {start}–{end} even after 3 retries.")
            break

    print(" Upload completed successfully!")


# ---------------------------------------------------------
# 4️ MAIN EXECUTION
# ---------------------------------------------------------
if __name__ == "__main__":
    create_table()
    load_data()
