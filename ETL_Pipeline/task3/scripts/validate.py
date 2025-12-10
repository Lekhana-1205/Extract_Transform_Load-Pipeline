# validate.py
# ============================
# Purpose: Validate loaded data in Supabase
# - No missing values in tenure, MonthlyCharges, TotalCharges
# - Unique row count equals original dataset
# - Row count matches Supabase table
# - All segments exist (tenure_group, monthly_charge_segment)
# - Contract codes only in {0,1,2}
# ============================

import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

def get_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in .env")
    return create_client(url, key)

def validate():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_path = os.path.join(base_dir, "data", "raw", "churn_raw.csv")
    staged_path = os.path.join(base_dir, "data", "staged", "churn_transformed.csv")

    if not os.path.exists(staged_path):
        raise FileNotFoundError(f"Transformed file not found: {staged_path}")
    df_staged = pd.read_csv(staged_path)

    # 1) No missing values in tenure, MonthlyCharges, TotalCharges (staged)
    missing_info = {
        "tenure": int(df_staged["tenure"].isnull().sum()) if "tenure" in df_staged else None,
        "MonthlyCharges": int(df_staged["MonthlyCharges"].isnull().sum()) if "MonthlyCharges" in df_staged else None,
        "TotalCharges": int(df_staged["TotalCharges"].isnull().sum()) if "TotalCharges" in df_staged else None
    }

    # 2) Unique count of rows = original dataset (raw)
    if os.path.exists(raw_path):
        df_raw = pd.read_csv(raw_path)
        raw_unique = df_raw.shape[0]
    else:
        raw_unique = None

    staged_unique = df_staged.shape[0]
    staged_unique_rows = df_staged.drop_duplicates().shape[0]

    # 3) Row count in Supabase
    supabase = get_supabase_client()
    try:
        resp = supabase.table("churn_data").select("*", count="exact").limit(1).execute()
        supabase_count = None
        if hasattr(resp, "count") and resp.count is not None:
            supabase_count = int(resp.count)
        else:
            try:
                supabase_count = int(resp.get("count"))
            except Exception:
                all_rows = supabase.table("churn_data").select("*").execute()
                supabase_count = len(all_rows.data) if hasattr(all_rows, "data") else len(all_rows)

    except Exception as e:
        print(" Could not fetch count from Supabase:", e)
        supabase_count = None

    # 4) All segments exist in staged data
    tenure_groups_present = set(df_staged["tenure_group"].dropna().unique()) if "tenure_group" in df_staged else set()
    monthly_segments_present = set(df_staged["monthly_charge_segment"].dropna().unique()) if "monthly_charge_segment" in df_staged else set()

    required_tenure_groups = {"New", "Regular", "Loyal", "Champion"}
    required_monthly_segments = {"Low", "Medium", "High"}

    # 5) Contract codes only {0,1,2}
    contract_codes_present = set(df_staged["contract_type_code"].dropna().unique()) if "contract_type_code" in df_staged else set()
    contract_codes_ok = contract_codes_present.issubset({-1,0,1,2})  

    # Print summary
    print("\n===== VALIDATION SUMMARY =====")
    print("Missing values (staged):")
    for k, v in missing_info.items():
        print(f"  {k}: {v}")
    print()
    print("Row counts:")
    print(f"  Raw file rows: {raw_unique}")
    print(f"  Staged file rows: {staged_unique}")
    print(f"  Staged unique rows: {staged_unique_rows}")
    print(f"  Supabase table rows: {supabase_count}")
    print()
    print("Segments presence:")
    print(f"  Tenure groups present: {tenure_groups_present}")
    print(f"  Monthly segments present: {monthly_segments_present}")
    print()
    print(f"Tenure groups completeness (should contain {required_tenure_groups}): {required_tenure_groups.issubset(tenure_groups_present)}")
    print(f"Monthly segments completeness (should contain {required_monthly_segments}): {required_monthly_segments.issubset(monthly_segments_present)}")
    print()
    print(f"Contract codes found: {contract_codes_present}")
    print(f"Contract codes OK (subset of -1,0,1,2): {contract_codes_ok}")
    print("\n===== END VALIDATION =====\n")

    # Return summary as dict if needed programmatically
    return {
        "missing_info": missing_info,
        "raw_rows": raw_unique,
        "staged_rows": staged_unique,
        "staged_unique_rows": staged_unique_rows,
        "supabase_count": supabase_count,
        "tenure_groups_present": tenure_groups_present,
        "monthly_segments_present": monthly_segments_present,
        "contract_codes_present": contract_codes_present,
        "contract_codes_ok": contract_codes_ok
    }

if __name__ == "__main__":
    validate()