'''
TRANSFORM (transform.py)
must perform advanced transformations, not just cleaning.
✔ Cleaning Tasks
Convert "TotalCharges" to numeric (dataset has spaces → become NaN).
Fill missing numeric values using:
Median for tenure, MonthlyCharges, TotalCharges.
Replace missing categorical values with "Unknown".
✔ Feature Engineering
Create the following new columns:
1. tenure_group
Based on tenure months:
0–12   → "New"
13–36  → "Regular"
37–60  → "Loyal"
60+    → "Champion"
2. monthly_charge_segment
MonthlyCharges < 30  → "Low"
30–70              → "Medium"
> 70                 → "High"
3. has_internet_service
Convert InternetService column:
"DSL" / "Fiber optic" → 1
"No" → 0
4. is_multi_line_user
1 if MultipleLines == "Yes"
0 otherwise
5. contract_type_code
Map:
Month-to-month → 0
One year      → 1
Two year      → 2
✔ Drop unnecessary fields
Remove:
customerID, gender
✔ Save output to:
data/staged/churn_transformed.csv
'''

import os
import pandas as pd

def transform_telecom_data(raw_path):

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)

    # Load CSV
    df = pd.read_csv(raw_path)
    # CLEANING TASKS

    df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce")

    numeric_cols = ["tenure", "MonthlyCharges", "TotalCharges"]
    for col in numeric_cols:
        df[col] = df[col].fillna(df[col].median())

    cat_cols = df.select_dtypes(include="object").columns
    for col in cat_cols:
        df[col] = df[col].fillna("Unknown")

    # FEATURE ENGINEERING
   
    # 1. tenure_group
    df["tenure_group"] = pd.cut(
        df["tenure"],
        bins=[0, 12, 36, 60, df["tenure"].max()],
        labels=["New", "Regular", "Loyal", "Champion"],
        include_lowest=True
    )

    # 2. monthly_charge_segment
    df["monthly_charge_segment"] = pd.cut(
        df["MonthlyCharges"],
        bins=[0, 30, 70, df["MonthlyCharges"].max()],
        labels=["Low", "Medium", "High"],
        include_lowest=True
    )

    # 3. has_internet_service
    df["has_internet_service"] = df["InternetService"].map({
        "DSL": 1,
        "Fiber optic": 1,
        "No": 0
    }).fillna(0)  # ensure no leftover NaN

    # 4. is_multi_line_user
    df["is_multi_line_user"] = df["MultipleLines"].apply(lambda x: 1 if x == "Yes" else 0)

    # 5. contract_type_code
    df["contract_type_code"] = df["Contract"].map({
        "Month-to-month": 0,
        "One year": 1,
        "Two year": 2
    }).fillna(0)

    # ================================
    # 3️ DROP UNNECESSARY FIELDS
    # ================================
    df.drop(columns=["customerID", "gender"], inplace=True, errors="ignore")

    # Save transformed dataset
    staged_path = os.path.join(staged_dir, "churn_transformed.csv")
    df.to_csv(staged_path, index=False)

    print(f" Telecom data transformed and saved at: {staged_path}")
    return staged_path

if __name__ == "__main__":
    from extract import extract_data
    raw_path = extract_data()
    transform_telecom_data(raw_path)