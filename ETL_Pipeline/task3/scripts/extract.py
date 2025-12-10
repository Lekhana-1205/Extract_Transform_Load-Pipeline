import os
import pandas as pd
import shutil
def extract_data():
    # Get project root directory
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # Define folder paths
    raw_dir = os.path.join(base_dir, "data", "raw")
    staged_dir = os.path.join(base_dir, "data", "staged")
    processed_dir = os.path.join(base_dir, "data", "processed")
    # Create folders
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(staged_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    print("Folders ensured (raw/staged/processed).")
    local_csv_path = r"C:\Users\lekha\OneDrive\Documents\Tekworks\10-12-25\task2\WA_Fn-UseC_-Telco-Customer-Churn.csv"  
    if not os.path.exists(local_csv_path):
        raise FileNotFoundError("Local CSV file not found. Check the path again.")
    # Save file into ETL raw folder
    raw_path = os.path.join(raw_dir, "churn_raw.csv")
    shutil.copy(local_csv_path, raw_path)
    print(f"Raw dataset copied to: {raw_path}")
    return raw_path
if __name__ == "__main__":
    extract_data()