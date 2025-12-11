import os
import subprocess
import datetime
import sys

# ---------------------------------------------------------
# Helper function to run a script and show live output
# ---------------------------------------------------------
def run_step(title, command):
    print("\n" + "=" * 60)
    print(f"▶️  {title}")
    print("=" * 60)

    try:
        # Run and stream output live
        result = subprocess.run(
            [sys.executable, command],
            capture_output=False,
            check=True
        )
        print(f"✅ {title} completed\n")

    except subprocess.CalledProcessError as e:
        print(f"❌ {title} failed with error code {e.returncode}")
        print("Stopping pipeline...\n")
        sys.exit(1)


# ---------------------------------------------------------
# MAIN PIPELINE EXECUTION
# ---------------------------------------------------------
if __name__ == "__main__":
    start_time = datetime.datetime.now()
    print("\n==========================================================")
    print("   🌍 AtmosTrack — Full ETL Pipeline Runner")
    print("==========================================================")
    print(f"Run started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    # 1. Extract
    run_step("STEP 1 — Extract (extract.py)", "extract.py")

    # 2. Transform
    run_step("STEP 2 — Transform (transform.py)", "transform.py")

    # 3. Load
    run_step("STEP 3 — Load to Supabase (load.py)", "load.py")

    # 4. Analysis
    run_step("STEP 4 — Analysis & Reports (etl_analysis.py)", "etl_analysis.py")

    end_time = datetime.datetime.now()
    duration = end_time - start_time

    print("==========================================================")
    print("   🎉 FULL ATMOSTRACK ETL PIPELINE COMPLETED SUCCESSFULLY")
    print("==========================================================")
    print(f"Started   : {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Finished  : {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration  : {duration}")
    print("\nAll reports, plots, processed CSVs, and Supabase inserts done!")
