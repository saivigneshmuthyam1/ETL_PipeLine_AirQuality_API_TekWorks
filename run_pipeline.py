import time
import sys
from extract import extract_atmos_data
from transform import transform_data
from load import load_to_supabase
from etl_analysis import run_analysis

def run_full_pipeline():
    print("===================================================")
    print("🌍 STARTING ATMOSTRACK ETL PIPELINE")
    print("===================================================")

    # ---------------------------------------------------------
    # STEP 1: EXTRACT
    # ---------------------------------------------------------
    print("\n[1/4] 🚀 Extracting Data from Open-Meteo API...")
    try:
        raw_files = extract_atmos_data()
        if not raw_files:
            print("❌ Extraction failed or returned no files. Stopping.")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Critical Error in Extract: {e}")
        sys.exit(1)
    
    time.sleep(1) # Pause to ensure file system sync

    # ---------------------------------------------------------
    # STEP 2: TRANSFORM
    # ---------------------------------------------------------
    print("\n[2/4] 🔁 Transforming JSON to CSV...")
    try:
        staged_csv = transform_data(raw_files)
        if not staged_csv:
            print("❌ Transformation failed. Stopping.")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Critical Error in Transform: {e}")
        sys.exit(1)

    time.sleep(1)

    # ---------------------------------------------------------
    # STEP 3: LOAD
    # ---------------------------------------------------------
    print("\n[3/4] 📦 Loading Data to Supabase Table 'air_quality_data'...")
    try:
        # Note: We assume create_table_if_not_exists is handled manually 
        # or inside load_to_supabase if you added it there.
        load_to_supabase(staged_csv)
    except Exception as e:
        print(f"❌ Critical Error in Load: {e}")
        sys.exit(1)

    time.sleep(2) # Give DB a moment to index

    # ---------------------------------------------------------
    # STEP 4: ANALYSIS
    # ---------------------------------------------------------
    print("\n[4/4] 📊 Running Analysis & Generating Reports...")
    try:
        run_analysis()
    except Exception as e:
        print(f"❌ Critical Error in Analysis: {e}")
        sys.exit(1)

    print("\n===================================================")
    print("✅ PIPELINE COMPLETED SUCCESSFULLY")
    print("===================================================")

if __name__ == "__main__":
    run_full_pipeline()