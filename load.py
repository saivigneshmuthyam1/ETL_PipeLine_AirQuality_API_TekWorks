# load.py
'''import os
import pandas as pd
import numpy as np
import math
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client
from time import sleep

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[0]
STAGED_DIR = BASE_DIR / "data" / "staged"

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# New table to match the new schema
TABLE_NAME = "atmos_measurements_v3"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Updated SQL Schema including new features
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS public.{TABLE_NAME} (
    id BIGSERIAL PRIMARY KEY,
    city TEXT,
    time TIMESTAMP,
    hour INTEGER,
    pm10 DOUBLE PRECISION,
    pm2_5 DOUBLE PRECISION,
    carbon_monoxide DOUBLE PRECISION,
    nitrogen_dioxide DOUBLE PRECISION,
    sulphur_dioxide DOUBLE PRECISION,
    ozone DOUBLE PRECISION,
    uv_index DOUBLE PRECISION,
    aqi_category TEXT,
    severity_score DOUBLE PRECISION,
    risk_classification TEXT
);
"""

def create_table_if_not_exists():
    try:
        print(f"🔧 Creating table '{TABLE_NAME}'...")
        supabase.rpc("execute_sql", {"query": CREATE_TABLE_SQL}).execute()
        print(f"✅ Table '{TABLE_NAME}' ready.")
    except Exception as e:
        print(f"⚠️ RPC Error: {e}")
        print("ℹ️  Run the SQL manually in Supabase if needed.")
        print(CREATE_TABLE_SQL)

def clean_record_for_json(record):
    """Sanitizes record for JSON compliance (No NaN/Inf)."""
    cleaned = {}
    for key, value in record.items():
        if pd.isna(value):
            cleaned[key] = None
        elif isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            cleaned[key] = None
        elif key == "time":
            cleaned[key] = str(value) if str(value).lower() != "nan" else None
        else:
            cleaned[key] = value
    return cleaned

def load_to_supabase(csv_path):
    if not csv_path or not Path(csv_path).exists():
        print(f"⚠️ File missing: {csv_path}")
        return

    print(f"📦 Loading {Path(csv_path).name} to '{TABLE_NAME}'...")
    df = pd.read_csv(csv_path)

    raw_records = df.to_dict(orient="records")
    cleaned_records = [clean_record_for_json(rec) for rec in raw_records]
    
    total = len(cleaned_records)
    for i in range(0, total, 500):
        batch = cleaned_records[i:i+500]
        try:
            supabase.table(TABLE_NAME).insert(batch).execute()
            print(f"   > Inserted rows {i+1}-{min(i+500, total)}")
        except Exception as e:
            print(f"❌ Error inserting batch: {e}")
            sleep(1)

    print("🎯 Load Complete.")

if __name__ == "__main__":
    staged_files = sorted([str(p) for p in STAGED_DIR.glob("air_quality_transform_*.csv")])
    if staged_files:
        create_table_if_not_exists()
        load_to_supabase(staged_files[-1]'''

# load.py
import os
import math
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client
from time import sleep

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[0]
STAGED_DIR = BASE_DIR / "data" / "staged"

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = "air_quality_data"

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Please check .env for SUPABASE_URL and SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Exact Schema requested
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS public.{TABLE_NAME} (
    id BIGSERIAL PRIMARY KEY,
    city TEXT,
    time TIMESTAMP,
    pm10 DOUBLE PRECISION,
    pm2_5 DOUBLE PRECISION,
    carbon_monoxide DOUBLE PRECISION,
    nitrogen_dioxide DOUBLE PRECISION,
    sulphur_dioxide DOUBLE PRECISION,
    ozone DOUBLE PRECISION,
    uv_index DOUBLE PRECISION,
    aqi_category TEXT,
    severity_score DOUBLE PRECISION,
    risk_flag TEXT,
    hour INTEGER
);
"""

def create_table_if_not_exists():
    try:
        print(f"🔧 Creating table '{TABLE_NAME}'...")
        supabase.rpc("execute_sql", {"query": CREATE_TABLE_SQL}).execute()
        print(f"✅ Table '{TABLE_NAME}' ready.")
    except Exception as e:
        print(f"⚠️ RPC Error: {e}")
        print("ℹ️  Please run the SQL manually in Supabase Dashboard.")
        print(CREATE_TABLE_SQL)

def clean_record_for_json(record):
    """
    Sanitizes record to ensure JSON compliance (NaN -> None).
    Also handles column renaming if transform output differed slightly.
    """
    cleaned = {}
    
    # Map 'risk_classification' from transform to 'risk_flag' for load
    if "risk_classification" in record:
        record["risk_flag"] = record.pop("risk_classification")

    for key, value in record.items():
        # Handle NaN/Inf
        if pd.isna(value):
            cleaned[key] = None
        elif isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            cleaned[key] = None
        # Handle Time
        elif key == "time":
            cleaned[key] = str(value) if str(value).lower() != "nan" else None
        else:
            cleaned[key] = value
    return cleaned

def load_to_supabase(csv_path):
    if not csv_path or not Path(csv_path).exists():
        print(f"⚠️ File missing: {csv_path}")
        return

    print(f"📦 Loading {Path(csv_path).name} to '{TABLE_NAME}'...")
    df = pd.read_csv(csv_path)

    # Convert to dictionary records
    raw_records = df.to_dict(orient="records")
    
    # Clean records
    cleaned_records = [clean_record_for_json(rec) for rec in raw_records]
    total = len(cleaned_records)
    
    # Batch Insert (Size = 200)
    BATCH_SIZE = 200
    
    for i in range(0, total, BATCH_SIZE):
        batch = cleaned_records[i:i+BATCH_SIZE]
        
        # Retry Logic (2 Retries)
        max_retries = 2
        for attempt in range(max_retries + 1):
            try:
                supabase.table(TABLE_NAME).insert(batch).execute()
                print(f"   > Inserted rows {i+1}-{min(i+BATCH_SIZE, total)}")
                break # Success, exit retry loop
            except Exception as e:
                print(f"   ⚠️ Batch failed (Attempt {attempt+1}/{max_retries+1}): {e}")
                if attempt < max_retries:
                    sleep(2) # Wait before retry
                else:
                    print(f"   ❌ Skipping batch starting at row {i+1} due to repeated errors.")

    print(f"🎯 Load Complete. Processed {total} rows.")

if __name__ == "__main__":
    staged_files = sorted([str(p) for p in STAGED_DIR.glob("air_quality_transform_*.csv")])
    if staged_files:
        create_table_if_not_exists()
        load_to_supabase(staged_files[-1])