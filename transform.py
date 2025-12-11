# transform.py
import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parents[0]
STAGED_DIR = BASE_DIR / "data" / "staged"
STAGED_DIR.mkdir(parents=True, exist_ok=True)

def calculate_aqi_category(pm25):
    """Classifies AQI based on PM2.5 levels."""
    if pd.isna(pm25): return None
    if pm25 <= 50: return "Good"
    if pm25 <= 100: return "Moderate"
    if pm25 <= 200: return "Unhealthy"
    if pm25 <= 300: return "Very Unhealthy"
    return "Hazardous"

def calculate_risk(score):
    """Classifies risk based on Severity Score."""
    if pd.isna(score): return "Low Risk"
    if score > 400: return "High Risk"
    if score > 200: return "Moderate Risk"
    return "Low Risk"

def transform_data(raw_files):
    print("🔁 Starting Transformation...")
    dfs = []
    
    for path in raw_files:
        try:
            with open(path, "r") as f:
                payload = json.load(f)
            
            # Open-Meteo structure: {'hourly': {'time': [], 'pm10': []...}}
            hourly_data = payload.get("hourly", {})
            if not hourly_data: continue
            
            df = pd.DataFrame(hourly_data)
            df["city"] = payload.get("city_name", "Unknown")
            dfs.append(df)
        except Exception as e:
            print(f"⚠️ Failed to process {path}: {e}")

    if not dfs:
        print("❌ No data to transform.")
        return None

    # Merge all cities
    df_combined = pd.concat(dfs, ignore_index=True)

    # --- A. Data Cleaning & Renaming ---
    # Convert 'time' to datetime
    if "time" in df_combined.columns:
        df_combined["time"] = pd.to_datetime(df_combined["time"])
    
    # Ensure all pollutant columns are numeric
    pollutants = ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone", "uv_index"]
    for col in pollutants:
        if col in df_combined.columns:
            # Replace dots with underscores in col names just in case (api returns pm2_5 usually)
            df_combined[col] = pd.to_numeric(df_combined[col], errors="coerce")

    # Remove records where ALL pollutant readings are missing
    df_combined = df_combined.dropna(subset=pollutants, how="all")

    # --- B. Feature Engineering ---
    
    # 1. AQI based on PM2.5
    df_combined["aqi_category"] = df_combined["pm2_5"].apply(calculate_aqi_category)

    # 2. Pollution Severity Score
    # Formula: (pm2_5 * 5) + (pm10 * 3) + (no2 * 4) + (so2 * 4) + (co * 2) + (o3 * 3)
    df_combined["severity_score"] = (
        (df_combined["pm2_5"] * 5) +
        (df_combined["pm10"] * 3) +
        (df_combined["nitrogen_dioxide"] * 4) +
        (df_combined["sulphur_dioxide"] * 4) +
        (df_combined["carbon_monoxide"] * 2) +
        (df_combined["ozone"] * 3)
    )

    # 3. Risk Classification
    df_combined["risk_classification"] = df_combined["severity_score"].apply(calculate_risk)

    # 4. Temperature Hour-of-Day Feature
    df_combined["hour"] = df_combined["time"].dt.hour

    # --- C. Save Staged Data ---
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    staged_path = STAGED_DIR / f"air_quality_transform_{timestamp}.csv"
    
    # Select specific columns to keep it clean (reordering)
    final_cols = ["city", "time", "hour"] + pollutants + ["aqi_category", "severity_score", "risk_classification"]
    # Filter only existing columns
    final_cols = [c for c in final_cols if c in df_combined.columns]
    
    df_combined[final_cols].to_csv(staged_path, index=False)
    print(f"✅ Transformed data saved: {staged_path}")
    return str(staged_path)

if __name__ == "__main__":
    # For testing, you can pass a dummy list or run via run_pipeline.py
    print("Run via run_pipeline.py")