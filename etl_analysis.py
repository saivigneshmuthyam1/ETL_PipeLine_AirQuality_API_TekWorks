# etl_analysis.py
'''import os
import pandas as pd
import matplotlib.pyplot as plt
from supabase import create_client
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[0]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = "air_quality_metrics"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def run_analysis():
    print("🔍 Fetching data for analysis...")
    
    # Fetch data (Limit 1000 for quick analysis)
    res = supabase.table(TABLE_NAME).select("*").order("id", desc=True).limit(2000).execute()
    data = res.data
    
    if not data:
        print("⚠️ No data found in database.")
        return

    df = pd.DataFrame(data)
    df["value"] = pd.to_numeric(df["value"])

    print("📊 Generating Insights...")

    # Insight 1: Average PM2.5 per City
    pm25_df = df[df["parameter"] == "pm25"]
    
    if not pm25_df.empty:
        avg_pm25 = pm25_df.groupby("city")["value"].mean().sort_values(ascending=False)
        print("\n🏙️  Average PM2.5 Levels (µg/m³) by City:")
        print(avg_pm25)
        
        # Save CSV
        avg_pm25.to_csv(PROCESSED_DIR / "avg_pm25_by_city.csv")

        # Plot
        plt.figure(figsize=(10, 6))
        avg_pm25.plot(kind="bar", color="coral", edgecolor="black")
        plt.title("Average PM2.5 Levels by City")
        plt.ylabel("PM2.5 (µg/m³)")
        plt.xlabel("City")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(PROCESSED_DIR / "pm25_analysis.png")
        print(f"✅ Saved chart to {PROCESSED_DIR / 'pm25_analysis.png'}")
    else:
        print("⚠️ No PM2.5 data found to analyze.")

if __name__ == "__main__":
    run_analysis()'''

# etl_analysis.py
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv
from supabase import create_client
from pathlib import Path

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[0]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = "air_quality_data"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def run_analysis():
    print(f"🔍 Fetching data from '{TABLE_NAME}'...")
    
    # Fetch all data (adjust limit if dataset grows huge)
    res = supabase.table(TABLE_NAME).select("*").limit(5000).execute()
    
    if not res.data:
        print("⚠️ No data found in database.")
        return

    df = pd.DataFrame(res.data)
    
    # Ensure numeric types
    numeric_cols = ["pm2_5", "pm10", "ozone", "severity_score", "hour"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    print("📊 Performing Analytics...")

    # --- A. KPI Metrics ---
    metrics = {}
    
    # 1. City with highest average PM2.5
    avg_pm25 = df.groupby("city")["pm2_5"].mean()
    metrics["Worst City (Avg PM2.5)"] = avg_pm25.idxmax()
    metrics["Worst City PM2.5 Value"] = avg_pm25.max()
    
    # 2. City with highest severity score (Max single event)
    max_sev_idx = df["severity_score"].idxmax()
    metrics["Highest Severity Event City"] = df.loc[max_sev_idx, "city"]
    metrics["Highest Severity Score"] = df.loc[max_sev_idx, "severity_score"]
    
    # 3. Hour of day with worst AQI (Avg PM2.5)
    worst_hour = df.groupby("hour")["pm2_5"].mean().idxmax()
    metrics["Worst Hour of Day"] = worst_hour
    
    # 4. Risk Percentages (Global)
    risk_counts = df["risk_flag"].value_counts(normalize=True) * 100
    metrics["High Risk %"] = risk_counts.get("High Risk", 0)
    metrics["Moderate Risk %"] = risk_counts.get("Moderate Risk", 0)
    
    # Save Metrics Summary
    metrics_df = pd.DataFrame([metrics])
    metrics_df.to_csv(PROCESSED_DIR / "summary_metrics.csv", index=False)
    print(f"✅ Metrics saved to {PROCESSED_DIR / 'summary_metrics.csv'}")

    # --- B. City Pollution Trend Report ---
    # Filter columns
    trend_cols = ["city", "time", "pm2_5", "pm10", "ozone"]
    trend_df = df[[c for c in trend_cols if c in df.columns]]
    trend_df.to_csv(PROCESSED_DIR / "pollution_trends.csv", index=False)
    print(f"✅ Trends saved to {PROCESSED_DIR / 'pollution_trends.csv'}")

    # --- C. Export Outputs (Risk Distribution) ---
    risk_dist = df.groupby(["city", "risk_flag"]).size().unstack(fill_value=0)
    risk_dist.to_csv(PROCESSED_DIR / "city_risk_distribution.csv")
    print(f"✅ Risk dist saved to {PROCESSED_DIR / 'city_risk_distribution.csv'}")

    # --- D. Visualizations ---
    
    # 1. Histogram of PM2.5
    plt.figure(figsize=(8, 5))
    sns.histplot(df["pm2_5"], bins=30, kde=True, color="purple")
    plt.title("Distribution of PM2.5 Concentration")
    plt.xlabel("PM2.5 (µg/m³)")
    plt.savefig(PROCESSED_DIR / "pm25_histogram.png")
    plt.close()

    # 2. Bar chart of risk flags per city
    plt.figure(figsize=(10, 6))
    risk_dist.plot(kind="bar", stacked=True, colormap="viridis", figsize=(10, 6))
    plt.title("Risk Level Distribution by City")
    plt.ylabel("Count of Hours")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(PROCESSED_DIR / "city_risk_bar.png")
    plt.close()

    # 3. Line chart of hourly PM2.5 trends (Average across all days per hour)
    plt.figure(figsize=(10, 5))
    sns.lineplot(data=df, x="hour", y="pm2_5", hue="city", marker="o")
    plt.title("Hourly Average PM2.5 Trends")
    plt.ylabel("PM2.5 (µg/m³)")
    plt.xlabel("Hour of Day (0-23)")
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.savefig(PROCESSED_DIR / "hourly_pm25_trend.png")
    plt.close()

    # 4. Scatter: Severity Score vs PM2.5
    plt.figure(figsize=(8, 5))
    sns.scatterplot(data=df, x="pm2_5", y="severity_score", hue="risk_flag", alpha=0.7)
    plt.title("Severity Score vs PM2.5")
    plt.grid(True)
    plt.savefig(PROCESSED_DIR / "severity_scatter.png")
    plt.close()
    
    print("✅ All plots generated.")

if __name__ == "__main__":
    run_analysis()