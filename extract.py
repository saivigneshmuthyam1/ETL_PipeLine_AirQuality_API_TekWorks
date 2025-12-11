import json
import time
import requests
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[0]
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Open-Meteo API Endpoint (No Key Required)
API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

# City Coordinates for India
CITIES = {
    "Delhi":     {"lat": 28.7041, "lon": 77.1025},
    "Mumbai":    {"lat": 19.0760, "lon": 72.8777},
    "Bengaluru": {"lat": 12.9716, "lon": 77.5946},
    "Hyderabad": {"lat": 17.3850, "lon": 78.4867},
    "Kolkata":   {"lat": 22.5726, "lon": 88.3639}
}

METRICS = "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide,uv_index"

def fetch_data(city, lat, lon):
    params = {
        "latitude": lat, "longitude": lon,
        "hourly": METRICS, "timezone": "auto"
    }
    try:
        print(f"⏳ Fetching data for {city}...")
        resp = requests.get(API_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        data["city_name"] = city  # Tag the data with city name
        return data
    except Exception as e:
        print(f"⚠️ Error fetching {city}: {e}")
        return None

def extract_atmos_data():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    saved_files = []

    print("🚀 Starting Extraction...")
    for city, coords in CITIES.items():
        data = fetch_data(city, coords["lat"], coords["lon"])
        if data:
            filename = RAW_DIR / f"{city.lower()}_raw_{timestamp}.json"
            filename.write_text(json.dumps(data, indent=2))
            saved_files.append(str(filename))
            print(f"✅ Saved: {filename}")
        time.sleep(1)
    
    return saved_files

if __name__ == "__main__":
    extract_atmos_data()