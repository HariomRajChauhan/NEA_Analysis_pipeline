"""
extract_weather_api.py
Extract weather data for Nepal's major weather stations.
Primary source: OpenWeatherMap API
Fallback: Department of Hydrology and Meteorology (DHM), Nepal
Local backup: data/raw/weather_api/weather_data.csv

Weather is critical for electricity demand correlation:
  - Summer heat (Jestha/Ashadh) → AC/fan load spikes
  - Monsoon → hydropower availability peak, lower demand growth
  - Winter (Poush/Magh) → heating load, import dependency
"""

import logging
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import yaml
import time
import os

with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

logging.basicConfig(
    filename=config["logging"]["log_dir"] + config["logging"]["etl_log"],
    level=getattr(logging, config["logging"]["level"]),
    format=config["logging"]["format"]
)
logger = logging.getLogger(__name__)

WEATHER_DIR = Path(config["data_sources"]["raw"]["weather_api"])
STATIONS = config["apis"]["weather"]["stations"]
API_KEY = os.getenv("WEATHER_API_KEY", config["apis"]["weather"].get("api_key", ""))
BASE_URL = config["apis"]["weather"]["base_url"]


def fetch_current_weather(station: dict) -> dict | None:
    """
    Fetch current weather for a single station via OpenWeatherMap API.

    Args:
        station: dict with keys: id, city, lat, lon

    Returns:
        Parsed weather record or None on failure
    """
    if not API_KEY or API_KEY.startswith("${"):
        logger.warning("WEATHER_API_KEY not configured, using local fallback")
        return None

    url = f"{BASE_URL}weather"
    params = {
        "lat": station["lat"],
        "lon": station["lon"],
        "appid": API_KEY,
        "units": "metric",
    }

    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        return {
            "date": datetime.now().date().isoformat(),
            "station_id": station["id"],
            "station_name": station["city"],
            "avg_temp_c": data["main"]["temp"],
            "max_temp_c": data["main"]["temp_max"],
            "min_temp_c": data["main"]["temp_min"],
            "feels_like_c": data["main"]["feels_like"],
            "humidity_pct": data["main"]["humidity"],
            "wind_speed_kmh": round(data["wind"]["speed"] * 3.6, 1),
            "cloud_cover_pct": data["clouds"]["all"],
            "rainfall_mm": data.get("rain", {}).get("1h", 0.0),
            "weather_desc": data["weather"][0]["description"],
            "fetched_at": datetime.now().isoformat(),
        }
    except requests.RequestException as e:
        logger.error(f"Weather API error for {station['city']}: {e}")
        return None


def fetch_all_stations_current() -> pd.DataFrame:
    """Fetch current weather for all configured Nepal stations."""
    records = []
    for station in STATIONS:
        record = fetch_current_weather(station)
        if record:
            records.append(record)
        time.sleep(0.5)  # Respect API rate limits

    if records:
        df = pd.DataFrame(records)
        logger.info(f"Fetched current weather for {len(df)} stations")
        return df

    logger.warning("Live API unavailable, loading local weather backup")
    return load_local_weather()


def load_local_weather(
    start_date: str = None,
    end_date: str = None
) -> pd.DataFrame:
    """
    Load weather data from local CSV backup.

    Args:
        start_date: 'YYYY-MM-DD' filter start
        end_date: 'YYYY-MM-DD' filter end

    Returns:
        Weather DataFrame with columns matching fetched records
    """
    file_path = WEATHER_DIR / "weather_data.csv"
    logger.info(f"Loading local weather data from {file_path}")

    df = pd.read_csv(file_path, parse_dates=["date"])

    if start_date:
        df = df[df["date"] >= start_date]
    if end_date:
        df = df[df["date"] <= end_date]

    logger.info(f"Loaded {len(df)} weather records from {df['date'].min()} to {df['date'].max()}")
    return df


def calculate_heating_cooling_degree_days(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Heating Degree Days (HDD) and Cooling Degree Days (CDD).
    These are direct proxies for electricity demand from HVAC.

    Base temperatures commonly used for Nepal:
      HDD base: 18°C (heating needed below this)
      CDD base: 24°C (cooling needed above this)
    """
    HDD_BASE = 18.0
    CDD_BASE = 24.0

    df = df.copy()
    df["hdd"] = (HDD_BASE - df["avg_temp_c"]).clip(lower=0).round(2)
    df["cdd"] = (df["avg_temp_c"] - CDD_BASE).clip(lower=0).round(2)

    # Humidity discomfort index (Nepal's humid Terai summers)
    # THI = T - 0.55*(1 - RH/100)*(T - 14.5)
    T = df["avg_temp_c"]
    RH = df["humidity_pct"]
    df["thermal_discomfort_index"] = (
        T - 0.55 * (1 - RH / 100) * (T - 14.5)
    ).round(2)

    logger.info("Calculated HDD, CDD, and thermal discomfort index")
    return df


def enrich_with_monsoon_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add Nepal monsoon season flags.
    Nepal monsoon: June–September (approx months 6–9)
    Pre-monsoon: March–May (high demand for cooling)
    Dry season: October–February (low hydro, potential import)
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    month = df["date"].dt.month

    df["season"] = pd.cut(
        month,
        bins=[0, 2, 5, 9, 12],
        labels=["Winter", "Pre-Monsoon", "Monsoon", "Post-Monsoon"],
        ordered=False
    )
    df["is_monsoon"] = month.between(6, 9)
    df["is_dry_season"] = ~df["is_monsoon"]

    logger.info("Added monsoon season flags")
    return df


def run_extraction() -> dict:
    """Run all weather extraction tasks."""
    logger.info("=== Starting Weather Data Extraction ===")

    raw_weather = load_local_weather()
    enriched = calculate_heating_cooling_degree_days(raw_weather)
    enriched = enrich_with_monsoon_flags(enriched)

    results = {
        "weather_raw": raw_weather,
        "weather_enriched": enriched,
    }

    logger.info("=== Weather Extraction Complete ===")
    return results


if __name__ == "__main__":
    data = run_extraction()
    for name, df in data.items():
        print(f"\n[{name.upper()}] shape={df.shape}")
        print(df.head(3).to_string())
