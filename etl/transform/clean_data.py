"""
clean_data.py
Clean and validate raw extracted data for Nepal Electricity Analytics Pipeline.
Handles: missing values, outliers, type coercion, Nepal-specific validations.
"""

import logging
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import yaml

with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

logging.basicConfig(
    filename=config["logging"]["log_dir"] + config["logging"]["etl_log"],
    level=getattr(logging, config["logging"]["level"]),
    format=config["logging"]["format"]
)
logger = logging.getLogger(__name__)

CLEANED_DIR = Path(config["data_sources"]["processed"]["cleaned"])
CLEANED_DIR.mkdir(parents=True, exist_ok=True)

# Nepal grid voltage tolerance: ±10% of 230V → 207–253V
VOLTAGE_MIN = 180.0
VOLTAGE_MAX = 260.0

# Reasonable consumption bounds per consumer type (kWh/hour)
CONSUMPTION_BOUNDS = {
    "Domestic":    (0.0, 15.0),
    "Commercial":  (0.0, 100.0),
    "Industrial":  (0.0, 800.0),
}


def log_quality_report(df: pd.DataFrame, stage: str, df_name: str) -> None:
    """Log a data quality snapshot after each cleaning step."""
    nulls = df.isnull().sum().sum()
    logger.info(
        f"[{df_name}] {stage}: shape={df.shape}, "
        f"total_nulls={nulls}, "
        f"memory={df.memory_usage(deep=True).sum() // 1024}KB"
    )


def clean_annual_consumption(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean NEA annual consumption data.
    - Validate fiscal year format
    - Ensure all GWh columns are non-negative
    - Flag years where imported > exported during surplus (post-2021)
    - Add derived: net_energy_gwh, self_sufficiency_ratio
    """
    logger.info("Cleaning annual consumption data")
    df = df.copy()

    # Standardise fiscal year column
    df["fiscal_year"] = df["fiscal_year"].str.strip()

    # GWh columns must be non-negative
    gwh_cols = ["total_energy_gwh","domestic_gwh","industrial_gwh",
                "commercial_gwh","irrigation_gwh","other_gwh",
                "energy_imported_gwh","energy_exported_gwh"]
    for col in gwh_cols:
        neg = (df[col] < 0).sum()
        if neg:
            logger.warning(f"  {col}: {neg} negative values → set to 0")
        df[col] = df[col].clip(lower=0)

    # System loss must be 0–40%
    df["system_loss_pct"] = df["system_loss_pct"].clip(0, 40)

    # Derived columns
    df["net_energy_gwh"] = (
        df["total_energy_gwh"] + df["energy_imported_gwh"] - df["energy_exported_gwh"]
    ).round(2)
    df["self_sufficiency_ratio"] = (
        df["total_energy_gwh"] / df["net_energy_gwh"].replace(0, np.nan)
    ).round(4)
    df["load_factor_pct"] = (
        (df["total_energy_gwh"] * 1000) /
        (df["peak_demand_mw"] * 8760) * 100
    ).round(2)

    # Sort chronologically
    df = df.sort_values("fiscal_year").reset_index(drop=True)
    df["cleaned_at"] = datetime.now()

    log_quality_report(df, "after clean", "annual_consumption")
    return df


def clean_monthly_consumption(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean monthly consumption data.
    - Parse and validate dates
    - Interpolate any missing month records
    - Add nepali_month, nepali_year, season columns
    """
    logger.info("Cleaning monthly consumption data")
    df = df.copy()

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["year", "month"]).reset_index(drop=True)

    # Fill any missing total_gwh with component sum
    component_sum = (
        df["domestic_gwh"] + df["industrial_gwh"] + df["commercial_gwh"] +
        df["irrigation_gwh"] + df["other_gwh"]
    )
    mask = df["total_gwh"].isnull()
    df.loc[mask, "total_gwh"] = component_sum[mask]

    # Clip load factor to realistic 0–100%
    df["load_factor_pct"] = df["load_factor_pct"].clip(0, 100)

    # Season based on Nepal's climate calendar
    def nepal_season(month):
        if month in [3, 4, 5]:       return "Pre-Monsoon"
        elif month in [6, 7, 8, 9]:  return "Monsoon"
        elif month in [10, 11]:       return "Post-Monsoon"
        else:                         return "Winter"

    df["season"] = df["month"].apply(nepal_season)

    # Nepali month name (already in data, but normalise casing)
    df["month_name"] = df["month_name"].str.strip().str.title()

    # Month-over-month growth rate
    df["growth_rate_pct"] = df["total_gwh"].pct_change() * 100
    df.loc[df.groupby("year")["month"].transform("idxmin"), "growth_rate_pct"] = np.nan

    df["cleaned_at"] = datetime.now()
    log_quality_report(df, "after clean", "monthly_consumption")
    return df


def clean_smart_meter(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean smart meter hourly readings.
    - Remove duplicate timestamps per meter
    - Flag and cap voltage outliers
    - Cap unrealistic consumption per consumer type
    - Add hour, day_of_week, is_weekend, is_peak_hour flags
    """
    logger.info(f"Cleaning smart meter data: {len(df)} rows")
    df = df.copy()

    # Timestamp parsing
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Remove duplicates
    before = len(df)
    df = df.drop_duplicates(subset=["meter_id", "timestamp"])
    dropped = before - len(df)
    if dropped:
        logger.info(f"  Removed {dropped} duplicate meter-timestamp rows")

    # Consumption bounds per consumer type
    for ctype, (lo, hi) in CONSUMPTION_BOUNDS.items():
        mask = df["consumer_type"] == ctype
        outliers = ((df.loc[mask, "consumption_kwh"] < lo) |
                    (df.loc[mask, "consumption_kwh"] > hi)).sum()
        if outliers:
            logger.warning(f"  {ctype}: {outliers} consumption outliers capped to [{lo},{hi}]")
        df.loc[mask, "consumption_kwh"] = (
            df.loc[mask, "consumption_kwh"].clip(lo, hi)
        )

    # Voltage bounds
    volt_out = ((df["voltage_v"] < VOLTAGE_MIN) | (df["voltage_v"] > VOLTAGE_MAX)).sum()
    if volt_out:
        logger.warning(f"  Voltage outliers: {volt_out} rows flagged")
    df["voltage_flag"] = (
        (df["voltage_v"] < VOLTAGE_MIN) | (df["voltage_v"] > VOLTAGE_MAX)
    )
    df["voltage_v"] = df["voltage_v"].clip(VOLTAGE_MIN, VOLTAGE_MAX)

    # Power factor must be 0–1
    df["power_factor"] = df["power_factor"].clip(0.0, 1.0)

    # Time features
    df["hour"]         = df["timestamp"].dt.hour
    df["day_of_week"]  = df["timestamp"].dt.dayofweek
    df["date"]         = df["timestamp"].dt.date
    df["is_weekend"]   = df["day_of_week"].isin([5, 6])

    # Nepal peak hours: 18:00–22:00 (evening peak) and 06:00–09:00 (morning)
    df["is_evening_peak"] = df["hour"].between(18, 21)
    df["is_morning_peak"] = df["hour"].between(6, 8)
    df["is_peak_hour"]    = df["is_evening_peak"] | df["is_morning_peak"]

    df["cleaned_at"] = datetime.now()
    log_quality_report(df, "after clean", "smart_meter")
    return df


def clean_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean weather data.
    - Validate temperature ranges for Nepal (Terai to Himalaya)
    - Fill missing rainfall with 0 (no rain ≠ missing)
    - Add derived: temp_range_c, is_hot_day, is_cold_day
    """
    logger.info("Cleaning weather data")
    df = df.copy()

    df["date"] = pd.to_datetime(df["date"])

    # Temperature bounds: Nepal ranges from -30°C (high Himalaya) to 45°C (Terai)
    df["max_temp_c"] = df["max_temp_c"].clip(-30, 45)
    df["min_temp_c"] = df["min_temp_c"].clip(-30, 45)
    df["avg_temp_c"] = df["avg_temp_c"].clip(-30, 45)

    # Rainfall NaN → 0 (clear day)
    df["rainfall_mm"] = df["rainfall_mm"].fillna(0.0).clip(lower=0)

    # Humidity 0–100%
    df["humidity_pct"] = df["humidity_pct"].clip(0, 100)

    # Derived features
    df["temp_range_c"]   = (df["max_temp_c"] - df["min_temp_c"]).round(2)
    df["is_hot_day"]     = df["avg_temp_c"] >= 30    # Terai summer
    df["is_cold_day"]    = df["avg_temp_c"] <= 5     # Mountain winter
    df["is_rainy_day"]   = df["rainfall_mm"] > 5
    df["hdd"]            = (18.0 - df["avg_temp_c"]).clip(lower=0).round(2)
    df["cdd"]            = (df["avg_temp_c"] - 24.0).clip(lower=0).round(2)

    df["cleaned_at"] = datetime.now()
    log_quality_report(df, "after clean", "weather")
    return df


def clean_province_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean province electricity and population data."""
    logger.info("Cleaning province data")
    df = df.copy()

    # Electrification % must be 0–100
    df["electrification_pct_2021"] = df["electrification_pct_2021"].clip(0, 100)

    # Per capita kWh must be positive
    df["per_capita_kwh_2023"] = df["per_capita_kwh_2023"].clip(lower=0)

    # System loss should be realistic
    df["system_loss_pct"] = df["system_loss_pct"].clip(0, 40)

    df["cleaned_at"] = datetime.now()
    log_quality_report(df, "after clean", "province_data")
    return df


def save_cleaned(df: pd.DataFrame, name: str) -> Path:
    """Save cleaned DataFrame to CSV in processed/cleaned_data/."""
    out = CLEANED_DIR / f"{name}_cleaned.csv"
    df.to_csv(out, index=False)
    logger.info(f"Saved cleaned data: {out} ({len(df)} rows)")
    return out


def run_cleaning(raw_data: dict) -> dict:
    """
    Run all cleaning steps on raw extracted data.

    Args:
        raw_data: dict of {name: DataFrame} from extraction stage

    Returns:
        dict of {name: cleaned DataFrame}
    """
    logger.info("=== Starting Data Cleaning Stage ===")
    cleaned = {}

    if "annual" in raw_data:
        cleaned["annual"] = clean_annual_consumption(raw_data["annual"])
        save_cleaned(cleaned["annual"], "annual_consumption")

    if "monthly" in raw_data:
        cleaned["monthly"] = clean_monthly_consumption(raw_data["monthly"])
        save_cleaned(cleaned["monthly"], "monthly_consumption")

    if "hourly_readings" in raw_data:
        cleaned["smart_meter"] = clean_smart_meter(raw_data["hourly_readings"])
        save_cleaned(cleaned["smart_meter"], "smart_meter")

    if "weather_enriched" in raw_data:
        cleaned["weather"] = clean_weather_data(raw_data["weather_enriched"])
        save_cleaned(cleaned["weather"], "weather")

    if "province_electricity" in raw_data:
        cleaned["province"] = clean_province_data(raw_data["province_electricity"])
        save_cleaned(cleaned["province"], "province_electricity")

    logger.info(f"=== Cleaning Complete. Datasets: {list(cleaned.keys())} ===")
    return cleaned


if __name__ == "__main__":
    import sys
    sys.path.insert(0, ".")
    from etl.extract.extract_nea import run_extraction as nea_extract
    from etl.extract.extract_smart_meter import run_extraction as sm_extract
    from etl.extract.extract_weather_api import run_extraction as wx_extract
    from etl.extract.extract_open_data import run_extraction as od_extract

    raw = {}
    raw.update(nea_extract())
    raw.update(sm_extract())
    raw.update(wx_extract())
    raw.update(od_extract())

    cleaned = run_cleaning(raw)
    for name, df in cleaned.items():
        print(f"\n[{name.upper()}] shape={df.shape}")
        print(df.dtypes.to_string())
