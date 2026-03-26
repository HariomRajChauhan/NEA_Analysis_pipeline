"""
transform_data.py
Feature engineering, enrichment, and joins for Nepal Electricity Analytics Pipeline.
Produces analysis-ready datasets from cleaned data.
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

TRANSFORMED_DIR = Path(config["data_sources"]["processed"]["transformed"])
TRANSFORMED_DIR.mkdir(parents=True, exist_ok=True)

TARIFF = config["tariff"]


def calculate_revenue(df_monthly: pd.DataFrame, df_annual: pd.DataFrame) -> pd.DataFrame:
    """
    Estimate NEA revenue using tariff rates and consumption mix.
    Revenue = Σ(sector_gwh × avg_tariff_per_sector × 1e6)
    All amounts in NPR (Nepali Rupees).
    """
    logger.info("Computing revenue estimates from tariff rates")
    df = df_monthly.copy()

    # Weighted average tariffs by sector (NPR/kWh)
    df["domestic_revenue_npr"]    = df["domestic_gwh"]    * 1e6 * TARIFF["domestic_medium"]
    df["industrial_revenue_npr"]  = df["industrial_gwh"]  * 1e6 * TARIFF["industrial_ht_11kv"]
    df["commercial_revenue_npr"]  = df["commercial_gwh"]  * 1e6 * TARIFF["commercial"]
    df["irrigation_revenue_npr"]  = df["irrigation_gwh"]  * 1e6 * TARIFF["irrigation"]
    df["other_revenue_npr"]       = df["other_gwh"]       * 1e6 * TARIFF["institutional"]

    df["total_revenue_npr"] = (
        df["domestic_revenue_npr"] + df["industrial_revenue_npr"] +
        df["commercial_revenue_npr"] + df["irrigation_revenue_npr"] +
        df["other_revenue_npr"]
    )
    df["total_revenue_nrs_mn"] = (df["total_revenue_npr"] / 1e6).round(2)

    logger.info("Revenue calculation complete")
    return df


def add_fiscal_year_columns(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    """
    Add Nepali fiscal year columns.
    Nepal's fiscal year: Shrawan 1 (mid-July, ~July 17) to Ashadh end (mid-July next year).
    Fiscal year 2023/24 = July 17 2023 – July 16 2024.
    """
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])

    # Approx fiscal year: if month >= 7 and day >= 17 → FY starts that year
    def get_fy(dt):
        if (dt.month > 7) or (dt.month == 7 and dt.day >= 17):
            start = dt.year
        else:
            start = dt.year - 1
        return f"{start}/{str(start+1)[-2:]}"

    df["fiscal_year"] = df[date_col].apply(get_fy)
    df["fiscal_quarter"] = df[date_col].apply(lambda dt: (
        "Q1" if dt.month in [7,8,9] else
        "Q2" if dt.month in [10,11,12] else
        "Q3" if dt.month in [1,2,3] else "Q4"
    ))
    return df


def engineer_smart_meter_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Engineer features from hourly smart meter data for ML and analytics.
    Adds rolling stats, demand categories, load profile features.
    """
    logger.info("Engineering smart meter features")
    df = df.copy().sort_values(["meter_id", "timestamp"])

    # Rolling 24-hour and 7-day averages per meter
    df["rolling_24h_kwh"] = (
        df.groupby("meter_id")["consumption_kwh"]
        .transform(lambda x: x.rolling(24, min_periods=1).mean())
        .round(4)
    )
    df["rolling_7d_kwh"] = (
        df.groupby("meter_id")["consumption_kwh"]
        .transform(lambda x: x.rolling(24*7, min_periods=1).mean())
        .round(4)
    )

    # Demand category per reading
    def demand_category(row):
        pct = row["consumption_kwh"] / (row["rolling_24h_kwh"] + 1e-9)
        if pct >= 1.5:   return "High"
        elif pct >= 0.8: return "Normal"
        else:            return "Low"

    df["demand_category"] = df.apply(demand_category, axis=1)

    # Power quality flag
    df["low_power_factor"] = df["power_factor"] < 0.85
    df["voltage_dip"]      = df["voltage_v"] < 210

    # Consumption deviation from meter's daily average
    daily_avg = (
        df.groupby(["meter_id", "date"])["consumption_kwh"]
        .transform("mean")
    )
    df["deviation_from_daily_avg"] = (df["consumption_kwh"] - daily_avg).round(4)

    logger.info("Smart meter feature engineering complete")
    return df


def create_consumption_weather_features(
    df_monthly: pd.DataFrame,
    df_weather: pd.DataFrame
) -> pd.DataFrame:
    """
    Join monthly consumption with weather aggregates.
    Enables weather-demand correlation analysis.
    """
    logger.info("Creating consumption-weather joint features")

    # Aggregate weather to monthly level (Kathmandu as proxy for national)
    df_weather["date"] = pd.to_datetime(df_weather["date"])
    df_weather["year"]  = df_weather["date"].dt.year
    df_weather["month"] = df_weather["date"].dt.month

    ktm_weather = df_weather[df_weather["station_id"] == "KTM001"].copy()
    monthly_wx = (
        ktm_weather.groupby(["year","month"])
        .agg(
            avg_temp_c=("avg_temp_c", "mean"),
            max_temp_c=("max_temp_c", "max"),
            total_rainfall_mm=("rainfall_mm", "sum"),
            avg_humidity_pct=("humidity_pct", "mean"),
            total_hdd=("hdd", "sum"),
            total_cdd=("cdd", "sum"),
        )
        .reset_index()
    )

    df_monthly["year"]  = pd.to_datetime(df_monthly["date"]).dt.year
    df_monthly["month"] = pd.to_datetime(df_monthly["date"]).dt.month

    merged = df_monthly.merge(monthly_wx, on=["year","month"], how="left")

    # Weather-demand correlation features
    merged["temp_demand_index"] = (
        merged["total_cdd"] * 0.6 + merged["total_hdd"] * 0.4
    ).round(2)

    logger.info(f"Weather-consumption join: {len(merged)} rows")
    return merged


def create_province_enriched(
    df_province: pd.DataFrame,
    df_population: pd.DataFrame
) -> pd.DataFrame:
    """
    Enrich province electricity data with population and economic indicators.
    """
    logger.info("Enriching province data with population metrics")

    pop_cols = ["province_id","per_capita_income_usd","gdp_contribution_pct",
                "avg_household_size","urban_pct","literacy_rate_pct"]
    existing_pop_cols = [c for c in pop_cols if c in df_population.columns] if not df_population.empty else []

    if existing_pop_cols and len(existing_pop_cols) > 1:
        merged = df_province.merge(df_population[existing_pop_cols], on="province_id", how="left")
    else:
        merged = df_province.copy()

    # Derived ratios
    merged["kwh_per_household"] = (
        merged["annual_consumption_gwh_2023"] * 1e6 /
        merged["households_2021"].replace(0, np.nan)
    ).round(2)

    merged["electrification_gap_pct"] = (
        100 - merged["electrification_pct_2021"]
    ).round(2)

    merged["consumers_per_km2"] = (
        merged["electricity_consumers_2023"] /
        merged.get("area_sq_km", 1)
    ).round(4)

    # Revenue potential per province (NPR per year)
    merged["estimated_revenue_npr_mn"] = (
        merged["annual_consumption_gwh_2023"] * 1e6 *
        TARIFF["domestic_medium"] / 1e6
    ).round(2)

    logger.info(f"Province enrichment complete: {len(merged)} rows, {len(merged.columns)} columns")
    return merged


def create_hourly_system_load(df_sm: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate smart meter data to system-level hourly load profile.
    Scaled to national grid (extrapolated from sample).
    """
    logger.info("Creating system-level hourly load profile")

    hourly = (
        df_sm.groupby(["date","hour","consumer_type"])
        .agg(
            total_kwh=("consumption_kwh","sum"),
            meter_count=("meter_id","nunique"),
            avg_voltage=("voltage_v","mean"),
            avg_pf=("power_factor","mean"),
        )
        .reset_index()
    )

    # Scale factor: 100 sample meters → ~4.95M total consumers
    # Conservative extrapolation for illustrative purposes
    SCALE = 4_950_000 / 100
    hourly["scaled_mwh"] = (hourly["total_kwh"] * SCALE / 1000).round(2)

    hourly = add_fiscal_year_columns(hourly, "date")
    logger.info(f"System load profile: {len(hourly)} hourly records")
    return hourly


def save_transformed(df: pd.DataFrame, name: str) -> Path:
    out = TRANSFORMED_DIR / f"{name}_transformed.csv"
    df.to_csv(out, index=False)
    logger.info(f"Saved transformed: {out} ({len(df)} rows)")
    return out


def run_transformation(cleaned: dict) -> dict:
    """Run all transformation steps on cleaned data."""
    logger.info("=== Starting Transformation Stage ===")
    transformed = {}

    if "monthly" in cleaned:
        monthly_rev = calculate_revenue(cleaned["monthly"], cleaned.get("annual", pd.DataFrame()))
        monthly_rev = add_fiscal_year_columns(monthly_rev)
        transformed["monthly_revenue"] = monthly_rev
        save_transformed(monthly_rev, "monthly_revenue")

    if "annual" in cleaned:
        annual_with_date = cleaned["annual"].copy()
        # fiscal_year is like "2015/16"; extract start year for date
        annual_with_date["date"] = annual_with_date["fiscal_year"].str[:4].apply(
            lambda y: f"{y}-07-17"
        )
        annual_fy = add_fiscal_year_columns(annual_with_date)
        transformed["annual"] = annual_fy
        save_transformed(annual_fy, "annual")

    if "smart_meter" in cleaned:
        sm_features = engineer_smart_meter_features(cleaned["smart_meter"])
        transformed["smart_meter_features"] = sm_features
        save_transformed(sm_features, "smart_meter_features")

        hourly_load = create_hourly_system_load(sm_features)
        transformed["hourly_system_load"] = hourly_load
        save_transformed(hourly_load, "hourly_system_load")

    if "monthly" in cleaned and "weather" in cleaned:
        wx_consumption = create_consumption_weather_features(
            cleaned["monthly"], cleaned["weather"]
        )
        transformed["weather_consumption"] = wx_consumption
        save_transformed(wx_consumption, "weather_consumption")

    if "province" in cleaned:
        pop_df = cleaned.get("population", pd.DataFrame())
        province_enriched = create_province_enriched(cleaned["province"], pop_df)
        transformed["province_enriched"] = province_enriched
        save_transformed(province_enriched, "province_enriched")

    logger.info(f"=== Transformation Complete. Datasets: {list(transformed.keys())} ===")
    return transformed


if __name__ == "__main__":
    import sys
    sys.path.insert(0, ".")
    from etl.transform.clean_data import run_cleaning
    from etl.extract.extract_nea import run_extraction as nea_ex
    from etl.extract.extract_smart_meter import run_extraction as sm_ex
    from etl.extract.extract_weather_api import run_extraction as wx_ex
    from etl.extract.extract_open_data import run_extraction as od_ex

    raw = {}
    raw.update(nea_ex())
    raw.update(sm_ex())
    raw.update(wx_ex())
    raw.update(od_ex())

    cleaned = run_cleaning(raw)
    transformed = run_transformation(cleaned)
    for name, df in transformed.items():
        print(f"\n[{name.upper()}] shape={df.shape}")
        print(df.head(2).to_string())
