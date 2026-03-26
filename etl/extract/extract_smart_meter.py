"""
extract_smart_meter.py
Extract and ingest smart meter AMR/AMI data for NEA consumers.
Handles hourly readings from 100+ smart meters across Nepal.
Data format: timestamp, meter_id, consumer_type, consumption_kwh,
             voltage_v, power_factor, apparent_power_kva
"""

import logging
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import yaml

with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

logging.basicConfig(
    filename=config["logging"]["log_dir"] + config["logging"]["etl_log"],
    level=getattr(logging, config["logging"]["level"]),
    format=config["logging"]["format"]
)
logger = logging.getLogger(__name__)

RAW_SM_DIR = Path(config["data_sources"]["raw"]["smart_meter"])
CHUNK_SIZE = config["etl"]["smart_meter_chunk_size"]


def extract_hourly_readings(
    file_path: Path = None,
    start_date: str = None,
    end_date: str = None
) -> pd.DataFrame:
    """
    Extract hourly smart meter readings.

    Args:
        file_path: Path to smart_meter_hourly.csv (defaults to config path)
        start_date: Filter from date 'YYYY-MM-DD'
        end_date: Filter to date 'YYYY-MM-DD'

    Returns:
        DataFrame with columns:
            timestamp, meter_id, consumer_type, consumption_kwh,
            voltage_v, power_factor, apparent_power_kva
    """
    if file_path is None:
        file_path = RAW_SM_DIR / "smart_meter_hourly.csv"

    logger.info(f"Extracting smart meter data from {file_path}")

    dtype_map = {
        "meter_id": "str",
        "consumer_type": "category",
        "consumption_kwh": "float32",
        "voltage_v": "float32",
        "power_factor": "float32",
        "apparent_power_kva": "float32",
    }

    chunks = []
    for chunk in pd.read_csv(file_path, dtype=dtype_map, chunksize=CHUNK_SIZE,
                              parse_dates=["timestamp"]):
        if start_date:
            chunk = chunk[chunk["timestamp"] >= start_date]
        if end_date:
            chunk = chunk[chunk["timestamp"] <= end_date]
        chunks.append(chunk)

    if not chunks:
        logger.warning("No smart meter data found for given date range")
        return pd.DataFrame()

    df = pd.concat(chunks, ignore_index=True)
    df["extracted_at"] = datetime.now()

    # Basic stats log
    logger.info(
        f"Smart meter extract: {len(df)} rows, "
        f"{df['meter_id'].nunique()} meters, "
        f"date range {df['timestamp'].min()} to {df['timestamp'].max()}"
    )
    return df


def extract_meter_registry() -> pd.DataFrame:
    """
    Return metadata about registered smart meters.
    In production this would query the meter management system (MMS).
    """
    registry_path = RAW_SM_DIR / "meter_registry.csv"
    if registry_path.exists():
        return pd.read_csv(registry_path)

    # Generate from smart meter data if registry not available
    sm_data = extract_hourly_readings()
    registry = (
        sm_data.groupby(["meter_id", "consumer_type"])
        .agg(
            first_reading=("timestamp", "min"),
            last_reading=("timestamp", "max"),
            avg_consumption_kwh=("consumption_kwh", "mean"),
            avg_voltage=("voltage_v", "mean"),
            avg_power_factor=("power_factor", "mean"),
        )
        .reset_index()
    )
    registry["extracted_at"] = datetime.now()
    logger.info(f"Generated meter registry for {len(registry)} meters")
    return registry


def extract_outage_events(df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Detect outage events from smart meter readings.
    Outage: consumption_kwh == 0 or voltage_v < 180 for 2+ consecutive hours.

    Args:
        df: Smart meter DataFrame (loads from file if None)

    Returns:
        DataFrame of detected outage events with meter_id, start, end, duration_h
    """
    if df is None:
        df = extract_hourly_readings()

    df = df.sort_values(["meter_id", "timestamp"])
    df["is_outage"] = (df["consumption_kwh"] == 0) | (df["voltage_v"] < 180)

    outages = []
    for meter_id, group in df.groupby("meter_id"):
        group = group.reset_index(drop=True)
        in_outage = False
        outage_start = None

        for i, row in group.iterrows():
            if row["is_outage"] and not in_outage:
                in_outage = True
                outage_start = row["timestamp"]
            elif not row["is_outage"] and in_outage:
                duration = (row["timestamp"] - outage_start).total_seconds() / 3600
                if duration >= 2:  # Only record outages >= 2 hours
                    outages.append({
                        "meter_id": meter_id,
                        "consumer_type": row["consumer_type"],
                        "outage_start": outage_start,
                        "outage_end": row["timestamp"],
                        "duration_hours": round(duration, 2),
                    })
                in_outage = False

    outage_df = pd.DataFrame(outages)
    logger.info(f"Detected {len(outage_df)} outage events across all meters")
    return outage_df


def extract_demand_peaks(df: pd.DataFrame = None, top_n: int = 100) -> pd.DataFrame:
    """
    Extract peak demand records per meter.

    Args:
        df: Smart meter DataFrame
        top_n: Number of top peak records per meter

    Returns:
        DataFrame with peak demand timestamps and values per meter
    """
    if df is None:
        df = extract_hourly_readings()

    peaks = (
        df.sort_values("consumption_kwh", ascending=False)
        .groupby("meter_id")
        .head(top_n)
        [["meter_id", "consumer_type", "timestamp", "consumption_kwh", "voltage_v", "power_factor"]]
        .rename(columns={"consumption_kwh": "peak_consumption_kwh"})
    )
    logger.info(f"Extracted peak demand records: {len(peaks)} rows")
    return peaks


def run_extraction() -> dict:
    """Run all smart meter extraction tasks."""
    logger.info("=== Starting Smart Meter Extraction ===")

    hourly = extract_hourly_readings()
    registry = extract_meter_registry()
    outages = extract_outage_events(hourly.copy())
    peaks = extract_demand_peaks(hourly.copy())

    results = {
        "hourly_readings": hourly,
        "meter_registry": registry,
        "outage_events": outages,
        "demand_peaks": peaks,
    }
    logger.info(f"=== Smart Meter Extraction Complete ===")
    return results


if __name__ == "__main__":
    data = run_extraction()
    for name, df in data.items():
        print(f"\n[{name.upper()}] shape={df.shape}")
        if not df.empty:
            print(df.head(3).to_string())
