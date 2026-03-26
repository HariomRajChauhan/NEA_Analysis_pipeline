"""
extract_nea.py
Extract data from Nepal Electricity Authority (NEA) annual and monthly reports.
Sources: nea.org.np annual reports, operational reports (CSV/PDF format)
"""

import os
import logging
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime
import yaml

# Load config
with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

logging.basicConfig(
    filename=config["logging"]["log_dir"] + config["logging"]["etl_log"],
    level=getattr(logging, config["logging"]["level"]),
    format=config["logging"]["format"]
)
logger = logging.getLogger(__name__)

RAW_NEA_DIR = Path(config["data_sources"]["raw"]["nea_reports"])


def extract_annual_consumption() -> pd.DataFrame:
    """
    Load NEA annual electricity consumption report.
    File: nea_annual_consumption.csv
    Columns: fiscal_year, total_energy_gwh, domestic_gwh, industrial_gwh,
             commercial_gwh, irrigation_gwh, other_gwh, peak_demand_mw,
             total_consumers, per_capita_kwh, system_loss_pct,
             energy_imported_gwh, energy_exported_gwh, installed_capacity_mw
    """
    file_path = RAW_NEA_DIR / "nea_annual_consumption.csv"
    logger.info(f"Extracting annual consumption from: {file_path}")
    try:
        df = pd.read_csv(file_path)
        df["extracted_at"] = datetime.now()
        df["source"] = "NEA Annual Report"
        logger.info(f"Extracted {len(df)} annual records")
        return df
    except Exception as e:
        logger.error(f"Failed to extract annual consumption: {e}")
        raise


def extract_monthly_consumption() -> pd.DataFrame:
    """
    Load NEA monthly electricity consumption data.
    File: nea_monthly_consumption.csv
    Columns: year, month, month_name, domestic_gwh, industrial_gwh,
             commercial_gwh, irrigation_gwh, other_gwh, total_gwh,
             peak_demand_mw, avg_daily_mwh, load_factor_pct
    """
    file_path = RAW_NEA_DIR / "nea_monthly_consumption.csv"
    logger.info(f"Extracting monthly consumption from: {file_path}")
    try:
        df = pd.read_csv(file_path)
        # Construct date column from year + month
        df["date"] = pd.to_datetime(
            df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2) + "-01"
        )
        df["extracted_at"] = datetime.now()
        df["source"] = "NEA Monthly Operational Report"
        logger.info(f"Extracted {len(df)} monthly records")
        return df
    except Exception as e:
        logger.error(f"Failed to extract monthly consumption: {e}")
        raise


def extract_tariff_structure() -> pd.DataFrame:
    """
    Load NEA consumer tariff structure.
    File: nea_tariff_structure.csv
    Contains tariff codes, categories, rates in NPR/kWh.
    """
    file_path = RAW_NEA_DIR / "nea_tariff_structure.csv"
    logger.info(f"Extracting tariff structure from: {file_path}")
    try:
        df = pd.read_csv(file_path)
        df["extracted_at"] = datetime.now()
        logger.info(f"Extracted {len(df)} tariff categories")
        return df
    except Exception as e:
        logger.error(f"Failed to extract tariff structure: {e}")
        raise


def download_nea_operational_report(report_date: str = None) -> bool:
    """
    Attempt to download latest operational report from nea.org.np.
    Falls back to local file if download fails.
    """
    if report_date is None:
        report_date = datetime.now().strftime("%Y-%m-%d")

    url = f"https://www.nea.org.np/daily-operational-report/{report_date}"
    local_fallback = RAW_NEA_DIR / f"operational_report_{report_date}.csv"

    try:
        logger.info(f"Attempting to download NEA report for {report_date}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        local_fallback.write_bytes(response.content)
        logger.info(f"Downloaded NEA report: {local_fallback}")
        return True
    except requests.RequestException as e:
        logger.warning(f"Download failed ({e}), using local data")
        return False


def extract_hydropower_projects() -> pd.DataFrame:
    """
    Extract installed hydropower project data.
    Returns commissioned projects with capacity and type.
    """
    data = [
        {"project_name": "Upper Tamakoshi", "province_id": 3, "capacity_mw": 456, "type": "Storage", "commissioned": 2021, "operator": "THDC"},
        {"project_name": "Kulekhani I", "province_id": 3, "capacity_mw": 60, "type": "Storage", "commissioned": 1982, "operator": "NEA"},
        {"project_name": "Kulekhani II", "province_id": 3, "capacity_mw": 32, "type": "Storage", "commissioned": 1986, "operator": "NEA"},
        {"project_name": "Marsyangdi", "province_id": 4, "capacity_mw": 69, "type": "RoR", "commissioned": 1989, "operator": "NEA"},
        {"project_name": "Kaligandaki A", "province_id": 5, "capacity_mw": 144, "type": "RoR", "commissioned": 2002, "operator": "NEA"},
        {"project_name": "Middle Marsyangdi", "province_id": 4, "capacity_mw": 70, "type": "RoR", "commissioned": 2008, "operator": "NEA"},
        {"project_name": "Chilime", "province_id": 3, "capacity_mw": 22, "type": "RoR", "commissioned": 2003, "operator": "Chilime Hydro"},
        {"project_name": "Modi Khola", "province_id": 4, "capacity_mw": 14.8, "type": "RoR", "commissioned": 2001, "operator": "Butwal Power"},
        {"project_name": "Khimti I", "province_id": 3, "capacity_mw": 60, "type": "RoR", "commissioned": 2000, "operator": "Himal Power"},
        {"project_name": "Bhote Koshi", "province_id": 3, "capacity_mw": 36, "type": "RoR", "commissioned": 2000, "operator": "Bhote Koshi Power"},
        {"project_name": "Trishuli", "province_id": 3, "capacity_mw": 24, "type": "RoR", "commissioned": 1967, "operator": "NEA"},
        {"project_name": "Gandak", "province_id": 5, "capacity_mw": 15, "type": "RoR", "commissioned": 1979, "operator": "NEA"},
        {"project_name": "Sunkoshi", "province_id": 3, "capacity_mw": 10.05, "type": "RoR", "commissioned": 1972, "operator": "NEA"},
        {"project_name": "Panauti", "province_id": 3, "capacity_mw": 2.4, "type": "RoR", "commissioned": 1965, "operator": "NEA"},
    ]
    df = pd.DataFrame(data)
    df["extracted_at"] = datetime.now()
    logger.info(f"Extracted {len(df)} hydropower projects")
    return df


def run_extraction() -> dict:
    """Run all NEA extraction tasks and return DataFrames."""
    logger.info("=== Starting NEA Data Extraction ===")
    results = {}

    results["annual"] = extract_annual_consumption()
    results["monthly"] = extract_monthly_consumption()
    results["tariff"] = extract_tariff_structure()
    results["projects"] = extract_hydropower_projects()

    logger.info(f"=== NEA Extraction Complete. Datasets: {list(results.keys())} ===")
    return results


if __name__ == "__main__":
    data = run_extraction()
    for name, df in data.items():
        print(f"\n[{name.upper()}] shape={df.shape}")
        print(df.head(3).to_string())
