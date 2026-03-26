"""
extract_open_data.py
Extract data from OpenDataNepal (opendatanepal.com) and CBS Nepal.
Sources:
  - opendatanepal.com: province electricity, district-level data
  - censusnepal.cbs.gov.np: population, household, electrification data
  - data.worldbank.org: per capita kWh trends
"""

import logging
import pandas as pd
import requests
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

OPEN_DATA_DIR = Path(config["data_sources"]["raw"]["open_data_nepal"])
POP_DATA_DIR = Path(config["data_sources"]["external"]["population"])


def extract_province_electricity() -> pd.DataFrame:
    """
    Load province-level electricity consumption and infrastructure data.
    Source: open_data_nepal/province_electricity_data.csv
    Data grounded in NEA Annual Report 2023/24 and Census 2021.

    Key columns:
        province_id, province_name, population_2021, households_2021,
        electrification_pct, electricity_consumers_2023,
        annual_consumption_gwh_2023, per_capita_kwh_2023,
        peak_demand_mw_2023, installed_capacity_mw_2023,
        transmission_lines_km, distribution_lines_km, system_loss_pct
    """
    file_path = OPEN_DATA_DIR / "province_electricity_data.csv"
    logger.info(f"Extracting province electricity data from {file_path}")
    df = pd.read_csv(file_path)
    df["extracted_at"] = datetime.now()
    logger.info(f"Loaded {len(df)} province records")
    return df


def extract_district_electricity() -> pd.DataFrame:
    """
    Load district-level electricity data.
    Source: open_data_nepal/district_electricity.csv
    77 districts across 7 provinces.
    """
    file_path = OPEN_DATA_DIR / "district_electricity.csv"
    logger.info(f"Extracting district electricity data from {file_path}")
    df = pd.read_csv(file_path)
    df["extracted_at"] = datetime.now()
    logger.info(f"Loaded {len(df)} district records")
    return df


def extract_population_data() -> pd.DataFrame:
    """
    Load Nepal Census 2021 province-level population data.
    Source: external/population_data/nepal_province_population.csv

    Includes: population, household size, urban %, literacy,
              per capita income, GDP contribution, electrification %
    """
    file_path = POP_DATA_DIR / "nepal_province_population.csv"
    logger.info(f"Extracting population data from {file_path}")
    df = pd.read_csv(file_path)
    df["extracted_at"] = datetime.now()
    logger.info(f"Loaded population data for {len(df)} provinces")
    return df


def fetch_worldbank_electricity_data() -> pd.DataFrame:
    """
    Fetch Nepal per-capita electricity consumption from World Bank API.
    Indicator: EG.USE.ELEC.KH.PC
    Falls back to embedded historical data if API unavailable.
    """
    WB_API = "https://api.worldbank.org/v2/country/NP/indicator/EG.USE.ELEC.KH.PC"
    params = {"format": "json", "per_page": 30, "mrv": 20}

    try:
        resp = requests.get(WB_API, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        records = [
            {"year": int(item["date"]), "per_capita_kwh_wb": item["value"]}
            for item in data[1] if item["value"] is not None
        ]
        df = pd.DataFrame(records).sort_values("year")
        df["source"] = "World Bank API"
        logger.info(f"World Bank: fetched {len(df)} per-capita kWh records")
        return df
    except Exception as e:
        logger.warning(f"World Bank API unavailable ({e}), using embedded data")
        # Historical data from World Bank and NEA reports
        embedded = [
            {"year": 2010, "per_capita_kwh_wb": 87},
            {"year": 2011, "per_capita_kwh_wb": 97},
            {"year": 2012, "per_capita_kwh_wb": 102},
            {"year": 2013, "per_capita_kwh_wb": 110},
            {"year": 2014, "per_capita_kwh_wb": 117},
            {"year": 2015, "per_capita_kwh_wb": 131},
            {"year": 2016, "per_capita_kwh_wb": 165},
            {"year": 2017, "per_capita_kwh_wb": 180},
            {"year": 2018, "per_capita_kwh_wb": 204},
            {"year": 2019, "per_capita_kwh_wb": 220},
            {"year": 2020, "per_capita_kwh_wb": 238},
            {"year": 2021, "per_capita_kwh_wb": 270},
            {"year": 2022, "per_capita_kwh_wb": 305},
            {"year": 2023, "per_capita_kwh_wb": 400},
        ]
        df = pd.DataFrame(embedded)
        df["source"] = "Embedded (NEA/WB)"
        return df


def extract_ipp_projects() -> pd.DataFrame:
    """
    Extract Independent Power Producer (IPP) project data.
    Based on NEA Power Trade Department records.
    """
    data = [
        {"project": "Khimti", "mw": 60, "province": 3, "type": "RoR", "ipp": "Himal Power Ltd", "status": "Operating"},
        {"project": "Bhote Koshi", "mw": 36, "province": 3, "type": "RoR", "ipp": "BKPCL", "status": "Operating"},
        {"project": "Chilime", "mw": 22, "province": 3, "type": "RoR", "ipp": "Chilime Hydro", "status": "Operating"},
        {"project": "Modi Khola", "mw": 14.8, "province": 4, "type": "RoR", "ipp": "Butwal Power", "status": "Operating"},
        {"project": "Rairang", "mw": 30, "province": 3, "type": "RoR", "ipp": "Private", "status": "Operating"},
        {"project": "Upper Marsyangdi A", "mw": 50, "province": 4, "type": "RoR", "ipp": "UMHL", "status": "Operating"},
        {"project": "Upper Trishuli 3A", "mw": 60, "province": 3, "type": "RoR", "ipp": "NWEDC", "status": "Operating"},
        {"project": "Arun III", "mw": 900, "province": 1, "type": "Storage", "ipp": "SJVNL", "status": "Under Construction"},
        {"project": "Upper Arun", "mw": 639, "province": 1, "type": "Storage", "ipp": "NWEDC", "status": "Under Construction"},
        {"project": "West Seti", "mw": 750, "province": 7, "type": "Storage", "ipp": "NHDCL", "status": "Planning"},
        {"project": "Tamor", "mw": 762, "province": 1, "type": "Storage", "ipp": "NEA", "status": "DPR Stage"},
        {"project": "Pancheshwar", "mw": 6480, "province": 7, "type": "Storage", "ipp": "PPCL (India-Nepal)", "status": "Planning"},
        {"project": "Upper Karnali", "mw": 900, "province": 6, "type": "RoR", "ipp": "GMR", "status": "Under Development"},
        {"project": "Lower Karnali", "mw": 3000, "province": 6, "type": "RoR", "ipp": "GMR", "status": "Planning"},
    ]
    df = pd.DataFrame(data)
    df["extracted_at"] = datetime.now()
    logger.info(f"Extracted {len(df)} IPP project records")
    return df


def run_extraction() -> dict:
    """Run all open data extraction tasks."""
    logger.info("=== Starting Open Data Nepal Extraction ===")

    results = {
        "province_electricity": extract_province_electricity(),
        "district_electricity": extract_district_electricity(),
        "population": extract_population_data(),
        "worldbank_percapita": fetch_worldbank_electricity_data(),
        "ipp_projects": extract_ipp_projects(),
    }

    logger.info("=== Open Data Extraction Complete ===")
    return results


if __name__ == "__main__":
    data = run_extraction()
    for name, df in data.items():
        print(f"\n[{name.upper()}] shape={df.shape}")
        print(df.head(3).to_string())
