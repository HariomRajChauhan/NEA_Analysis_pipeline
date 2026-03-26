"""
consumption_analysis.py
Core electricity consumption analysis for Nepal Electricity Authority.
Covers: national trends, sector breakdown, growth rates, per-capita,
        import/export balance, and forecasting baseline.
"""

import logging
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import yaml

with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

logger = logging.getLogger(__name__)
AGG_DIR = Path(config["data_sources"]["processed"]["aggregated"])
REPORT_DIR = Path("reports/output")
REPORT_DIR.mkdir(parents=True, exist_ok=True)


def load_annual_data() -> pd.DataFrame:
    path = AGG_DIR / "annual_kpi_summary.csv"
    if path.exists():
        return pd.read_csv(path)
    from etl.extract.extract_nea import run_extraction
    from etl.transform.clean_data import clean_annual_consumption
    raw = run_extraction()
    return clean_annual_consumption(raw["annual"])


def load_monthly_data() -> pd.DataFrame:
    path = AGG_DIR / "monthly_trends.csv"
    if path.exists():
        return pd.read_csv(path, parse_dates=["date"])
    from etl.extract.extract_nea import run_extraction
    from etl.transform.clean_data import clean_monthly_consumption
    raw = run_extraction()
    return clean_monthly_consumption(raw["monthly"])


# ── 1. National Growth Trends ──────────────────────────────────────────────

def analyse_national_growth(df: pd.DataFrame) -> dict:
    """
    Analyse national electricity consumption growth.

    Key Nepal context:
    - Upper Tamakoshi (456 MW) commissioned 2021 → surplus from 2022
    - Per capita grew from 131 kWh (2015/16) to 400 kWh (2023/24)
    - System loss declining: 24.5% → 15.8%
    """
    df = df.sort_values("fiscal_year")
    n = len(df)

    cagr = (
        (df["total_energy_gwh"].iloc[-1] / df["total_energy_gwh"].iloc[0]) ** (1/(n-1)) - 1
    ) * 100

    peak_cagr = (
        (df["per_capita_kwh"].iloc[-1] / df["per_capita_kwh"].iloc[0]) ** (1/(n-1)) - 1
    ) * 100

    latest = df.iloc[-1]
    prev   = df.iloc[-2]

    result = {
        "period": f"{df['fiscal_year'].iloc[0]} – {df['fiscal_year'].iloc[-1]}",
        "consumption_cagr_pct": round(cagr, 2),
        "per_capita_cagr_pct": round(peak_cagr, 2),
        "latest_fiscal_year": latest["fiscal_year"],
        "latest_total_gwh": latest["total_energy_gwh"],
        "yoy_growth_pct": round(
            (latest["total_energy_gwh"] / prev["total_energy_gwh"] - 1) * 100, 2
        ),
        "latest_per_capita_kwh": latest["per_capita_kwh"],
        "latest_peak_mw": latest["peak_demand_mw"],
        "latest_consumers": latest["total_consumers"],
        "latest_system_loss_pct": latest["system_loss_pct"],
        "total_loss_reduction_pp": round(
            df["system_loss_pct"].iloc[0] - df["system_loss_pct"].iloc[-1], 2
        ),
    }
    logger.info(f"National growth analysis: CAGR={cagr:.2f}%, per-capita CAGR={peak_cagr:.2f}%")
    return result


# ── 2. Sector Breakdown ───────────────────────────────────────────────────

def analyse_sector_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sector share analysis: Domestic, Industrial, Commercial, Irrigation, Other.
    Tracks structural shift as Nepal industrialises.
    """
    sectors = ["domestic_gwh","industrial_gwh","commercial_gwh","irrigation_gwh","other_gwh"]
    existing = [c for c in sectors if c in df.columns]

    df = df.copy()
    df["sector_total"] = df[existing].sum(axis=1)

    for col in existing:
        label = col.replace("_gwh","_share_pct")
        df[label] = (df[col] / df["sector_total"] * 100).round(2)

    logger.info(f"Sector breakdown computed for {len(df)} years")
    return df[["fiscal_year"] + existing + [c.replace("_gwh","_share_pct") for c in existing]]


# ── 3. Import / Export Balance ────────────────────────────────────────────

def analyse_trade_balance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyse Nepal's electricity trade with India.
    Nepal shifted from net importer to net exporter by FY 2022/23
    due to Upper Tamakoshi and other new projects.
    """
    df = df.copy()
    df["trade_balance_gwh"] = df.get(
        "energy_exported_gwh", pd.Series(0, index=df.index)
    ) - df.get(
        "energy_imported_gwh", pd.Series(0, index=df.index)
    )
    df["trade_status"] = df["trade_balance_gwh"].apply(
        lambda x: "Net Exporter" if x > 0 else "Net Importer"
    )
    df["import_dependency_pct"] = (
        df.get("energy_imported_gwh", pd.Series(0, index=df.index)) /
        df["total_energy_gwh"].replace(0, np.nan) * 100
    ).round(2)

    return df[["fiscal_year","energy_imported_gwh","energy_exported_gwh",
               "trade_balance_gwh","trade_status","import_dependency_pct"]]


# ── 4. Simple Demand Forecast ─────────────────────────────────────────────

def forecast_demand(df: pd.DataFrame, years_ahead: int = 5) -> pd.DataFrame:
    """
    Linear trend forecast of national electricity demand.
    Uses log-linear regression (consistent with exponential growth).
    Returns forecast for next `years_ahead` Nepali fiscal years.
    """
    df = df.copy().sort_values("fiscal_year")
    df["t"] = range(len(df))

    # Log-linear regression
    log_y = np.log(df["total_energy_gwh"].values)
    t     = df["t"].values
    coeffs = np.polyfit(t, log_y, 1)
    slope, intercept = coeffs

    # CAGR from regression
    cagr = (np.exp(slope) - 1) * 100

    last_fy   = df["fiscal_year"].iloc[-1]
    last_year = int(last_fy[:4])
    last_t    = df["t"].iloc[-1]

    forecasts = []
    for i in range(1, years_ahead + 1):
        t_proj   = last_t + i
        gwh_proj = np.exp(intercept + slope * t_proj)
        fy       = f"{last_year+i}/{str(last_year+i+1)[-2:]}"
        forecasts.append({
            "fiscal_year": fy,
            "forecast_gwh": round(gwh_proj, 1),
            "growth_assumption_pct": round(cagr, 2),
            "confidence": "Moderate",
            "method": "Log-linear trend",
        })

    forecast_df = pd.DataFrame(forecasts)
    logger.info(
        f"Demand forecast: CAGR={cagr:.2f}%, "
        f"projected {years_ahead}yr total = {forecast_df['forecast_gwh'].sum():.0f} GWh"
    )
    return forecast_df


# ── 5. Monthly Seasonal Analysis ─────────────────────────────────────────

def analyse_seasonality(df_monthly: pd.DataFrame) -> dict:
    """
    Identify seasonal consumption patterns.
    Nepal peak: Monsoon July–August (max hydro generation + summer AC load)
    Nepal trough: Winter Dec–Feb (load shedding era ended, but still lower demand)
    """
    df = df_monthly.copy()
    monthly_avg = df.groupby("month")["total_gwh"].mean()

    peak_month   = int(monthly_avg.idxmax())
    trough_month = int(monthly_avg.idxmin())

    month_names = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                   7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}

    return {
        "peak_month": month_names[peak_month],
        "peak_avg_gwh": round(float(monthly_avg.max()), 2),
        "trough_month": month_names[trough_month],
        "trough_avg_gwh": round(float(monthly_avg.min()), 2),
        "seasonal_ratio": round(float(monthly_avg.max() / monthly_avg.min()), 3),
        "monsoon_share_pct": round(
            float(df[df["month"].between(6,9)]["total_gwh"].sum() /
                  df["total_gwh"].sum() * 100), 2
        ),
    }


# ── Main ──────────────────────────────────────────────────────────────────

def run_consumption_analysis() -> dict:
    """Run all consumption analysis modules."""
    logger.info("=== Consumption Analysis ===")
    annual  = load_annual_data()
    monthly = load_monthly_data()

    results = {
        "national_growth":  analyse_national_growth(annual),
        "sector_breakdown": analyse_sector_breakdown(annual),
        "trade_balance":    analyse_trade_balance(annual),
        "demand_forecast":  forecast_demand(annual, years_ahead=5),
        "seasonality":      analyse_seasonality(monthly),
    }

    # Export sector breakdown
    results["sector_breakdown"].to_csv(
        REPORT_DIR / "sector_breakdown.csv", index=False
    )
    results["demand_forecast"].to_csv(
        REPORT_DIR / "demand_forecast.csv", index=False
    )

    print("\n=== NATIONAL ELECTRICITY CONSUMPTION ANALYSIS ===")
    for k, v in results["national_growth"].items():
        print(f"  {k}: {v}")

    print("\n=== SEASONALITY ===")
    for k, v in results["seasonality"].items():
        print(f"  {k}: {v}")

    print("\n=== 5-YEAR DEMAND FORECAST ===")
    print(results["demand_forecast"].to_string(index=False))

    return results


if __name__ == "__main__":
    run_consumption_analysis()
