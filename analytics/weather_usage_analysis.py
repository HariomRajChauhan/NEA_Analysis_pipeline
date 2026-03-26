"""
weather_usage_analysis.py
Correlate weather variables with electricity consumption.
Nepal-specific: monsoon hydrology, temperature extremes in Terai,
and the unique dual effect of monsoon (high generation + moderate demand).
"""

import logging
import pandas as pd
import numpy as np
from pathlib import Path
import yaml

with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

logger = logging.getLogger(__name__)


def compute_correlations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pearson correlation between weather variables and total electricity consumption.
    Returns a ranked correlation table.
    """
    weather_vars = [
        "avg_temp_c","max_temp_c","humidity_pct","rainfall_mm",
        "total_hdd","total_cdd","temp_demand_index",
    ]
    existing = [c for c in weather_vars if c in df.columns]

    if "total_gwh" not in df.columns:
        logger.warning("total_gwh not found in dataset for correlation")
        return pd.DataFrame()

    correlations = []
    for var in existing:
        corr = df[[var, "total_gwh"]].dropna().corr().iloc[0, 1]
        correlations.append({
            "weather_variable": var,
            "correlation_r": round(corr, 4),
            "abs_r": abs(corr),
            "direction": "Positive" if corr > 0 else "Negative",
            "strength": (
                "Strong" if abs(corr) > 0.7 else
                "Moderate" if abs(corr) > 0.4 else
                "Weak"
            ),
        })

    return (
        pd.DataFrame(correlations)
        .sort_values("abs_r", ascending=False)
        .drop(columns="abs_r")
        .reset_index(drop=True)
    )


def monsoon_impact_analysis(df: pd.DataFrame) -> dict:
    """
    Compare consumption during monsoon (June–Sep) vs dry season.
    Nepal paradox: monsoon = peak hydro generation BUT moderate consumption
    (industry doesn't surge during rains; agriculture is rainfed).
    """
    if "is_monsoon" not in df.columns:
        if "month" in df.columns:
            df = df.copy()
            df["is_monsoon"] = df["month"].between(6, 9)
        else:
            return {}

    monsoon    = df[df["is_monsoon"]]
    dry_season = df[~df["is_monsoon"]]

    return {
        "monsoon_avg_gwh":     round(float(monsoon["total_gwh"].mean()), 3),
        "dry_season_avg_gwh":  round(float(dry_season["total_gwh"].mean()), 3),
        "monsoon_premium_pct": round(
            float((monsoon["total_gwh"].mean() / dry_season["total_gwh"].mean() - 1) * 100), 2
        ),
        "monsoon_avg_rainfall_mm": round(float(monsoon.get("total_rainfall_mm", pd.Series([0])).mean()), 1),
        "monsoon_months": "June, July, August, September",
        "insight": (
            "Monsoon shows higher consumption due to pre-monsoon heat (AC load) "
            "and irrigation pumping, but hydropower surplus peaks simultaneously."
        ),
    }


def temperature_demand_regression(df: pd.DataFrame) -> dict:
    """
    Simple OLS regression: GWh ~ avg_temp + HDD + CDD + rainfall.
    Returns coefficients and R² for the model.
    """
    from numpy.linalg import lstsq

    features = ["avg_temp_c","total_hdd","total_cdd","total_rainfall_mm"]
    existing = [c for c in features if c in df.columns]
    df_clean = df[["total_gwh"] + existing].dropna()

    if len(df_clean) < 10:
        return {"error": "Insufficient data for regression"}

    X = np.column_stack([np.ones(len(df_clean))] + [df_clean[c].values for c in existing])
    y = df_clean["total_gwh"].values

    coeffs, residuals, rank, sv = lstsq(X, y, rcond=None)
    y_pred = X @ coeffs
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - y.mean()) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0

    result = {
        "r_squared": round(float(r2), 4),
        "intercept": round(float(coeffs[0]), 4),
        "n_observations": len(df_clean),
    }
    for i, col in enumerate(existing):
        result[f"coeff_{col}"] = round(float(coeffs[i+1]), 6)

    return result


def seasonal_demand_profile(df_monthly: pd.DataFrame) -> pd.DataFrame:
    """
    Average consumption profile by Nepal's 4 seasons.
    Maps well to Nepal's electricity planning calendar.
    """
    if "season" not in df_monthly.columns:
        df_monthly = df_monthly.copy()
        df_monthly["season"] = df_monthly["month"].apply(
            lambda m: (
                "Pre-Monsoon" if m in [3,4,5] else
                "Monsoon"     if m in [6,7,8,9] else
                "Post-Monsoon" if m in [10,11] else
                "Winter"
            )
        )

    season_order = ["Winter","Pre-Monsoon","Monsoon","Post-Monsoon"]
    seasonal = (
        df_monthly.groupby("season")
        .agg(
            avg_gwh=("total_gwh","mean"),
            max_gwh=("total_gwh","max"),
            min_gwh=("total_gwh","min"),
            months_count=("total_gwh","count"),
        )
        .round(3)
        .reindex(season_order)
        .reset_index()
    )
    return seasonal


def run_weather_analysis(df_weather_consumption: pd.DataFrame = None) -> dict:
    """Run all weather-usage analysis."""
    logger.info("=== Weather-Consumption Analysis ===")

    if df_weather_consumption is None:
        from etl.extract.extract_nea import run_extraction as nea_ex
        from etl.extract.extract_weather_api import run_extraction as wx_ex
        from etl.transform.clean_data import run_cleaning
        from etl.transform.transform_data import create_consumption_weather_features
        raw = {}
        raw.update(nea_ex())
        raw.update(wx_ex())
        cleaned = run_cleaning(raw)
        df_weather_consumption = create_consumption_weather_features(
            cleaned.get("monthly", pd.DataFrame()),
            cleaned.get("weather", pd.DataFrame()),
        )

    results = {
        "correlations":           compute_correlations(df_weather_consumption),
        "monsoon_impact":         monsoon_impact_analysis(df_weather_consumption),
        "regression":             temperature_demand_regression(df_weather_consumption),
        "seasonal_demand_profile": seasonal_demand_profile(df_weather_consumption),
    }

    print("\n=== WEATHER-CONSUMPTION ANALYSIS ===")
    print("\n── Correlations ──")
    print(results["correlations"].to_string(index=False))

    print("\n── Monsoon Impact ──")
    for k, v in results["monsoon_impact"].items():
        print(f"  {k}: {v}")

    print("\n── Regression (GWh ~ Weather) ──")
    for k, v in results["regression"].items():
        print(f"  {k}: {v}")

    print("\n── Seasonal Demand Profile ──")
    print(results["seasonal_demand_profile"].to_string(index=False))

    return results


if __name__ == "__main__":
    run_weather_analysis()
