"""
peak_demand_analysis.py
Peak demand pattern analysis for Nepal's national grid.
Identifies morning/evening peaks, seasonal demand spikes,
and load duration curves for NEA grid planning.
"""

import logging
import pandas as pd
import numpy as np
from pathlib import Path
import yaml

with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

logger = logging.getLogger(__name__)
AGG_DIR = Path(config["data_sources"]["processed"]["aggregated"])


def analyse_daily_load_curve(df_sm: pd.DataFrame) -> pd.DataFrame:
    """
    Compute average load curve by hour for each consumer type.
    Nepal peak hours: 06:00–09:00 (morning), 18:00–22:00 (evening).
    The 18:00–21:00 evening peak is the critical grid stress period.
    """
    profile = (
        df_sm.groupby(["consumer_type","hour","is_weekend"])
        .agg(
            avg_kwh=("consumption_kwh","mean"),
            max_kwh=("consumption_kwh","max"),
            p90_kwh=("consumption_kwh", lambda x: np.percentile(x, 90)),
        )
        .reset_index()
    )
    # Peak hour flag
    profile["is_nea_peak"] = profile["hour"].between(18, 21)
    profile["time_label"] = profile["hour"].apply(
        lambda h: f"{h:02d}:00"
    )
    return profile


def calculate_load_factor(df_sm: pd.DataFrame) -> pd.DataFrame:
    """
    Daily load factor = Average load / Peak load.
    Higher load factor = better grid utilisation.
    Nepal target: >70% (current: ~68-72%).
    """
    daily = df_sm.groupby(["date","consumer_type"]).agg(
        avg_kwh=("consumption_kwh","mean"),
        peak_kwh=("consumption_kwh","max"),
        total_kwh=("consumption_kwh","sum"),
    ).reset_index()

    daily["load_factor_pct"] = (
        daily["avg_kwh"] / daily["peak_kwh"].replace(0, np.nan) * 100
    ).round(2)
    return daily


def identify_system_peak_days(df_sm: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """
    Find the highest peak demand days in the dataset.
    These represent worst-case grid stress events.
    """
    daily_peak = (
        df_sm.groupby("date")["consumption_kwh"]
        .max()
        .reset_index()
        .rename(columns={"consumption_kwh":"peak_kwh"})
        .sort_values("peak_kwh", ascending=False)
        .head(top_n)
    )
    daily_peak["date"] = pd.to_datetime(daily_peak["date"])
    daily_peak["weekday"] = daily_peak["date"].dt.day_name()
    daily_peak["month"]   = daily_peak["date"].dt.month
    daily_peak["rank"]    = range(1, len(daily_peak)+1)
    return daily_peak


def calculate_peak_to_offpeak_ratio(df_sm: pd.DataFrame) -> dict:
    """
    Compute peak vs off-peak consumption ratio by consumer type.
    Used for Time-of-Use (ToU) tariff policy design.
    Nepal NEA is evaluating ToU tariffs to shift industrial load.
    """
    peak_hours    = list(range(6, 9))  + list(range(18, 22))
    offpeak_hours = [h for h in range(24) if h not in peak_hours]

    df = df_sm.copy()
    df["period"] = df["hour"].apply(
        lambda h: "Peak" if h in peak_hours else "Off-Peak"
    )
    breakdown = (
        df.groupby(["consumer_type","period"])["consumption_kwh"]
        .mean()
        .unstack()
        .reset_index()
    )
    if "Peak" in breakdown.columns and "Off-Peak" in breakdown.columns:
        breakdown["peak_to_offpeak_ratio"] = (
            breakdown["Peak"] / breakdown["Off-Peak"].replace(0, np.nan)
        ).round(3)

    return breakdown.to_dict("records")


def load_duration_curve(df_sm: pd.DataFrame, consumer_type: str = None) -> pd.DataFrame:
    """
    Build load duration curve (LDC) — sorted hourly loads descending.
    LDC shows what % of time the system exceeds a given load level.
    Critical for capacity planning and reserve margin assessment.
    """
    if consumer_type:
        df_sm = df_sm[df_sm["consumer_type"] == consumer_type]

    hourly_totals = df_sm.groupby("timestamp")["consumption_kwh"].sum().sort_values(ascending=False)
    n = len(hourly_totals)

    ldc = pd.DataFrame({
        "rank": range(1, n+1),
        "load_kwh": hourly_totals.values,
        "exceedance_pct": [i/n*100 for i in range(1, n+1)],
    })
    ldc["consumer_type"] = consumer_type or "All"
    return ldc


def run_peak_demand_analysis(df_sm: pd.DataFrame = None) -> dict:
    """Run all peak demand analysis modules."""
    logger.info("=== Peak Demand Analysis ===")

    if df_sm is None:
        from etl.extract.extract_smart_meter import run_extraction
        from etl.transform.clean_data import clean_smart_meter
        raw = run_extraction()
        df_sm = clean_smart_meter(raw["hourly_readings"])

    results = {
        "daily_load_curve":      analyse_daily_load_curve(df_sm),
        "load_factor":           calculate_load_factor(df_sm),
        "system_peak_days":      identify_system_peak_days(df_sm),
        "peak_offpeak_ratio":    calculate_peak_to_offpeak_ratio(df_sm),
        "load_duration_curve":   load_duration_curve(df_sm),
    }

    # Summary print
    lf = results["load_factor"]
    print("\n=== PEAK DEMAND ANALYSIS SUMMARY ===")
    print(f"Average load factor: {lf['load_factor_pct'].mean():.1f}%")
    print(f"\nTop 5 Peak Demand Days:")
    print(results["system_peak_days"].head(5)[["rank","date","peak_kwh","weekday"]].to_string(index=False))
    print(f"\nPeak-to-Off-Peak Ratios:")
    for r in results["peak_offpeak_ratio"]:
        print(f"  {r.get('consumer_type','N/A')}: {r.get('peak_to_offpeak_ratio','N/A'):.3f}x")

    return results


if __name__ == "__main__":
    run_peak_demand_analysis()
