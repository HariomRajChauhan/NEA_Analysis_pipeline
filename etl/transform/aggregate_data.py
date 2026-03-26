"""
aggregate_data.py
Create aggregated summary tables for dashboards, reports, and Power BI.
Produces final analytics-ready datasets saved to processed/aggregated_data/.
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
AGG_DIR.mkdir(parents=True, exist_ok=True)


def aggregate_annual_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Annual KPI summary table — primary dashboard feed."""
    logger.info("Aggregating annual KPI summary")
    summary = df[[
        "fiscal_year","total_energy_gwh","domestic_gwh","industrial_gwh",
        "commercial_gwh","irrigation_gwh","peak_demand_mw","total_consumers",
        "per_capita_kwh","system_loss_pct","installed_capacity_mw",
        "energy_imported_gwh","energy_exported_gwh","net_energy_gwh",
        "self_sufficiency_ratio","load_factor_pct"
    ]].copy()

    # YoY growth rates
    for col in ["total_energy_gwh","peak_demand_mw","total_consumers","per_capita_kwh"]:
        summary[f"{col}_yoy_pct"] = summary[col].pct_change() * 100

    summary["trade_balance_gwh"] = (
        summary["energy_exported_gwh"] - summary["energy_imported_gwh"]
    )
    summary["aggregated_at"] = datetime.now()
    return summary


def aggregate_monthly_trends(df: pd.DataFrame) -> pd.DataFrame:
    """Monthly consumption trends with seasonal indices."""
    logger.info("Aggregating monthly trends")

    # Seasonal index: month's average / annual average
    annual_avg = df.groupby("year")["total_gwh"].transform("mean")
    df["seasonal_index"] = (df["total_gwh"] / annual_avg).round(4)

    # 3-month moving average
    df = df.sort_values(["year","month"])
    df["total_gwh_3m_avg"] = df["total_gwh"].rolling(3, min_periods=1).mean().round(2)
    df["aggregated_at"] = datetime.now()
    return df


def aggregate_province_ranking(df: pd.DataFrame) -> pd.DataFrame:
    """Province ranking table for comparative analysis."""
    logger.info("Aggregating province rankings")
    df = df.copy()

    rank_cols = {
        "per_capita_kwh_2023": "rank_per_capita",
        "annual_consumption_gwh_2023": "rank_consumption",
        "electrification_pct_2021": "rank_electrification",
        "system_loss_pct": "rank_system_loss",  # lower is better
    }
    for col, rank_col in rank_cols.items():
        ascending = col == "system_loss_pct"
        df[rank_col] = df[col].rank(ascending=ascending, method="min").astype(int)

    df["aggregated_at"] = datetime.now()
    return df


def aggregate_hourly_load_profile(df_sm: pd.DataFrame) -> pd.DataFrame:
    """
    Average hourly load profile by consumer type and weekday/weekend.
    Used for peak demand analysis and tariff optimisation.
    """
    logger.info("Aggregating hourly load profiles")
    profile = (
        df_sm.groupby(["consumer_type","hour","is_weekend"])
        .agg(
            avg_kwh=("consumption_kwh","mean"),
            p95_kwh=("consumption_kwh", lambda x: np.percentile(x, 95)),
            p05_kwh=("consumption_kwh", lambda x: np.percentile(x, 5)),
            std_kwh=("consumption_kwh","std"),
            sample_count=("consumption_kwh","count"),
        )
        .reset_index()
    )
    profile["avg_kwh"]  = profile["avg_kwh"].round(4)
    profile["p95_kwh"]  = profile["p95_kwh"].round(4)
    profile["p05_kwh"]  = profile["p05_kwh"].round(4)
    profile["std_kwh"]  = profile["std_kwh"].round(4)
    profile["day_type"] = profile["is_weekend"].map({True:"Weekend",False:"Weekday"})
    profile["aggregated_at"] = datetime.now()
    return profile


def aggregate_peak_demand_calendar(df_sm: pd.DataFrame) -> pd.DataFrame:
    """
    Daily peak demand calendar: max hourly consumption per day.
    Useful for heatmap visualisations.
    """
    logger.info("Aggregating peak demand calendar")
    daily = (
        df_sm.groupby(["date","consumer_type"])
        .agg(
            daily_total_kwh=("consumption_kwh","sum"),
            peak_hour_kwh=("consumption_kwh","max"),
            peak_hour=("consumption_kwh", lambda x: x.idxmax()),
            avg_kwh=("consumption_kwh","mean"),
            avg_voltage=("voltage_v","mean"),
            avg_pf=("power_factor","mean"),
        )
        .reset_index()
    )
    daily["date"] = pd.to_datetime(daily["date"])
    daily["weekday"] = daily["date"].dt.day_name()
    daily["week_number"] = daily["date"].dt.isocalendar().week.astype(int)
    daily["aggregated_at"] = datetime.now()
    return daily


def aggregate_system_losses_by_province(df_province: pd.DataFrame) -> pd.DataFrame:
    """
    System loss analysis by province.
    System loss = transmission + distribution losses.
    NEA target: reduce to <15% by 2027.
    """
    logger.info("Aggregating system losses by province")
    df = df_province[[
        "province_id","province_name",
        "annual_consumption_gwh_2023","system_loss_pct",
        "transmission_lines_km","distribution_lines_km"
    ]].copy()

    df["estimated_loss_gwh"] = (
        df["annual_consumption_gwh_2023"] * df["system_loss_pct"] / 100
    ).round(2)

    df["loss_per_km_mwh"] = (
        df["estimated_loss_gwh"] * 1000 /
        (df["transmission_lines_km"] + df["distribution_lines_km"]).replace(0, np.nan)
    ).round(2)

    df["loss_reduction_target_pct"] = (df["system_loss_pct"] - 15).clip(lower=0).round(2)
    df["potential_saving_gwh"] = (
        df["annual_consumption_gwh_2023"] * df["loss_reduction_target_pct"] / 100
    ).round(2)

    df["aggregated_at"] = datetime.now()
    return df.sort_values("system_loss_pct", ascending=False)


def aggregate_revenue_by_sector(df_monthly: pd.DataFrame) -> pd.DataFrame:
    """Annual revenue breakdown by consumer sector."""
    logger.info("Aggregating revenue by sector")
    rev_cols = [
        "domestic_revenue_npr","industrial_revenue_npr",
        "commercial_revenue_npr","irrigation_revenue_npr","other_revenue_npr"
    ]
    existing = [c for c in rev_cols if c in df_monthly.columns]

    if not existing:
        logger.warning("Revenue columns not found in monthly data")
        return pd.DataFrame()

    annual_rev = df_monthly.groupby("year")[existing].sum().reset_index()
    annual_rev["total_revenue_npr"] = annual_rev[existing].sum(axis=1)
    for col in existing:
        annual_rev[f"{col}_pct"] = (
            annual_rev[col] / annual_rev["total_revenue_npr"] * 100
        ).round(2)

    annual_rev["total_revenue_nrs_bn"] = (annual_rev["total_revenue_npr"] / 1e9).round(3)
    annual_rev["aggregated_at"] = datetime.now()
    return annual_rev


def create_dashboard_export(aggregated: dict) -> pd.DataFrame:
    """
    Create a single consolidated CSV for Power BI / dashboard ingestion.
    Combines key metrics from all aggregated tables.
    """
    logger.info("Creating dashboard export CSV")
    rows = []

    if "annual_summary" in aggregated:
        for _, r in aggregated["annual_summary"].iterrows():
            rows.append({
                "metric_type": "Annual",
                "period": r["fiscal_year"],
                "total_gwh": r["total_energy_gwh"],
                "peak_mw": r["peak_demand_mw"],
                "consumers": r["total_consumers"],
                "per_capita_kwh": r["per_capita_kwh"],
                "system_loss_pct": r["system_loss_pct"],
                "self_sufficiency": r.get("self_sufficiency_ratio",np.nan),
            })

    dashboard_df = pd.DataFrame(rows)
    dashboard_df["exported_at"] = datetime.now()
    return dashboard_df


def save_aggregated(df: pd.DataFrame, name: str) -> Path:
    out = AGG_DIR / f"{name}.csv"
    df.to_csv(out, index=False)
    logger.info(f"Saved aggregated: {out} ({len(df)} rows)")
    return out


def run_aggregation(transformed: dict, cleaned: dict) -> dict:
    """Run all aggregation steps."""
    logger.info("=== Starting Aggregation Stage ===")
    aggregated = {}

    if "annual" in transformed:
        aggregated["annual_summary"] = aggregate_annual_summary(transformed["annual"])
        save_aggregated(aggregated["annual_summary"], "annual_kpi_summary")

    if "monthly_revenue" in transformed:
        aggregated["monthly_trends"] = aggregate_monthly_trends(transformed["monthly_revenue"])
        save_aggregated(aggregated["monthly_trends"], "monthly_trends")

        aggregated["revenue_by_sector"] = aggregate_revenue_by_sector(transformed["monthly_revenue"])
        save_aggregated(aggregated["revenue_by_sector"], "revenue_by_sector")

    if "province_enriched" in transformed:
        aggregated["province_ranking"] = aggregate_province_ranking(transformed["province_enriched"])
        save_aggregated(aggregated["province_ranking"], "province_ranking")

        aggregated["system_losses"] = aggregate_system_losses_by_province(transformed["province_enriched"])
        save_aggregated(aggregated["system_losses"], "system_losses_by_province")

    if "smart_meter_features" in transformed:
        sm = transformed["smart_meter_features"]
        aggregated["hourly_load_profile"] = aggregate_hourly_load_profile(sm)
        save_aggregated(aggregated["hourly_load_profile"], "hourly_load_profile")

        aggregated["peak_demand_calendar"] = aggregate_peak_demand_calendar(sm)
        save_aggregated(aggregated["peak_demand_calendar"], "peak_demand_calendar")

    # Dashboard export
    dashboard = create_dashboard_export(aggregated)
    out_path = Path(config["dashboard"]["output_path"]) / "dashboard_data.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    dashboard.to_csv(out_path, index=False)
    logger.info(f"Dashboard export saved: {out_path}")

    logger.info(f"=== Aggregation Complete. Tables: {list(aggregated.keys())} ===")
    return aggregated


if __name__ == "__main__":
    print("Run via main_etl.py for full pipeline execution.")
