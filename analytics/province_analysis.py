"""
province_analysis.py
Province-wise electricity consumption and infrastructure analysis for NEA.
Nepal has 7 provinces with wide variation in electrification,
consumption patterns, and system loss levels.
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


PROVINCE_CONTEXT = {
    1: {"name": "Koshi",         "note": "Eastern hub; tea industry, Arun III upcoming"},
    2: {"name": "Madhesh",       "note": "Dense Terai; agriculture dominant; high losses"},
    3: {"name": "Bagmati",       "note": "Kathmandu Valley; 45% of national GDP; lowest loss"},
    4: {"name": "Gandaki",       "note": "Pokhara tourism hub; major hydro installed"},
    5: {"name": "Lumbini",       "note": "Buddhist circuit; growing manufacturing in Butwal"},
    6: {"name": "Karnali",       "note": "Lowest electrification; high mountains; lowest demand"},
    7: {"name": "Sudurpashchim", "note": "Remote far-west; cross-border trade with India"},
}


def load_province_data() -> pd.DataFrame:
    path = AGG_DIR / "province_ranking.csv"
    if path.exists():
        return pd.read_csv(path)
    from etl.extract.extract_open_data import run_extraction
    from etl.transform.clean_data import clean_province_data
    raw = run_extraction()
    return clean_province_data(raw["province_electricity"])


def analyse_electrification_gap(df: pd.DataFrame) -> pd.DataFrame:
    """
    Quantify electrification gap by province.
    Karnali (83.7%) and Sudurpashchim (87.4%) lag significantly
    behind Bagmati (98.5%) and Koshi (94.8%).
    """
    df = df.copy()
    df["unelectrified_households"] = (
        df["households_2021"] * (1 - df["electrification_pct_2021"] / 100)
    ).round(0).astype(int)

    # Estimated connection cost at NPR 25,000 per household (NEA standard estimate)
    df["est_connection_cost_npr_mn"] = (
        df["unelectrified_households"] * 25_000 / 1_000_000
    ).round(2)

    df["gap_vs_national_avg_pp"] = (
        df["electrification_pct_2021"] - df["electrification_pct_2021"].mean()
    ).round(2)

    return df.sort_values("electrification_pct_2021")[
        ["province_name","province_id","electrification_pct_2021",
         "unelectrified_households","est_connection_cost_npr_mn","gap_vs_national_avg_pp"]
    ]


def analyse_consumption_inequality(df: pd.DataFrame) -> dict:
    """
    Measure disparity in per-capita electricity consumption across provinces.
    Gini coefficient and ratio of highest to lowest province.
    """
    values = df["per_capita_kwh_2023"].dropna().sort_values().values
    n = len(values)

    # Gini coefficient
    index = np.arange(1, n+1)
    gini = ((2 * index - n - 1) * values).sum() / (n * values.sum())

    return {
        "max_province": df.loc[df["per_capita_kwh_2023"].idxmax(), "province_name"],
        "max_per_capita_kwh": float(df["per_capita_kwh_2023"].max()),
        "min_province": df.loc[df["per_capita_kwh_2023"].idxmin(), "province_name"],
        "min_per_capita_kwh": float(df["per_capita_kwh_2023"].min()),
        "ratio_max_to_min": round(
            float(df["per_capita_kwh_2023"].max() / df["per_capita_kwh_2023"].min()), 2
        ),
        "gini_coefficient": round(float(gini), 4),
        "national_avg_kwh": round(float(df["per_capita_kwh_2023"].mean()), 1),
    }


def analyse_system_losses(df: pd.DataFrame) -> pd.DataFrame:
    """
    System loss analysis by province.
    NEA target: reduce total system loss to <15% by FY 2027/28.
    Madhesh and Sudurpashchim have the highest losses due to
    long distribution lines in remote areas.
    """
    df = df.copy()

    df["energy_lost_gwh"] = (
        df["annual_consumption_gwh_2023"] * df["system_loss_pct"] / 100
    ).round(3)

    # Revenue lost to system losses at average tariff NPR 9/kWh
    df["revenue_lost_npr_mn"] = (
        df["energy_lost_gwh"] * 1e6 * 9 / 1e6
    ).round(2)

    # If all provinces achieve 15% loss target
    target_loss = 15.0
    df["recoverable_gwh"] = (
        df["annual_consumption_gwh_2023"] *
        (df["system_loss_pct"] - target_loss).clip(lower=0) / 100
    ).round(3)

    df["recoverable_revenue_npr_mn"] = (
        df["recoverable_gwh"] * 1e6 * 9 / 1e6
    ).round(2)

    return df.sort_values("system_loss_pct", ascending=False)[
        ["province_name","system_loss_pct","energy_lost_gwh",
         "revenue_lost_npr_mn","recoverable_gwh","recoverable_revenue_npr_mn"]
    ]


def rank_provinces(df: pd.DataFrame) -> pd.DataFrame:
    """Composite province ranking on 4 key electricity metrics."""
    df = df.copy()

    rank_config = [
        ("per_capita_kwh_2023",       False, "Consumption per capita"),
        ("electrification_pct_2021",  False, "Electrification rate"),
        ("system_loss_pct",           True,  "System efficiency (lower loss = better)"),
        ("installed_capacity_mw_2023",False, "Generation capacity"),
    ]

    for col, asc, _ in rank_config:
        if col in df.columns:
            df[f"rank_{col}"] = df[col].rank(ascending=asc, method="min").astype(int)

    rank_cols = [f"rank_{c}" for c, _, _ in rank_config if f"rank_{c}" in df.columns]
    if rank_cols:
        df["composite_score"] = df[rank_cols].mean(axis=1).round(2)
        df["overall_rank"]    = df["composite_score"].rank(method="min").astype(int)

    return df.sort_values("overall_rank")[
        ["province_name","overall_rank","composite_score"] + rank_cols
    ]


def run_province_analysis() -> dict:
    """Run all province analysis modules."""
    logger.info("=== Province Analysis ===")
    df = load_province_data()

    results = {
        "electrification_gap": analyse_electrification_gap(df),
        "consumption_inequality": analyse_consumption_inequality(df),
        "system_losses": analyse_system_losses(df),
        "province_ranking": rank_provinces(df),
    }

    print("\n=== PROVINCE ANALYSIS SUMMARY ===")
    print("\n── Electrification Gap ──")
    print(results["electrification_gap"].to_string(index=False))

    print("\n── Consumption Inequality ──")
    for k, v in results["consumption_inequality"].items():
        print(f"  {k}: {v}")

    print("\n── System Losses ──")
    print(results["system_losses"].to_string(index=False))

    print("\n── Province Ranking ──")
    print(results["province_ranking"].to_string(index=False))

    return results


if __name__ == "__main__":
    run_province_analysis()
