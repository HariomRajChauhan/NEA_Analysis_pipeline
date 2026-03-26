"""
monthly_report.py
Nepal Electricity Authority monthly electricity consumption report.
Covers: sector analysis, province comparison, revenue estimation,
        seasonal patterns, and month-over-month trends.
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
OUTPUT_DIR = Path("reports/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
AGG_DIR = Path(config["data_sources"]["processed"]["aggregated"])


def build_monthly_report_data(year: int = None, month: int = None) -> dict:
    """Assemble all data needed for the monthly report."""
    if year is None:
        year = datetime.now().year
    if month is None:
        month = datetime.now().month - 1 or 12

    report_data = {"year": year, "month": month}

    # Monthly trends
    path = AGG_DIR / "monthly_trends.csv"
    if path.exists():
        df = pd.read_csv(path)
        report_data["monthly_df"] = df
        month_row = df[(df["year"] == year) & (df["month"] == month)]
        if not month_row.empty:
            report_data["current_month"] = month_row.iloc[0].to_dict()

    # Province ranking
    prov_path = AGG_DIR / "province_ranking.csv"
    if prov_path.exists():
        report_data["province_df"] = pd.read_csv(prov_path)

    # System losses
    loss_path = AGG_DIR / "system_losses_by_province.csv"
    if loss_path.exists():
        report_data["losses_df"] = pd.read_csv(loss_path)

    # Annual KPIs
    kpi_path = AGG_DIR / "annual_kpi_summary.csv"
    if kpi_path.exists():
        report_data["annual_df"] = pd.read_csv(kpi_path)

    return report_data


def format_monthly_report(data: dict) -> str:
    """Format monthly report as detailed text."""
    year  = data["year"]
    month = data["month"]
    month_names = {1:"January",2:"February",3:"March",4:"April",5:"May",6:"June",
                   7:"July",8:"August",9:"September",10:"October",11:"November",12:"December"}
    month_name = month_names.get(month, str(month))

    lines = [
        "=" * 70,
        "  NEPAL ELECTRICITY AUTHORITY",
        f"  MONTHLY ELECTRICITY CONSUMPTION REPORT — {month_name.upper()} {year}",
        "=" * 70,
        f"  Prepared by: NEA Data Analytics Division",
        f"  Date:        {datetime.now().strftime('%B %d, %Y')}",
        f"  Fiscal Year: {'2024/25' if month >= 7 else '2023/24'} (Nepali FY)",
        "",
    ]

    # Current month metrics
    if "current_month" in data:
        cm = data["current_month"]
        lines += [
            "  1. MONTHLY CONSUMPTION SUMMARY",
            "  " + "-" * 60,
            f"  Total Consumption:    {cm.get('total_gwh', 'N/A'):.3f} GWh",
            f"  Domestic:             {cm.get('domestic_gwh', 'N/A'):.3f} GWh",
            f"  Industrial:           {cm.get('industrial_gwh', 'N/A'):.3f} GWh",
            f"  Commercial:           {cm.get('commercial_gwh', 'N/A'):.3f} GWh",
            f"  Irrigation:           {cm.get('irrigation_gwh', 'N/A'):.3f} GWh",
            f"  Peak Demand:          {cm.get('peak_demand_mw', 'N/A'):.1f} MW",
            f"  Load Factor:          {cm.get('load_factor_pct', 'N/A'):.1f}%",
            f"  Seasonal Index:       {cm.get('seasonal_index', 'N/A'):.4f}",
            f"  MoM Growth:           {cm.get('growth_rate_pct', 0.0):.2f}%",
            "",
        ]

    # Province summary
    if "province_df" in data:
        prov = data["province_df"]
        lines += [
            "  2. PROVINCE-WISE PERFORMANCE",
            "  " + "-" * 60,
            f"  {'Province':<20} {'GWh (2023)':<14} {'Per Capita':<12} {'Loss %':<10} {'Elec. %'}",
            "  " + "-" * 60,
        ]
        for _, r in prov.sort_values("annual_consumption_gwh_2023", ascending=False).iterrows():
            lines.append(
                f"  {r.get('province_name',''):<20} "
                f"{r.get('annual_consumption_gwh_2023', 0):>10.1f}     "
                f"{r.get('per_capita_kwh_2023', 0):>8.0f}     "
                f"{r.get('system_loss_pct', 0):>6.1f}%    "
                f"{r.get('electrification_pct_2021', 0):.1f}%"
            )
        lines.append("")

    # System loss summary
    if "losses_df" in data:
        loss_df = data["losses_df"]
        total_loss_gwh = loss_df["estimated_loss_gwh"].sum()
        total_recoverable = loss_df["potential_saving_gwh"].sum()
        lines += [
            "  3. SYSTEM LOSS ANALYSIS",
            "  " + "-" * 60,
            f"  Total estimated loss:       {total_loss_gwh:.2f} GWh/year",
            f"  Revenue impact:             NPR {(loss_df['estimated_loss_gwh'] * 9).sum():.1f} million/year",
            f"  Recoverable if 15% target:  {total_recoverable:.2f} GWh/year",
            f"  Highest loss province:      {loss_df.iloc[0]['province_name']} ({loss_df.iloc[0]['system_loss_pct']:.1f}%)",
            f"  Lowest loss province:       {loss_df.iloc[-1]['province_name']} ({loss_df.iloc[-1]['system_loss_pct']:.1f}%)",
            "",
        ]

    lines += [
        "  4. KEY HIGHLIGHTS — NEPAL CONTEXT",
        "  " + "-" * 60,
        "  • Upper Tamakoshi (456 MW) continues full operation",
        "  • NEA electricity export to India via Dhalkebar-Muzaffarpur 400kV line",
        "  • EV charging stations under NEA policy expansion",
        "  • Rural electrification progress: Karnali Province focus area",
        "  • Arun III (900 MW) construction ongoing in Koshi Province",
        "",
        "  5. NEXT MONTH OUTLOOK",
        "  " + "-" * 60,
        "  • Monitor pre-monsoon temperature rise (Terai AC load)",
        "  • Track hydropower reservoir levels (Kulekhani, Upper Tamakoshi)",
        "  • Review smart meter rollout milestones",
        "",
        "=" * 70,
        "  Nepal Electricity Authority | नेपाल विद्युत् प्राधिकरण",
        "  Durbarmarg, Kathmandu, Nepal | nea.org.np",
        "=" * 70,
    ]

    return "\n".join(lines)


def save_monthly_report(report_text: str, year: int, month: int) -> dict:
    """Save monthly report in multiple formats."""
    month_str = f"{year}-{month:02d}"
    saved = {}

    txt_path = OUTPUT_DIR / f"monthly_report_{month_str}.txt"
    txt_path.write_text(report_text)
    saved["txt"] = str(txt_path)

    logger.info(f"Monthly report saved: {saved}")
    return saved


def generate_monthly_report(year: int = None, month: int = None) -> dict:
    """Full monthly report generation."""
    logger.info(f"Generating monthly report for {year}/{month}")
    data        = build_monthly_report_data(year, month)
    report_text = format_monthly_report(data)
    print(report_text)
    saved = save_monthly_report(report_text, data["year"], data["month"])
    return {"data": data, "files": saved}


if __name__ == "__main__":
    generate_monthly_report()
