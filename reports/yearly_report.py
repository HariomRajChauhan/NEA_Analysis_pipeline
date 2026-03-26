"""
yearly_report.py
Nepal Electricity Authority annual board-level electricity report.
Covers: national KPIs, 9-year trends, 5-year forecast, hydropower pipeline,
        province scorecard, trade balance, and strategic recommendations.
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


def build_yearly_report_data(fiscal_year: str = None) -> dict:
    """Assemble multi-source data for the annual report."""
    data = {"fiscal_year": fiscal_year or "2023/24"}

    annual_path = AGG_DIR / "annual_kpi_summary.csv"
    if annual_path.exists():
        data["annual_df"] = pd.read_csv(annual_path)

    prov_path = AGG_DIR / "province_ranking.csv"
    if prov_path.exists():
        data["province_df"] = pd.read_csv(prov_path)

    loss_path = AGG_DIR / "system_losses_by_province.csv"
    if loss_path.exists():
        data["losses_df"] = pd.read_csv(loss_path)

    return data


def compute_9yr_summary(df: pd.DataFrame) -> dict:
    """Compute 9-year performance summary for the board report."""
    df = df.sort_values("fiscal_year")
    first, last = df.iloc[0], df.iloc[-1]
    n = len(df)

    def cagr(end, start, yrs):
        return round(((end / start) ** (1 / yrs) - 1) * 100, 2) if start > 0 else 0

    return {
        "period":                   f"{first['fiscal_year']} – {last['fiscal_year']}",
        "total_gwh_start":          first["total_energy_gwh"],
        "total_gwh_end":            last["total_energy_gwh"],
        "consumption_cagr_pct":     cagr(last["total_energy_gwh"], first["total_energy_gwh"], n - 1),
        "per_capita_start_kwh":     first["per_capita_kwh"],
        "per_capita_end_kwh":       last["per_capita_kwh"],
        "per_capita_cagr_pct":      cagr(last["per_capita_kwh"], first["per_capita_kwh"], n - 1),
        "peak_demand_start_mw":     first["peak_demand_mw"],
        "peak_demand_end_mw":       last["peak_demand_mw"],
        "consumers_start":          int(first["total_consumers"]),
        "consumers_end":            int(last["total_consumers"]),
        "system_loss_start_pct":    first["system_loss_pct"],
        "system_loss_end_pct":      last["system_loss_pct"],
        "loss_improvement_pp":      round(first["system_loss_pct"] - last["system_loss_pct"], 2),
        "capacity_start_mw":        first["installed_capacity_mw"],
        "capacity_end_mw":          last["installed_capacity_mw"],
        "capacity_added_mw":        round(last["installed_capacity_mw"] - first["installed_capacity_mw"], 1),
    }


def compute_demand_forecast(df: pd.DataFrame, horizon: int = 5) -> list:
    """Log-linear 5-year demand forecast."""
    df = df.sort_values("fiscal_year").reset_index(drop=True)
    t = np.arange(len(df))
    log_gwh = np.log(df["total_energy_gwh"].values)
    slope, intercept = np.polyfit(t, log_gwh, 1)
    cagr = (np.exp(slope) - 1) * 100

    last_year = int(df["fiscal_year"].iloc[-1][:4])
    last_t    = len(df) - 1

    forecasts = []
    for i in range(1, horizon + 1):
        fy  = f"{last_year + i}/{str(last_year + i + 1)[-2:]}"
        gwh = round(np.exp(intercept + slope * (last_t + i)), 1)
        forecasts.append({"fiscal_year": fy, "forecast_gwh": gwh, "cagr_assumption_pct": round(cagr, 2)})
    return forecasts


def format_yearly_report(data: dict) -> str:
    """Render the full annual report as formatted text."""
    fy  = data["fiscal_year"]
    now = datetime.now().strftime("%B %d, %Y")
    sep = "=" * 72

    lines = [
        sep,
        "  NEPAL ELECTRICITY AUTHORITY",
        f"  ANNUAL ELECTRICITY REPORT — FISCAL YEAR {fy}",
        "  नेपाल विद्युत् प्राधिकरण — वार्षिक विद्युत् प्रतिवेदन",
        sep,
        f"  Prepared: {now}  |  Prepared by: NEA Data Analytics Division",
        f"  Classification: Public  |  Distributed to: Board, MoEWRI, GoN",
        "",
    ]

    # ── Section 1: Executive Summary ─────────────────────────────────────
    lines += [
        "  SECTION 1 — EXECUTIVE SUMMARY",
        "  " + "-" * 60,
    ]

    if "annual_df" in data:
        s = compute_9yr_summary(data["annual_df"])
        latest = data["annual_df"].sort_values("fiscal_year").iloc[-1]
        lines += [
            f"  Reporting Fiscal Year:    {fy}",
            f"  Total Consumption:        {latest['total_energy_gwh']:,.1f} GWh",
            f"  Peak Demand:              {latest['peak_demand_mw']:,.1f} MW",
            f"  Total Consumers:          {int(latest['total_consumers']):,}",
            f"  Per Capita Consumption:   {latest['per_capita_kwh']:,.0f} kWh",
            f"  System Loss:              {latest['system_loss_pct']:.1f}%",
            f"  Installed Capacity:       {latest['installed_capacity_mw']:,.1f} MW",
            f"  Energy Exported:          {latest.get('energy_exported_gwh', 0):,.1f} GWh",
            f"  Energy Imported:          {latest.get('energy_imported_gwh', 0):,.1f} GWh",
            "",
        ]

    # ── Section 2: 9-Year Performance ────────────────────────────────────
    lines += ["  SECTION 2 — NINE-YEAR PERFORMANCE REVIEW (2015/16 – 2023/24)",
              "  " + "-" * 60]

    if "annual_df" in data:
        s = compute_9yr_summary(data["annual_df"])
        lines += [
            f"  {'Metric':<40} {'2015/16':>10}  {'2023/24':>10}  {'Change':>10}",
            "  " + "-" * 60,
            f"  {'Total Consumption (GWh)':<40} {s['total_gwh_start']:>10,.1f}  {s['total_gwh_end']:>10,.1f}  CAGR {s['consumption_cagr_pct']:>4.1f}%",
            f"  {'Per Capita Consumption (kWh)':<40} {s['per_capita_start_kwh']:>10,.0f}  {s['per_capita_end_kwh']:>10,.0f}  CAGR {s['per_capita_cagr_pct']:>4.1f}%",
            f"  {'Peak Demand (MW)':<40} {s['peak_demand_start_mw']:>10,.1f}  {s['peak_demand_end_mw']:>10,.1f}",
            f"  {'Total Consumers':<40} {s['consumers_start']:>10,}  {s['consumers_end']:>10,}",
            f"  {'System Loss (%)':<40} {s['system_loss_start_pct']:>10.1f}  {s['system_loss_end_pct']:>10.1f}  -{s['loss_improvement_pp']:.1f}pp",
            f"  {'Installed Capacity (MW)':<40} {s['capacity_start_mw']:>10,.1f}  {s['capacity_end_mw']:>10,.1f}  +{s['capacity_added_mw']:,.1f} MW",
            "",
            f"  Key Milestone: Nepal became a NET ELECTRICITY EXPORTER in FY 2022/23,",
            f"  driven by Upper Tamakoshi HPP (456 MW) commissioned in FY 2021/22.",
            "",
        ]

    # ── Section 3: 5-Year Demand Forecast ────────────────────────────────
    lines += ["  SECTION 3 — FIVE-YEAR DEMAND FORECAST (2024/25 – 2028/29)",
              "  " + "-" * 60]

    if "annual_df" in data:
        forecasts = compute_demand_forecast(data["annual_df"])
        lines.append(f"  {'Fiscal Year':<14} {'Forecast GWh':>14}  {'Assumed CAGR':>14}")
        lines.append("  " + "-" * 44)
        for f in forecasts:
            lines.append(
                f"  {f['fiscal_year']:<14} {f['forecast_gwh']:>14,.1f}  {f['cagr_assumption_pct']:>13.2f}%"
            )
        lines += [
            "",
            "  Forecast methodology: Log-linear trend regression on 9-year historical data.",
            "  Key assumption: GDP growth 5–6%, electrification expansion, EV adoption.",
            "",
        ]

    # ── Section 4: Province Scorecard ────────────────────────────────────
    lines += ["  SECTION 4 — PROVINCE ELECTRICITY SCORECARD",
              "  " + "-" * 60]

    if "province_df" in data:
        pdf = data["province_df"]
        lines.append(
            f"  {'Province':<20} {'GWh':>8} {'kWh/cap':>9} {'Elec%':>7} {'Loss%':>7} {'Rank'}"
        )
        lines.append("  " + "-" * 60)
        for _, r in pdf.sort_values("annual_consumption_gwh_2023", ascending=False).iterrows():
            lines.append(
                f"  {str(r.get('province_name','')):<20}"
                f" {r.get('annual_consumption_gwh_2023', 0):>8.1f}"
                f" {r.get('per_capita_kwh_2023', 0):>9.0f}"
                f" {r.get('electrification_pct_2021', 0):>6.1f}%"
                f" {r.get('system_loss_pct', 0):>6.1f}%"
                f" {int(r.get('rank_per_capita', 0)):>5}"
            )
        lines.append("")

    # ── Section 5: System Loss Analysis ──────────────────────────────────
    if "losses_df" in data:
        ldf = data["losses_df"]
        total_loss    = ldf["estimated_loss_gwh"].sum()
        total_rev_loss = (ldf["estimated_loss_gwh"] * 9).sum()
        total_recover  = ldf["potential_saving_gwh"].sum()
        lines += [
            "  SECTION 5 — SYSTEM LOSS ANALYSIS",
            "  " + "-" * 60,
            f"  Total estimated system loss:            {total_loss:.2f} GWh/year",
            f"  Revenue impact @ avg NPR 9/kWh:        NPR {total_rev_loss:.1f} million/year",
            f"  Recoverable GWh (if all reach 15%):    {total_recover:.2f} GWh/year",
            f"  NEA target:                             <15% by FY 2027/28",
            "",
            f"  Highest loss province:  {ldf.iloc[0]['province_name']} ({ldf.iloc[0]['system_loss_pct']:.1f}%)",
            f"  Lowest loss province:   {ldf.iloc[-1]['province_name']} ({ldf.iloc[-1]['system_loss_pct']:.1f}%)",
            "",
        ]

    # ── Section 6: Hydropower Pipeline ───────────────────────────────────
    lines += [
        "  SECTION 6 — HYDROPOWER DEVELOPMENT PIPELINE",
        "  " + "-" * 60,
        "  Operating Projects (>100 MW):",
        f"  {'Project':<28} {'Capacity (MW)':>14}  {'Province':<14}  Status",
        "  " + "-" * 60,
        f"  {'Upper Tamakoshi':<28} {'456':>14}  {'Bagmati':<14}  Operating (2021)",
        f"  {'Kaligandaki A':<28} {'144':>14}  {'Lumbini':<14}  Operating (2002)",
        f"  {'Middle Marsyangdi':<28} {'70':>14}  {'Gandaki':<14}  Operating (2008)",
        "",
        "  Under Construction:",
        f"  {'Arun III (SJVNL)':<28} {'900':>14}  {'Koshi':<14}  Expected 2026",
        f"  {'Upper Arun':<28} {'639':>14}  {'Koshi':<14}  Expected 2028",
        "",
        "  Planning / DPR Stage:",
        f"  {'Tamor Storage':<28} {'762':>14}  {'Koshi':<14}  DPR underway",
        f"  {'West Seti':<28} {'750':>14}  {'Sudurpashchim':<14}  Environmental clearance",
        f"  {'Pancheshwar (India-Nepal)':<28} {'6,480':>14}  {'Sudurpashchim':<14}  Joint project",
        "",
    ]

    # ── Section 7: Strategic Recommendations ─────────────────────────────
    lines += [
        "  SECTION 7 — STRATEGIC RECOMMENDATIONS",
        "  " + "-" * 60,
        "  1. SYSTEM LOSS REDUCTION",
        "     Target: Reduce system loss from 15.8% to 13% by FY 2026/27.",
        "     Priority provinces: Karnali (21.4%), Sudurpashchim (20.8%), Madhesh (18.9%).",
        "     Action: Replace aged distribution transformers; deploy smart metering.",
        "",
        "  2. RURAL ELECTRIFICATION",
        "     Karnali has 16.3% unelectrified households (~67,800 HH).",
        "     Estimated connection cost: NPR 1,695 million.",
        "     Action: Prioritise off-grid solar + mini-hydro in remote VDCs.",
        "",
        "  3. ELECTRICITY EXPORT EXPANSION",
        "     Nepal exported 1,950 GWh in FY 2023/24 via Dhalkebar-Muzaffarpur 400kV.",
        "     New cross-border transmission lines needed for Arun III export.",
        "     Action: Expedite Butwal-Gorakhpur 400kV line (1,000 MW capacity).",
        "",
        "  4. SMART METERING ROLLOUT",
        "     Current: ~100 AMI meters (pilot). Target: 500,000 by FY 2027/28.",
        "     Benefits: Real-time demand data, outage detection, revenue assurance.",
        "     Action: Accelerate procurement under SASEC Power Connectivity project.",
        "",
        "  5. EV CHARGING INFRASTRUCTURE",
        "     EV tariff (NPR 8/kWh) approved. <50 public charging stations installed.",
        "     Action: Partner with private operators; target 500 stations by 2026.",
        "",
        "  6. TIME-OF-USE (ToU) TARIFF PILOT",
        "     Evening peak (18:00-21:00) causes grid stress. ToU incentives can shift",
        "     industrial load to off-peak hours, improving load factor by 5–8%.",
        "     Action: Launch 6-month pilot with 50 industrial consumers in FY 2024/25.",
        "",
    ]

    lines += [
        sep,
        "  NEPAL ELECTRICITY AUTHORITY | नेपाल विद्युत् प्राधिकरण",
        "  Durbarmarg, Kathmandu 44600, Nepal",
        "  Tel: +977-1-4153000  |  nea.org.np  |  info@nea.org.np",
        sep,
    ]

    return "\n".join(lines)


def save_yearly_report(report_text: str, fiscal_year: str) -> dict:
    fy_safe = fiscal_year.replace("/", "-")
    saved   = {}

    txt_path = OUTPUT_DIR / f"yearly_report_{fy_safe}.txt"
    txt_path.write_text(report_text, encoding="utf-8")
    saved["txt"] = str(txt_path)

    logger.info(f"Yearly report saved: {saved}")
    return saved


def generate_yearly_report(fiscal_year: str = None) -> dict:
    if fiscal_year is None:
        fiscal_year = "2023/24"
    logger.info(f"Generating annual report for FY {fiscal_year}")
    data        = build_yearly_report_data(fiscal_year)
    report_text = format_yearly_report(data)
    print(report_text)
    saved       = save_yearly_report(report_text, fiscal_year)
    return {"data": data, "files": saved}


if __name__ == "__main__":
    generate_yearly_report()
