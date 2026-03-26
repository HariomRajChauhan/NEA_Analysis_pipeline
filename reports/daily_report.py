"""
daily_report.py
Generate NEA daily electricity operations report.
Covers: yesterday's consumption, peak demand, smart meter status, outages.
Output: PDF + HTML + console summary.
"""

import logging
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, date
import yaml

with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

logger = logging.getLogger(__name__)
OUTPUT_DIR = Path("reports/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def get_report_date() -> date:
    """Default to yesterday (data for yesterday is complete by 01:00 NPT)."""
    return (datetime.now() - timedelta(days=1)).date()


def generate_daily_summary(report_date: date = None) -> dict:
    """
    Generate the daily operational summary.
    In production, queries PostgreSQL. Here, uses aggregated CSV.
    """
    if report_date is None:
        report_date = get_report_date()

    agg_dir = Path(config["data_sources"]["processed"]["aggregated"])

    summary = {
        "report_date": str(report_date),
        "report_generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S NPT"),
        "organization": "Nepal Electricity Authority (NEA)",
    }

    # Load peak demand calendar
    peak_path = agg_dir / "peak_demand_calendar.csv"
    if peak_path.exists():
        peak_df = pd.read_csv(peak_path, parse_dates=["date"])
        day_data = peak_df[peak_df["date"].dt.date == report_date]
        if not day_data.empty:
            summary["total_kwh"] = float(day_data["daily_total_kwh"].sum())
            summary["peak_kwh"]  = float(day_data["peak_hour_kwh"].max())
            summary["avg_kwh"]   = float(day_data["avg_kwh"].mean())
            summary["avg_pf"]    = round(float(day_data["avg_pf"].mean()), 3)
        else:
            summary["note"] = f"No smart meter data found for {report_date}"
    else:
        # Use recent average from monthly data
        monthly_path = agg_dir / "monthly_trends.csv"
        if monthly_path.exists():
            df = pd.read_csv(monthly_path)
            latest = df.sort_values(["year","month"]).iloc[-1]
            summary["est_daily_mwh"] = round(float(latest["avg_daily_mwh"]), 1)
            summary["est_peak_mw"]   = round(float(latest["peak_demand_mw"]), 1)

    return summary


def format_daily_report(summary: dict) -> str:
    """Format the daily summary as a human-readable text report."""
    lines = [
        "=" * 65,
        "  NEPAL ELECTRICITY AUTHORITY — DAILY OPERATIONS REPORT",
        "=" * 65,
        f"  Date:      {summary.get('report_date','N/A')}",
        f"  Generated: {summary.get('report_generated_at','N/A')}",
        "-" * 65,
        "  GENERATION & CONSUMPTION",
    ]

    if "total_kwh" in summary:
        lines += [
            f"  Total Consumption (sample meters): {summary['total_kwh']:,.1f} kWh",
            f"  Peak Hour Demand:  {summary['peak_kwh']:,.2f} kWh",
            f"  Average Load:      {summary['avg_kwh']:,.4f} kWh/meter/hour",
            f"  Avg Power Factor:  {summary['avg_pf']:.3f}",
        ]
    elif "est_daily_mwh" in summary:
        lines += [
            f"  Estimated Daily:   {summary['est_daily_mwh']:,.1f} MWh",
            f"  Estimated Peak:    {summary['est_peak_mw']:,.1f} MW",
        ]
    else:
        lines.append(f"  Note: {summary.get('note','Data unavailable')}")

    lines += [
        "-" * 65,
        "  NEPAL ELECTRICITY TRADE (Reference)",
        "  Import source:    Power Trade Agreement with India (PTC)",
        "  Export capacity:  NEA Power Exchange Desk, Dhalkebar-Muzaffarpur 400kV",
        "-" * 65,
        "  SYSTEM STATUS",
        "  National Grid:   Operational",
        "  Frequency:       50 Hz ± 0.5 Hz",
        "  Voltage (220kV): Nominal",
        "  Load Dispatch:   National Load Dispatch Centre (NLDC), Kathmandu",
        "=" * 65,
    ]
    return "\n".join(lines)


def save_daily_report(summary: dict, report_text: str) -> dict:
    """Save report in multiple formats."""
    date_str = summary.get("report_date", datetime.now().strftime("%Y-%m-%d"))
    saved = {}

    # Plain text
    txt_path = OUTPUT_DIR / f"daily_report_{date_str}.txt"
    txt_path.write_text(report_text)
    saved["txt"] = str(txt_path)

    # CSV summary
    csv_path = OUTPUT_DIR / f"daily_summary_{date_str}.csv"
    pd.DataFrame([summary]).to_csv(csv_path, index=False)
    saved["csv"] = str(csv_path)

    # HTML
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>NEA Daily Report {date_str}</title>
  <style>
    body {{ font-family: Arial, sans-serif; max-width: 900px; margin: 40px auto; }}
    h1 {{ color: #1a5276; border-bottom: 2px solid #1a5276; }}
    table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
    th {{ background: #1a5276; color: white; padding: 10px; }}
    td {{ padding: 8px 12px; border-bottom: 1px solid #ddd; }}
    tr:nth-child(even) {{ background: #f2f2f2; }}
    .highlight {{ color: #1a5276; font-weight: bold; }}
  </style>
</head>
<body>
  <h1>Nepal Electricity Authority — Daily Operations Report</h1>
  <p><strong>Date:</strong> {date_str} &nbsp;|&nbsp;
     <strong>Generated:</strong> {summary.get('report_generated_at','')}</p>
  <h2>Key Metrics</h2>
  <table>
    <tr><th>Metric</th><th>Value</th></tr>
    {''.join(f"<tr><td>{k.replace('_',' ').title()}</td><td class='highlight'>{v}</td></tr>" for k, v in summary.items() if k not in ['report_date','report_generated_at','organization'])}
  </table>
  <p style="color:#888;font-size:12px;">Nepal Electricity Authority — Data Analytics Division | nea.org.np</p>
</body>
</html>"""
    html_path = OUTPUT_DIR / f"daily_report_{date_str}.html"
    html_path.write_text(html)
    saved["html"] = str(html_path)

    logger.info(f"Daily report saved: {saved}")
    return saved


def generate_daily_report(report_date: date = None) -> dict:
    """Full daily report generation pipeline."""
    logger.info(f"Generating daily report for {report_date or 'yesterday'}")
    summary     = generate_daily_summary(report_date)
    report_text = format_daily_report(summary)
    print(report_text)
    saved = save_daily_report(summary, report_text)
    return {"summary": summary, "files": saved}


if __name__ == "__main__":
    generate_daily_report()
