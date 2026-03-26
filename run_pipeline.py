"""
run_pipeline.py
Top-level entry point for Nepal Electricity Analytics Pipeline.
Usage:
    python run_pipeline.py                        # Full pipeline
    python run_pipeline.py --mode extract          # Extract only
    python run_pipeline.py --skip-load            # ETL without DB load
    python run_pipeline.py --dry-run              # Validate without output
    python run_pipeline.py --report daily         # Generate daily report
    python run_pipeline.py --report monthly       # Generate monthly report
    python run_pipeline.py --analytics all        # Run all analytics modules
"""

import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))


def parse_args():
    parser = argparse.ArgumentParser(
        description="Nepal Electricity Analytics Pipeline — Entry Point",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py                          # Full ETL pipeline
  python run_pipeline.py --mode extract           # Extract only
  python run_pipeline.py --skip-load             # ETL, skip DB write
  python run_pipeline.py --report daily           # Daily ops report
  python run_pipeline.py --report monthly         # Monthly report
  python run_pipeline.py --analytics consumption  # Consumption analysis
  python run_pipeline.py --analytics all          # All analytics
        """
    )
    parser.add_argument(
        "--mode", choices=["full","extract","transform","load","validate"],
        default="full"
    )
    parser.add_argument(
        "--report", choices=["daily","monthly","yearly"],
        help="Generate a specific report"
    )
    parser.add_argument(
        "--analytics",
        choices=["consumption","peak","weather","province","all"],
        help="Run a specific analytics module"
    )
    parser.add_argument("--skip-load",  action="store_true")
    parser.add_argument("--dry-run",    action="store_true")
    parser.add_argument("--start-date", help="YYYY-MM-DD")
    parser.add_argument("--end-date",   help="YYYY-MM-DD")
    return parser.parse_args()


def run_report(report_type: str):
    if report_type == "daily":
        from reports.daily_report import generate_daily_report
        generate_daily_report()
    elif report_type == "monthly":
        from reports.monthly_report import generate_monthly_report
        generate_monthly_report()
    elif report_type == "yearly":
        from reports.yearly_report import generate_yearly_report
        generate_yearly_report()


def run_analytics(analytics_type: str):
    if analytics_type in ["consumption", "all"]:
        from analytics.consumption_analysis import run_consumption_analysis
        run_consumption_analysis()
    if analytics_type in ["peak", "all"]:
        from analytics.peak_demand_analysis import run_peak_demand_analysis
        run_peak_demand_analysis()
    if analytics_type in ["weather", "all"]:
        from analytics.weather_usage_analysis import run_weather_analysis
        run_weather_analysis()
    if analytics_type in ["province", "all"]:
        from analytics.province_analysis import run_province_analysis
        run_province_analysis()


def main():
    args = parse_args()

    if args.report:
        run_report(args.report)
    elif args.analytics:
        run_analytics(args.analytics)
    else:
        from etl.main_etl import main as etl_main
        sys.argv = [sys.argv[0]]  # Reset argv for etl_main's argparse
        if args.mode != "full":
            sys.argv += ["--mode", args.mode]
        if args.skip_load:
            sys.argv += ["--skip-load"]
        if args.dry_run:
            sys.argv += ["--dry-run"]
        if args.start_date:
            sys.argv += ["--start-date", args.start_date]
        if args.end_date:
            sys.argv += ["--end-date", args.end_date]
        etl_main()


if __name__ == "__main__":
    main()
