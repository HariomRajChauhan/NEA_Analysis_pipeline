"""
main_etl.py
Master orchestration for Nepal Electricity Analytics ETL Pipeline.
Runs: Extract → Clean → Transform → Aggregate → Load
Can be triggered manually, by cron, or by Airflow DAG.
"""

import sys
import logging
import argparse
import time
from datetime import datetime
from pathlib import Path
import yaml

# Ensure project root is in path
sys.path.insert(0, str(Path(__file__).resolve().parent))

with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

# Configure logging
log_dir = Path(config["logging"]["log_dir"])
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=getattr(logging, config["logging"]["level"]),
    format=config["logging"]["format"],
    handlers=[
        logging.FileHandler(log_dir / config["logging"]["etl_log"]),
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger("main_etl")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Nepal Electricity Analytics ETL Pipeline"
    )
    parser.add_argument(
        "--mode", choices=["full","extract","transform","load","validate"],
        default="full", help="Pipeline mode to run"
    )
    parser.add_argument("--start-date", help="Filter start date YYYY-MM-DD")
    parser.add_argument("--end-date",   help="Filter end date YYYY-MM-DD")
    parser.add_argument("--skip-load",  action="store_true",
                        help="Run ETL without loading to database")
    parser.add_argument("--dry-run",    action="store_true",
                        help="Run pipeline but do not write any output")
    return parser.parse_args()


def run_extract(args) -> dict:
    """Run all extraction modules."""
    logger.info("── EXTRACT stage ──────────────────────────────")
    from etl.extract.extract_nea import run_extraction as nea_ex
    from etl.extract.extract_smart_meter import run_extraction as sm_ex
    from etl.extract.extract_weather_api import run_extraction as wx_ex
    from etl.extract.extract_open_data import run_extraction as od_ex

    raw = {}
    raw.update(nea_ex())
    raw.update(sm_ex())
    raw.update(wx_ex())
    raw.update(od_ex())

    logger.info(f"Extracted datasets: {list(raw.keys())}")
    return raw


def run_clean(raw: dict, args) -> dict:
    """Run data cleaning."""
    logger.info("── CLEAN stage ────────────────────────────────")
    from etl.transform.clean_data import run_cleaning
    return run_cleaning(raw)


def run_transform(cleaned: dict, args) -> dict:
    """Run feature engineering and transformation."""
    logger.info("── TRANSFORM stage ────────────────────────────")
    from etl.transform.transform_data import run_transformation
    return run_transformation(cleaned)


def run_aggregate(transformed: dict, cleaned: dict, args) -> dict:
    """Run aggregation."""
    logger.info("── AGGREGATE stage ─────────────────────────────")
    from etl.transform.aggregate_data import run_aggregation
    return run_aggregation(transformed, cleaned)


def run_load(transformed: dict, aggregated: dict, args) -> dict:
    """Load data to PostgreSQL."""
    if args.skip_load or args.dry_run:
        logger.info("── LOAD skipped (--skip-load or --dry-run) ────")
        return {}
    logger.info("── LOAD stage ─────────────────────────────────")
    from etl.load.load_to_postgres import run_load as do_load
    return do_load(transformed, aggregated)


def run_validation(transformed: dict, aggregated: dict) -> bool:
    """
    Basic data quality validation checks.
    Returns True if all checks pass.
    """
    logger.info("── VALIDATE stage ──────────────────────────────")
    checks_passed = True

    # Check 1: Annual consumption should be increasing (Nepal's growing demand)
    if "annual" in transformed:
        annual = transformed["annual"].sort_values("fiscal_year")
        if len(annual) > 1:
            is_growing = (
                annual["total_energy_gwh"].diff().dropna() > 0
            ).mean() >= 0.7  # at least 70% years show growth
            if not is_growing:
                logger.warning("VALIDATION WARN: Annual consumption trend not consistently growing")
                checks_passed = False
            else:
                logger.info("VALIDATION OK: Annual consumption trend is upward")

    # Check 2: Smart meter voltage should be within Nepal grid tolerance
    if "smart_meter_features" in transformed:
        sm = transformed["smart_meter_features"]
        voltage_ok = sm["voltage_flag"].mean() < 0.05  # <5% flagged
        if not voltage_ok:
            logger.warning(f"VALIDATION WARN: {sm['voltage_flag'].mean()*100:.1f}% meters have voltage issues")
        else:
            logger.info("VALIDATION OK: Smart meter voltage within tolerance")

    # Check 3: System loss should be 10–30% (Nepal typical range)
    if "province_enriched" in transformed:
        pe = transformed["province_enriched"]
        loss_ok = pe["system_loss_pct"].between(10, 35).all()
        if not loss_ok:
            logger.warning("VALIDATION WARN: Province system loss outside expected range")
        else:
            logger.info("VALIDATION OK: System loss values within expected range")

    # Check 4: Monthly totals should be positive
    if "monthly_revenue" in transformed:
        mr = transformed["monthly_revenue"]
        pos_ok = (mr["total_gwh"] > 0).all()
        if not pos_ok:
            logger.error("VALIDATION FAIL: Negative monthly consumption detected")
            checks_passed = False
        else:
            logger.info("VALIDATION OK: All monthly consumption values positive")

    return checks_passed


def main():
    args = parse_args()
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("Nepal Electricity Analytics Pipeline — START")
    logger.info(f"Mode: {args.mode} | Dry-run: {args.dry_run}")
    logger.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')} NPT")
    logger.info("=" * 60)

    try:
        raw, cleaned, transformed, aggregated = {}, {}, {}, {}

        if args.mode in ["full", "extract"]:
            raw = run_extract(args)

        if args.mode in ["full", "transform"] and raw:
            cleaned   = run_clean(raw, args)
            transformed = run_transform(cleaned, args)
            aggregated  = run_aggregate(transformed, cleaned, args)

        if args.mode in ["full", "load"] and transformed:
            load_stats = run_load(transformed, aggregated, args)
            logger.info(f"Load stats: {load_stats}")

        if args.mode in ["full", "validate"] and transformed:
            passed = run_validation(transformed, aggregated)
            if not passed:
                logger.warning("Validation completed with warnings")

        duration = (datetime.now() - start_time).total_seconds()
        logger.info("=" * 60)
        logger.info(f"Pipeline COMPLETED in {duration:.1f}s")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Pipeline FAILED: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
