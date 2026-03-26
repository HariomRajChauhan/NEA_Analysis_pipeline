"""
load_to_postgres.py
Load transformed and aggregated data into PostgreSQL (nea_electricity_db).
Uses SQLAlchemy + psycopg2 with upsert strategy (INSERT ... ON CONFLICT DO UPDATE).
"""

import logging
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
import yaml
import os

with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

logger = logging.getLogger(__name__)

DB = config["database"]
DB_PASSWORD = os.getenv("DB_PASSWORD", DB.get("password", "nea_secret"))
DATABASE_URL = (
    f"postgresql+psycopg2://{DB['user']}:{DB_PASSWORD}"
    f"@{DB['host']}:{DB['port']}/{DB['name']}"
)


def get_engine():
    return create_engine(
        DATABASE_URL,
        pool_size=DB["pool_size"],
        max_overflow=DB["max_overflow"],
        pool_pre_ping=True,
        connect_args={"options": f"-c timezone=Asia/Kathmandu"},
    )


def load_dataframe(
    df: pd.DataFrame,
    table: str,
    engine,
    if_exists: str = "append",
    chunksize: int = 5000,
) -> int:
    """
    Load a DataFrame to a PostgreSQL table.

    Args:
        df: DataFrame to load
        table: target table name
        engine: SQLAlchemy engine
        if_exists: 'append' | 'replace' | 'fail'
        chunksize: rows per batch

    Returns:
        Number of rows loaded
    """
    if df is None or df.empty:
        logger.warning(f"Empty DataFrame for {table}, skipping")
        return 0

    # Drop pipeline-internal columns before DB load
    drop_cols = [c for c in ["extracted_at","cleaned_at","aggregated_at"] if c in df.columns]
    df = df.drop(columns=drop_cols)

    # Replace pandas NA/NaN with None for PostgreSQL compatibility
    df = df.replace({np.nan: None})

    try:
        df.to_sql(
            name=table,
            con=engine,
            if_exists=if_exists,
            index=False,
            chunksize=chunksize,
            method="multi",
        )
        logger.info(f"Loaded {len(df)} rows → {table}")
        return len(df)
    except Exception as e:
        logger.error(f"Failed to load {table}: {e}")
        raise


def upsert_annual_consumption(df: pd.DataFrame, engine) -> int:
    """Upsert annual consumption using ON CONFLICT (fiscal_year) DO UPDATE."""
    if df is None or df.empty:
        return 0

    cols = [
        "fiscal_year","fiscal_year_bs","total_energy_gwh","domestic_gwh",
        "industrial_gwh","commercial_gwh","irrigation_gwh","other_gwh",
        "peak_demand_mw","total_consumers","per_capita_kwh","system_loss_pct",
        "installed_capacity_mw","energy_imported_gwh","energy_exported_gwh",
        "net_energy_gwh","self_sufficiency_ratio","load_factor_pct",
    ]
    existing_cols = [c for c in cols if c in df.columns]
    df_load = df[existing_cols].replace({np.nan: None})

    with engine.begin() as conn:
        stmt = text("""
            INSERT INTO fact_annual_consumption ({cols})
            VALUES ({vals})
            ON CONFLICT (fiscal_year) DO UPDATE SET
                total_energy_gwh = EXCLUDED.total_energy_gwh,
                peak_demand_mw = EXCLUDED.peak_demand_mw,
                total_consumers = EXCLUDED.total_consumers,
                per_capita_kwh = EXCLUDED.per_capita_kwh,
                system_loss_pct = EXCLUDED.system_loss_pct,
                loaded_at = NOW()
        """.format(
            cols=", ".join(existing_cols),
            vals=", ".join([f":{c}" for c in existing_cols])
        ))
        result = conn.execute(stmt, df_load.to_dict("records"))

    logger.info(f"Upserted {len(df_load)} rows → fact_annual_consumption")
    return len(df_load)


def upsert_monthly_consumption(df: pd.DataFrame, engine) -> int:
    """Upsert monthly consumption with ON CONFLICT (year, month) DO UPDATE."""
    if df is None or df.empty:
        return 0

    cols = [
        "year","month","month_name","fiscal_year","fiscal_quarter","season",
        "domestic_gwh","industrial_gwh","commercial_gwh","irrigation_gwh",
        "other_gwh","total_gwh","peak_demand_mw","load_factor_pct",
        "seasonal_index","total_gwh_3m_avg","growth_rate_pct",
    ]
    existing = [c for c in cols if c in df.columns]
    df_load = df[existing].replace({np.nan: None})

    with engine.begin() as conn:
        for _, row in df_load.iterrows():
            conn.execute(text("""
                INSERT INTO fact_monthly_consumption ({cols})
                VALUES ({vals})
                ON CONFLICT (year, month) DO UPDATE SET
                    total_gwh = EXCLUDED.total_gwh,
                    peak_demand_mw = EXCLUDED.peak_demand_mw,
                    seasonal_index = EXCLUDED.seasonal_index,
                    loaded_at = NOW()
            """.format(
                cols=", ".join(existing),
                vals=", ".join([f":{c}" for c in existing])
            )), row.to_dict())

    logger.info(f"Upserted {len(df_load)} rows → fact_monthly_consumption")
    return len(df_load)


def load_smart_meter_batch(df: pd.DataFrame, engine, chunksize: int = 10000) -> int:
    """
    Load smart meter hourly readings in batches.
    Uses COPY-based bulk insert for performance (via to_sql with method='multi').
    Skips duplicate (meter_id, timestamp) pairs.
    """
    if df is None or df.empty:
        return 0

    cols = [
        "meter_id","timestamp","hour","consumer_type","consumption_kwh",
        "voltage_v","power_factor","apparent_power_kva",
        "is_peak_hour","is_weekend","voltage_flag","low_power_factor",
        "demand_category","rolling_24h_kwh","deviation_from_daily_avg",
    ]
    existing = [c for c in cols if c in df.columns]
    df_load = df[existing].replace({np.nan: None})

    total = 0
    for i in range(0, len(df_load), chunksize):
        chunk = df_load.iloc[i:i+chunksize]
        try:
            chunk.to_sql(
                "fact_smart_meter_hourly",
                con=engine,
                if_exists="append",
                index=False,
                method="multi",
            )
            total += len(chunk)
        except Exception as e:
            if "unique" in str(e).lower():
                logger.debug(f"Skipped {len(chunk)} duplicate smart meter rows")
            else:
                logger.error(f"Smart meter batch load error: {e}")
                raise

    logger.info(f"Loaded {total} smart meter rows → fact_smart_meter_hourly")
    return total


def load_province_electricity(df: pd.DataFrame, engine) -> int:
    return load_dataframe(df, "fact_province_electricity", engine)


def load_weather_daily(df: pd.DataFrame, engine) -> int:
    return load_dataframe(df, "fact_weather_daily", engine)


def load_hydro_projects(df: pd.DataFrame, engine) -> int:
    return load_dataframe(df, "fact_hydro_projects", engine)


def log_pipeline_run(
    engine,
    pipeline_name: str,
    start_time: datetime,
    status: str,
    records_loaded: int,
    error_msg: str = None
) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO etl_pipeline_runs
                (pipeline_name, start_time, end_time, status, records_loaded, error_message)
            VALUES
                (:name, :start, NOW(), :status, :loaded, :err)
        """), {
            "name": pipeline_name,
            "start": start_time,
            "status": status,
            "loaded": records_loaded,
            "err": error_msg,
        })


def run_load(transformed: dict, aggregated: dict) -> dict:
    """Run all load tasks to PostgreSQL."""
    logger.info("=== Starting Load Stage ===")
    engine = get_engine()
    start = datetime.now()
    stats = {"tables": {}, "total_rows": 0}

    try:
        if "annual" in transformed:
            n = upsert_annual_consumption(transformed["annual"], engine)
            stats["tables"]["annual"] = n
            stats["total_rows"] += n

        if "monthly_revenue" in transformed:
            n = upsert_monthly_consumption(transformed["monthly_revenue"], engine)
            stats["tables"]["monthly"] = n
            stats["total_rows"] += n

        if "smart_meter_features" in transformed:
            n = load_smart_meter_batch(transformed["smart_meter_features"], engine)
            stats["tables"]["smart_meter"] = n
            stats["total_rows"] += n

        if "province_enriched" in transformed:
            n = load_province_electricity(transformed["province_enriched"], engine)
            stats["tables"]["province"] = n
            stats["total_rows"] += n

        if "weather" in transformed:
            n = load_weather_daily(transformed["weather"], engine)
            stats["tables"]["weather"] = n
            stats["total_rows"] += n

        log_pipeline_run(engine, "nea_main_etl", start, "success", stats["total_rows"])
        logger.info(f"=== Load Complete: {stats['total_rows']} total rows loaded ===")

    except Exception as e:
        log_pipeline_run(engine, "nea_main_etl", start, "failed", stats["total_rows"], str(e))
        logger.error(f"Load stage failed: {e}")
        raise

    return stats


if __name__ == "__main__":
    print("Run via main_etl.py for full pipeline execution.")
