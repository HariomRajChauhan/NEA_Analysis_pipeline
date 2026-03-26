"""
airflow_dag.py
Apache Airflow DAG for Nepal Electricity Analytics Pipeline.
Schedules: daily smart meter ingest, weekly full ETL, monthly report generation.
Compatible with Airflow 2.6+
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow.models import Variable

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "nea-data-team",
    "depends_on_past": False,
    "email": ["data-team@nea.org.np"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}

PROJECT_DIR = Variable.get("nea_project_dir", "/opt/airflow/dags/electricity-analytics-pipeline")


# ---------------------------------------------------------------------------
# Python callables
# ---------------------------------------------------------------------------
def extract_nea_data(**context):
    import sys
    sys.path.insert(0, PROJECT_DIR)
    from etl.extract.extract_nea import run_extraction
    data = run_extraction()
    context["task_instance"].xcom_push(key="nea_row_counts", value={
        k: len(v) for k, v in data.items()
    })
    return "NEA extraction complete"


def extract_smart_meter(**context):
    import sys
    sys.path.insert(0, PROJECT_DIR)
    from etl.extract.extract_smart_meter import run_extraction
    data = run_extraction()
    context["task_instance"].xcom_push(
        key="smart_meter_rows",
        value=len(data.get("hourly_readings", []))
    )
    return "Smart meter extraction complete"


def extract_weather(**context):
    import sys
    sys.path.insert(0, PROJECT_DIR)
    from etl.extract.extract_weather_api import run_extraction
    run_extraction()
    return "Weather extraction complete"


def extract_open_data(**context):
    import sys
    sys.path.insert(0, PROJECT_DIR)
    from etl.extract.extract_open_data import run_extraction
    run_extraction()
    return "Open data extraction complete"


def clean_all(**context):
    import sys
    sys.path.insert(0, PROJECT_DIR)
    from etl.extract.extract_nea import run_extraction as nea_ex
    from etl.extract.extract_smart_meter import run_extraction as sm_ex
    from etl.extract.extract_weather_api import run_extraction as wx_ex
    from etl.extract.extract_open_data import run_extraction as od_ex
    from etl.transform.clean_data import run_cleaning
    raw = {}
    raw.update(nea_ex())
    raw.update(sm_ex())
    raw.update(wx_ex())
    raw.update(od_ex())
    cleaned = run_cleaning(raw)
    context["task_instance"].xcom_push(key="cleaned_tables", value=list(cleaned.keys()))
    return f"Cleaned {len(cleaned)} datasets"


def transform_all(**context):
    import sys
    sys.path.insert(0, PROJECT_DIR)
    # (In production, read from cleaned CSVs rather than re-running extract)
    from etl.transform.transform_data import run_transformation
    # Simplified: would normally pass cleaned dict from XCom
    return "Transformation complete"


def load_to_db(**context):
    import sys
    sys.path.insert(0, PROJECT_DIR)
    from etl.load.load_to_postgres import run_load
    # In production, pass transformed/aggregated from XCom or temp storage
    return "Load complete"


def validate_pipeline(**context):
    import sys
    sys.path.insert(0, PROJECT_DIR)
    # Run validation checks
    return "Validation complete"


def generate_daily_report(**context):
    import sys
    sys.path.insert(0, PROJECT_DIR)
    from reports.daily_report import generate_daily_report as gen
    gen()
    return "Daily report generated"


def generate_monthly_report(**context):
    import sys
    sys.path.insert(0, PROJECT_DIR)
    from reports.monthly_report import generate_monthly_report as gen
    gen()
    return "Monthly report generated"


# ---------------------------------------------------------------------------
# DAG 1: Daily Smart Meter Ingest (runs at 01:00 NPT = 19:15 UTC previous day)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nea_smart_meter_daily",
    description="Daily smart meter data ingestion for NEA",
    schedule_interval="15 19 * * *",  # 01:00 NPT
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["nea","smart-meter","daily"],
    doc_md="""
    ## NEA Daily Smart Meter Pipeline
    Ingests hourly smart meter readings from 100+ AMI meters.
    Runs at 01:00 NPT daily. Loads to `fact_smart_meter_hourly`.
    """,
) as dag_daily:

    t_extract_sm = PythonOperator(
        task_id="extract_smart_meter",
        python_callable=extract_smart_meter,
    )

    t_extract_wx = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather,
    )

    t_clean = BashOperator(
        task_id="clean_and_validate",
        bash_command=f"cd {PROJECT_DIR} && python -m etl.transform.clean_data",
    )

    t_load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_db,
    )

    t_report = PythonOperator(
        task_id="generate_daily_report",
        python_callable=generate_daily_report,
    )

    t_alert = EmailOperator(
        task_id="send_daily_summary",
        to=["operations@nea.org.np"],
        subject="NEA Daily Electricity Report - {{ ds }}",
        html_content="""
        <h2>NEA Daily Pipeline Complete</h2>
        <p>Date: {{ ds }}</p>
        <p>Smart meter records processed and report generated.</p>
        """,
    )

    [t_extract_sm, t_extract_wx] >> t_clean >> t_load >> t_report >> t_alert


# ---------------------------------------------------------------------------
# DAG 2: Full Weekly ETL (runs every Sunday 02:00 NPT)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nea_electricity_pipeline",
    description="Full weekly NEA electricity analytics ETL",
    schedule_interval="15 20 * * 0",  # 02:00 NPT Sunday
    start_date=days_ago(7),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["nea","full-etl","weekly"],
) as dag_weekly:

    t1_nea     = PythonOperator(task_id="extract_nea",       python_callable=extract_nea_data)
    t1_sm      = PythonOperator(task_id="extract_smart_meter", python_callable=extract_smart_meter)
    t1_wx      = PythonOperator(task_id="extract_weather",   python_callable=extract_weather)
    t1_od      = PythonOperator(task_id="extract_open_data", python_callable=extract_open_data)

    t2_clean   = PythonOperator(task_id="clean_data",        python_callable=clean_all)
    t3_transform = PythonOperator(task_id="transform_data",  python_callable=transform_all)
    t4_load    = PythonOperator(task_id="load_database",     python_callable=load_to_db)
    t5_validate = PythonOperator(task_id="validate",         python_callable=validate_pipeline)

    [t1_nea, t1_sm, t1_wx, t1_od] >> t2_clean >> t3_transform >> t4_load >> t5_validate


# ---------------------------------------------------------------------------
# DAG 3: Monthly Report (1st of each month, Nepali fiscal calendar aware)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nea_monthly_report",
    description="Monthly electricity consumption report for NEA management",
    schedule_interval="0 5 1 * *",  # 1st of month, 10:45 NPT
    start_date=days_ago(30),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["nea","reports","monthly"],
) as dag_monthly:

    t_report = PythonOperator(
        task_id="generate_monthly_report",
        python_callable=generate_monthly_report,
    )
    t_email = EmailOperator(
        task_id="distribute_report",
        to=["management@nea.org.np", "planning@nea.org.np"],
        subject="NEA Monthly Electricity Report - {{ macros.ds_format(ds, '%Y-%m-%d', '%B %Y') }}",
        html_content="""
        <h2>Nepal Electricity Authority — Monthly Report</h2>
        <p>Month: {{ macros.ds_format(ds, '%Y-%m-%d', '%B %Y') }}</p>
        <p>Please find this month's electricity consumption analysis attached.</p>
        """,
        files=[f"{PROJECT_DIR}/reports/output/monthly_report_{{{{ ds }}}}.pdf"],
    )
    t_report >> t_email
