# ⚡ Nepal Electricity Authority — Electricity Usage Analytics Pipeline

A production-grade ETL and analytics pipeline for Nepal Electricity Authority (NEA), built to ingest, process, and analyse electricity consumption data across Nepal's 7 provinces and 77 districts.

---

## 📋 Project Overview

| Item | Detail |
|---|---|
| Organisation | Nepal Electricity Authority (NEA) — नेपाल विद्युत् प्राधिकरण |
| Data Coverage | FY 2015/16 – 2023/24 (annual), 2023–2024 (monthly & hourly) |
| Provinces | 7 (Koshi, Madhesh, Bagmati, Gandaki, Lumbini, Karnali, Sudurpashchim) |
| Districts | 77 |
| Smart Meters | 100 sample meters (Domestic, Commercial, Industrial) |
| Weather Stations | 7 (Kathmandu, Biratnagar, Pokhara, Janakpur, Dhangadhi, Birendranagar, Butwal) |
| Database | PostgreSQL 15+ |
| Scheduler | Apache Airflow 2.8 |
| Python | 3.10+ |

---

## 📁 Project Structure

```
electricity-analytics-pipeline/
├── data/
│   ├── raw/
│   │   ├── nea_reports/          # NEA annual & monthly consumption, tariff structure
│   │   ├── open_data_nepal/      # Province & district electricity data (77 districts)
│   │   ├── smart_meter/          # Hourly AMI readings (72,000 rows, 100 meters)
│   │   └── weather_api/          # DHM / OpenWeatherMap station data
│   ├── processed/
│   │   ├── cleaned_data/         # Validated, cleaned CSVs
│   │   ├── transformed_data/     # Feature-engineered datasets
│   │   └── aggregated_data/      # KPI tables for dashboards
│   └── external/
│       └── population_data/      # CBS Nepal Census 2021 data
│
├── etl/
│   ├── extract/
│   │   ├── extract_nea.py        # NEA annual/monthly/tariff/hydropower projects
│   │   ├── extract_open_data.py  # Province/district data, World Bank API
│   │   ├── extract_weather_api.py# OpenWeatherMap + local fallback + HDD/CDD
│   │   └── extract_smart_meter.py# Chunked AMI ingestion, outage detection
│   ├── transform/
│   │   ├── clean_data.py         # Validation, outlier handling, Nepal-specific rules
│   │   ├── transform_data.py     # Feature engineering, fiscal year, revenue calc
│   │   └── aggregate_data.py     # KPI aggregations, dashboard export
│   ├── load/
│   │   └── load_to_postgres.py   # Upsert to PostgreSQL with audit logging
│   └── main_etl.py               # Master orchestration: Extract→Clean→Transform→Load
│
├── database/
│   ├── schema.sql                # Full PostgreSQL schema (dim + fact + views)
│   └── queries.sql               # Common analytical queries
│
├── scheduler/
│   └── airflow_dag.py            # 3 DAGs: daily smart meter, weekly full ETL, monthly report
│
├── analytics/
│   ├── consumption_analysis.py   # Growth trends, sector breakdown, 5-year forecast
│   ├── peak_demand_analysis.py   # Load curves, LDC, peak-to-off-peak ratios
│   ├── weather_usage_analysis.py # Weather-demand correlation, monsoon impact
│   └── province_analysis.py      # Electrification gap, inequality, loss analysis
│
├── reports/
│   ├── daily_report.py           # Ops report: peak demand, meter status
│   ├── monthly_report.py         # Management report: sector, province, revenue
│   └── yearly_report.py          # Board report: trends, forecast, trade balance
│
├── monitoring/
│   └── metrics.py                # Pipeline health metrics (Prometheus-compatible)
│
├── config/config.yaml            # All settings: DB, APIs, tariffs, scheduling
├── requirements.txt
├── run_pipeline.py               # Universal CLI entry point
└── README.md
```

---

## 🗄️ Datasets

### Raw Datasets

#### `nea_annual_consumption.csv`
9 years of national consumption data.

| Column | Type | Example | Description |
|---|---|---|---|
| fiscal_year | str | `2023/24` | Nepal fiscal year |
| total_energy_gwh | float | 10200.0 | Total electricity consumed (GWh) |
| domestic_gwh | float | 4310.0 | Residential consumption |
| industrial_gwh | float | 3690.0 | Industrial sector |
| commercial_gwh | float | 1850.0 | Commercial sector |
| peak_demand_mw | float | 2212.0 | Annual peak demand (MW) |
| system_loss_pct | float | 15.8 | Transmission + distribution losses |
| energy_imported_gwh | float | 1900.0 | Import from India |
| energy_exported_gwh | float | 1950.0 | Export to India |
| per_capita_kwh | float | 400.0 | Per capita consumption |

**Key insight**: Nepal became a net electricity exporter in FY 2022/23 — a historic milestone driven by Upper Tamakoshi (456 MW, commissioned 2021).

---

#### `nea_monthly_consumption.csv`
24 months of monthly breakdowns.

| Column | Type | Description |
|---|---|---|
| year, month | int | Gregorian calendar |
| month_name | str | Nepali month name (Baisakh–Chaitra) |
| total_gwh | float | Monthly total consumption |
| peak_demand_mw | float | Monthly peak demand |
| load_factor_pct | float | Average load / peak load |

---

#### `nea_tariff_structure.csv`
18 consumer tariff categories (FY 2023/24 rates).

| Code | Category | Rate (NPR/kWh) | Notes |
|---|---|---|---|
| D1 | Domestic ≤20 units | 0.00 | Free lifeline electricity |
| D2 | Domestic 21–100 units | 7.30 | Standard domestic |
| D5 | Domestic >500 units | 13.00 | High consumption |
| I5 | Industrial EHT (66kV+) | 6.50 | Bulk industrial |
| AG1 | Irrigation pump | 4.50 | 15% rebate applied |
| EV1 | EV charging station | 8.00 | Promotes EV adoption |

---

#### `smart_meter_hourly.csv`
72,000 hourly readings from 100 smart meters (30 days).

| Column | Type | Description |
|---|---|---|
| timestamp | datetime | Hourly reading (NPT) |
| meter_id | str | `NEA-DOM-10001`, `NEA-COM-10003`, etc. |
| consumer_type | str | Domestic / Commercial / Industrial |
| consumption_kwh | float | Hourly consumption |
| voltage_v | float | Meter voltage (target: 230V ±10%) |
| power_factor | float | Power factor (target: ≥0.85) |

Load profiles modelled on Nepal's actual demand pattern:
- **Evening peak**: 18:00–21:00 (lighting + cooking)
- **Morning peak**: 06:00–09:00 (cooking + commute)
- **Industrial**: near-flat profile (80–100% capacity)

---

#### `province_electricity_data.csv`
All 7 provinces with full electricity and demographic data.

| Province | Population | Electrification % | Per Capita kWh | System Loss % |
|---|---|---|---|---|
| Koshi | 4,972,521 | 94.8% | 297 | 16.2% |
| Madhesh | 6,126,288 | 89.2% | 197 | 18.9% |
| Bagmati | 6,126,288 | 98.5% | 558 | 13.2% |
| Gandaki | 2,403,757 | 96.1% | 324 | 14.8% |
| Lumbini | 5,123,717 | 93.5% | 224 | 17.1% |
| Karnali | 1,688,682 | 83.7% | 168 | 21.4% |
| Sudurpashchim | 2,552,517 | 87.4% | 188 | 20.8% |

---

#### `district_electricity.csv`
77 districts with consumption, consumers, per-capita kWh.

---

#### `weather_data.csv`
7 weather stations with temperature, rainfall, humidity, HDD/CDD.

---

## 🚀 Quick Start

### 1. Clone & Install

```bash
git clone https://github.com/your-org/electricity-analytics-pipeline.git
cd electricity-analytics-pipeline
pip install -r requirements.txt
```

### 2. Configure

```bash
cp config/config.yaml config/config.local.yaml
# Edit: database credentials, API keys
export DB_PASSWORD="your_db_password"
export WEATHER_API_KEY="your_openweathermap_key"
```

### 3. Set Up Database

```bash
psql -U postgres -c "CREATE DATABASE nea_electricity_db;"
psql -U postgres -d nea_electricity_db -f database/schema.sql
```

### 4. Run the Pipeline

```bash
# Full ETL pipeline
python run_pipeline.py

# Extract only (no DB write)
python run_pipeline.py --mode extract --skip-load

# Dry run (validate without saving)
python run_pipeline.py --dry-run

# Generate reports
python run_pipeline.py --report daily
python run_pipeline.py --report monthly

# Run analytics
python run_pipeline.py --analytics all
python run_pipeline.py --analytics consumption
python run_pipeline.py --analytics province
```

### 5. Airflow Setup

```bash
export AIRFLOW_HOME=~/airflow
airflow db init
airflow users create --username admin --role Admin --firstname NEA --lastname Admin --email admin@nea.org.np --password admin
cp scheduler/airflow_dag.py $AIRFLOW_HOME/dags/
airflow webserver --port 8080 &
airflow scheduler &
```

---

## 📊 Analytics Outputs

| Module | Key Metrics |
|---|---|
| `consumption_analysis.py` | CAGR 13.8%, 5-year demand forecast, sector shares, import/export balance |
| `peak_demand_analysis.py` | Evening peak 18–21h, load factor ~70%, load duration curve |
| `weather_usage_analysis.py` | Monsoon correlation, HDD/CDD regression, seasonal index |
| `province_analysis.py` | Electrification gap (Karnali lowest 83.7%), Gini coefficient, system loss ranking |

---

## 🗃️ Database Schema

**Dimension tables**: `dim_province`, `dim_district`, `dim_date`, `dim_meter`, `dim_weather_station`, `dim_tariff`

**Fact tables**: `fact_annual_consumption`, `fact_monthly_consumption`, `fact_smart_meter_hourly`, `fact_province_electricity`, `fact_district_electricity`, `fact_weather_daily`, `fact_hydro_projects`

**Views**: `vw_daily_system_load`, `vw_province_performance`, `vw_latest_kpis`, `vw_monthly_revenue`

---

## 🔧 Configuration

Key settings in `config/config.yaml`:

```yaml
nepal:
  fiscal_year_start_month: 4    # Baisakh (mid-April)
  timezone: "Asia/Kathmandu"    # NPT = UTC+5:45

tariff:
  domestic_lifeline: 0.00       # D1: free for ≤20 units
  domestic_standard: 7.30       # D2: NPR/kWh
  industrial_ht_11kv: 8.00      # I3: 11kV industrial

scheduler:
  smart_meter_interval: "hourly"
  daily_report_time: "06:00"    # NPT
```

---

## 📅 Airflow DAGs

| DAG | Schedule | Purpose |
|---|---|---|
| `nea_smart_meter_daily` | Daily 01:00 NPT | Smart meter ingest + daily report |
| `nea_electricity_pipeline` | Weekly Sunday 02:00 NPT | Full ETL (all sources) |
| `nea_monthly_report` | 1st of month | Management report generation + email |

---

## 📈 Nepal Electricity Context

- **FY 2023/24**: 10,200 GWh consumed, 2,212 MW peak demand, 400 kWh per capita
- **Net exporter**: Nepal exported 1,950 GWh and imported 1,900 GWh in FY 2023/24
- **Upper Tamakoshi** (456 MW): largest hydropower plant, commissioned 2021
- **Upcoming**: Arun III (900 MW), Upper Arun (639 MW), Pancheshwar (6,480 MW joint with India)
- **System loss target**: Reduce from 15.8% to <13% by FY 2027/28
- **EV policy**: NEA expanding EV charging infrastructure (tariff code EV1: NPR 8/kWh)

---

## 📞 Contact

**Nepal Electricity Authority — Data Analytics Division**
Durbarmarg, Kathmandu, Nepal
📧 data-team@nea.org.np
🌐 [nea.org.np](https://www.nea.org.np)
# NEA_Analysis_pipeline
