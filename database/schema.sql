-- =============================================================================
-- Nepal Electricity Authority (NEA) Analytics Database Schema
-- Database: nea_electricity_db  |  Engine: PostgreSQL 15+
-- Schema reflects Nepal's 7-province, 77-district administrative structure
-- =============================================================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

-- Province dimension (7 provinces per 2015 constitution)
CREATE TABLE IF NOT EXISTS dim_province (
    province_id         SMALLINT PRIMARY KEY,
    province_name_en    VARCHAR(50) NOT NULL,
    province_name_np    VARCHAR(100),
    headquarter         VARCHAR(50),
    area_sq_km          NUMERIC(10,2),
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- District dimension (77 districts)
CREATE TABLE IF NOT EXISTS dim_district (
    district_id         SMALLINT PRIMARY KEY,
    district_name       VARCHAR(100) NOT NULL,
    province_id         SMALLINT NOT NULL REFERENCES dim_province(province_id),
    area_sq_km          NUMERIC(10,2),
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Consumer category dimension
CREATE TABLE IF NOT EXISTS dim_consumer_category (
    category_id         SERIAL PRIMARY KEY,
    tariff_code         VARCHAR(10) UNIQUE NOT NULL,
    category_name       VARCHAR(50) NOT NULL,   -- Domestic, Commercial, Industrial, etc.
    sub_category        VARCHAR(100),
    voltage_level       VARCHAR(20),            -- LT, 11kV, 33kV, 66kV+
    description         TEXT
);

-- Tariff dimension (versioned for rate changes)
CREATE TABLE IF NOT EXISTS dim_tariff (
    tariff_id           SERIAL PRIMARY KEY,
    tariff_code         VARCHAR(10) NOT NULL REFERENCES dim_consumer_category(tariff_code),
    effective_from      DATE NOT NULL,
    effective_to        DATE,                   -- NULL = currently active
    energy_charge_npr   NUMERIC(8,2) NOT NULL,  -- NPR per kWh
    demand_charge_npr   NUMERIC(8,2) DEFAULT 0, -- NPR per kVA/month
    fixed_charge_npr    NUMERIC(8,2) DEFAULT 0, -- NPR per month
    rebate_pct          NUMERIC(5,2) DEFAULT 0,
    surcharge_pct       NUMERIC(5,2) DEFAULT 0,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Smart meter registry
CREATE TABLE IF NOT EXISTS dim_meter (
    meter_id            VARCHAR(20) PRIMARY KEY,
    consumer_type       VARCHAR(30) NOT NULL,
    district_id         SMALLINT REFERENCES dim_district(district_id),
    province_id         SMALLINT REFERENCES dim_province(province_id),
    install_date        DATE,
    meter_make          VARCHAR(50),
    meter_model         VARCHAR(50),
    communication       VARCHAR(20) DEFAULT 'GPRS',  -- GPRS, PLC, RF
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Weather station dimension
CREATE TABLE IF NOT EXISTS dim_weather_station (
    station_id          VARCHAR(10) PRIMARY KEY,
    station_name        VARCHAR(100) NOT NULL,
    province_id         SMALLINT REFERENCES dim_province(province_id),
    latitude            NUMERIC(9,6) NOT NULL,
    longitude           NUMERIC(9,6) NOT NULL,
    altitude_m          NUMERIC(7,1),
    operator            VARCHAR(50) DEFAULT 'DHM',  -- Dept. of Hydrology & Meteorology
    is_active           BOOLEAN DEFAULT TRUE
);

-- Date dimension (Gregorian + Nepali Bikram Sambat)
CREATE TABLE IF NOT EXISTS dim_date (
    date_id             INTEGER PRIMARY KEY,   -- YYYYMMDD format
    full_date           DATE NOT NULL UNIQUE,
    year                SMALLINT NOT NULL,
    month               SMALLINT NOT NULL,
    day                 SMALLINT NOT NULL,
    quarter             SMALLINT NOT NULL,
    day_of_week         SMALLINT NOT NULL,     -- 0=Monday
    day_name            VARCHAR(10) NOT NULL,
    month_name          VARCHAR(10) NOT NULL,
    is_weekend          BOOLEAN NOT NULL,
    is_public_holiday   BOOLEAN DEFAULT FALSE,
    holiday_name        VARCHAR(100),
    -- Nepal fiscal year (Shrawan to Ashadh)
    fiscal_year         VARCHAR(10) NOT NULL,  -- e.g. '2023/24'
    fiscal_quarter      VARCHAR(3) NOT NULL,   -- Q1-Q4
    fiscal_month        SMALLINT NOT NULL,     -- 1-12 from Shrawan
    -- Nepali calendar (Bikram Sambat)
    bs_year             SMALLINT,
    bs_month            SMALLINT,
    bs_day              SMALLINT,
    bs_month_name       VARCHAR(20),
    -- Nepal seasons
    season              VARCHAR(20),           -- Winter/Pre-Monsoon/Monsoon/Post-Monsoon
    is_monsoon          BOOLEAN NOT NULL DEFAULT FALSE
);

-- =============================================================================
-- FACT TABLES
-- =============================================================================

-- Annual electricity consumption (NEA Annual Report)
CREATE TABLE IF NOT EXISTS fact_annual_consumption (
    id                      SERIAL PRIMARY KEY,
    fiscal_year             VARCHAR(10) NOT NULL UNIQUE,
    fiscal_year_bs          VARCHAR(10),
    total_energy_gwh        NUMERIC(10,2) NOT NULL,
    domestic_gwh            NUMERIC(10,2),
    industrial_gwh          NUMERIC(10,2),
    commercial_gwh          NUMERIC(10,2),
    irrigation_gwh          NUMERIC(10,2),
    other_gwh               NUMERIC(10,2),
    peak_demand_mw          NUMERIC(8,2),
    total_consumers         INTEGER,
    per_capita_kwh          NUMERIC(8,2),
    system_loss_pct         NUMERIC(5,2),
    installed_capacity_mw   NUMERIC(10,2),
    energy_imported_gwh     NUMERIC(10,2) DEFAULT 0,
    energy_exported_gwh     NUMERIC(10,2) DEFAULT 0,
    net_energy_gwh          NUMERIC(10,2),
    self_sufficiency_ratio  NUMERIC(6,4),
    load_factor_pct         NUMERIC(6,2),
    trade_balance_gwh       NUMERIC(10,2),
    total_revenue_npr_mn    NUMERIC(12,2),
    source                  VARCHAR(100) DEFAULT 'NEA Annual Report',
    loaded_at               TIMESTAMPTZ DEFAULT NOW()
);

-- Monthly consumption (NEA operational reports)
CREATE TABLE IF NOT EXISTS fact_monthly_consumption (
    id                  SERIAL PRIMARY KEY,
    date_id             INTEGER NOT NULL REFERENCES dim_date(date_id),
    year                SMALLINT NOT NULL,
    month               SMALLINT NOT NULL,
    month_name          VARCHAR(20),
    fiscal_year         VARCHAR(10),
    fiscal_quarter      VARCHAR(3),
    season              VARCHAR(20),
    domestic_gwh        NUMERIC(8,3),
    industrial_gwh      NUMERIC(8,3),
    commercial_gwh      NUMERIC(8,3),
    irrigation_gwh      NUMERIC(8,3),
    other_gwh           NUMERIC(8,3),
    total_gwh           NUMERIC(8,3) NOT NULL,
    peak_demand_mw      NUMERIC(8,2),
    avg_daily_mwh       NUMERIC(10,2),
    load_factor_pct     NUMERIC(5,2),
    seasonal_index      NUMERIC(6,4),
    total_gwh_3m_avg    NUMERIC(8,3),
    growth_rate_pct     NUMERIC(8,4),
    revenue_npr_mn      NUMERIC(12,2),
    loaded_at           TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (year, month)
);

-- Smart meter hourly readings
CREATE TABLE IF NOT EXISTS fact_smart_meter_hourly (
    id                          BIGSERIAL PRIMARY KEY,
    meter_id                    VARCHAR(20) NOT NULL REFERENCES dim_meter(meter_id),
    timestamp                   TIMESTAMPTZ NOT NULL,
    date_id                     INTEGER REFERENCES dim_date(date_id),
    hour                        SMALLINT NOT NULL CHECK (hour BETWEEN 0 AND 23),
    consumer_type               VARCHAR(30),
    consumption_kwh             NUMERIC(10,4) NOT NULL,
    voltage_v                   NUMERIC(6,2),
    power_factor                NUMERIC(5,4),
    apparent_power_kva          NUMERIC(10,4),
    is_peak_hour                BOOLEAN,
    is_evening_peak             BOOLEAN,
    is_morning_peak             BOOLEAN,
    is_weekend                  BOOLEAN,
    voltage_flag                BOOLEAN DEFAULT FALSE,
    low_power_factor            BOOLEAN DEFAULT FALSE,
    demand_category             VARCHAR(10),    -- Low / Normal / High
    rolling_24h_kwh             NUMERIC(10,4),
    deviation_from_daily_avg    NUMERIC(10,4),
    loaded_at                   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (meter_id, timestamp)
);

-- Partition smart meter table by month for performance
-- CREATE TABLE fact_smart_meter_2024_01 PARTITION OF fact_smart_meter_hourly
--     FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- Province electricity summary
CREATE TABLE IF NOT EXISTS fact_province_electricity (
    id                              SERIAL PRIMARY KEY,
    province_id                     SMALLINT NOT NULL REFERENCES dim_province(province_id),
    report_year                     SMALLINT NOT NULL,
    population                      INTEGER,
    households                      INTEGER,
    electrification_pct             NUMERIC(5,2),
    electricity_consumers           INTEGER,
    annual_consumption_gwh          NUMERIC(10,2),
    per_capita_kwh                  NUMERIC(8,2),
    kwh_per_household               NUMERIC(8,2),
    peak_demand_mw                  NUMERIC(8,2),
    installed_capacity_mw           NUMERIC(10,2),
    transmission_lines_km           NUMERIC(8,2),
    distribution_lines_km           NUMERIC(8,2),
    system_loss_pct                 NUMERIC(5,2),
    estimated_loss_gwh              NUMERIC(8,2),
    loss_reduction_target_pct       NUMERIC(5,2),
    potential_saving_gwh            NUMERIC(8,2),
    rank_per_capita                 SMALLINT,
    rank_consumption                SMALLINT,
    rank_electrification            SMALLINT,
    estimated_revenue_npr_mn        NUMERIC(12,2),
    loaded_at                       TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (province_id, report_year)
);

-- District electricity summary
CREATE TABLE IF NOT EXISTS fact_district_electricity (
    id                      SERIAL PRIMARY KEY,
    district_id             SMALLINT NOT NULL REFERENCES dim_district(district_id),
    report_year             SMALLINT NOT NULL,
    population              INTEGER,
    households              INTEGER,
    electrification_pct     NUMERIC(5,2),
    annual_consumption_mwh  NUMERIC(12,2),
    consumers               INTEGER,
    per_capita_kwh          NUMERIC(8,2),
    urban_pct               NUMERIC(5,2),
    loaded_at               TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (district_id, report_year)
);

-- Weather observations
CREATE TABLE IF NOT EXISTS fact_weather_daily (
    id                          SERIAL PRIMARY KEY,
    station_id                  VARCHAR(10) NOT NULL REFERENCES dim_weather_station(station_id),
    date_id                     INTEGER NOT NULL REFERENCES dim_date(date_id),
    observation_date            DATE NOT NULL,
    max_temp_c                  NUMERIC(5,2),
    min_temp_c                  NUMERIC(5,2),
    avg_temp_c                  NUMERIC(5,2),
    feels_like_c                NUMERIC(5,2),
    temp_range_c                NUMERIC(5,2),
    humidity_pct                NUMERIC(5,2),
    rainfall_mm                 NUMERIC(8,2) DEFAULT 0,
    sunshine_hours              NUMERIC(4,2),
    wind_speed_kmh              NUMERIC(6,2),
    cloud_cover_pct             NUMERIC(5,2),
    hdd                         NUMERIC(5,2),   -- Heating Degree Days (base 18°C)
    cdd                         NUMERIC(5,2),   -- Cooling Degree Days (base 24°C)
    thermal_discomfort_index    NUMERIC(5,2),
    is_monsoon                  BOOLEAN,
    is_hot_day                  BOOLEAN,
    is_cold_day                 BOOLEAN,
    is_rainy_day                BOOLEAN,
    season                      VARCHAR(20),
    loaded_at                   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (station_id, observation_date)
);

-- Hydropower project registry
CREATE TABLE IF NOT EXISTS fact_hydro_projects (
    id                  SERIAL PRIMARY KEY,
    project_name        VARCHAR(100) NOT NULL,
    province_id         SMALLINT REFERENCES dim_province(province_id),
    capacity_mw         NUMERIC(8,2) NOT NULL,
    project_type        VARCHAR(20),    -- RoR (Run-of-River) or Storage
    operator            VARCHAR(100),
    commissioned_year   SMALLINT,
    status              VARCHAR(30),    -- Operating, Under Construction, Planning
    river               VARCHAR(50),
    design_energy_gwh   NUMERIC(10,2),
    is_ipp              BOOLEAN DEFAULT FALSE,
    loaded_at           TIMESTAMPTZ DEFAULT NOW()
);

-- Pipeline run audit log
CREATE TABLE IF NOT EXISTS etl_pipeline_runs (
    run_id              UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    pipeline_name       VARCHAR(100) NOT NULL,
    dag_id              VARCHAR(100),
    start_time          TIMESTAMPTZ NOT NULL,
    end_time            TIMESTAMPTZ,
    status              VARCHAR(20),   -- running, success, failed, partial
    records_extracted   INTEGER DEFAULT 0,
    records_cleaned     INTEGER DEFAULT 0,
    records_loaded      INTEGER DEFAULT 0,
    records_failed      INTEGER DEFAULT 0,
    error_message       TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================================================
-- INDEXES
-- =============================================================================

-- Smart meter: most queried by time range and meter
CREATE INDEX idx_sm_timestamp     ON fact_smart_meter_hourly (timestamp);
CREATE INDEX idx_sm_meter_ts      ON fact_smart_meter_hourly (meter_id, timestamp);
CREATE INDEX idx_sm_consumer_type ON fact_smart_meter_hourly (consumer_type, timestamp);
CREATE INDEX idx_sm_date_id       ON fact_smart_meter_hourly (date_id);
CREATE INDEX idx_sm_peak          ON fact_smart_meter_hourly (is_peak_hour, timestamp);

-- Monthly consumption
CREATE INDEX idx_monthly_date     ON fact_monthly_consumption (date_id);
CREATE INDEX idx_monthly_fy       ON fact_monthly_consumption (fiscal_year);

-- Province
CREATE INDEX idx_province_year    ON fact_province_electricity (report_year);

-- Weather
CREATE INDEX idx_weather_station_date ON fact_weather_daily (station_id, observation_date);
CREATE INDEX idx_weather_date_id      ON fact_weather_daily (date_id);

-- District
CREATE INDEX idx_district_year    ON fact_district_electricity (district_id, report_year);

-- =============================================================================
-- VIEWS
-- =============================================================================

-- National daily consumption snapshot
CREATE OR REPLACE VIEW vw_daily_system_load AS
SELECT
    d.full_date,
    d.fiscal_year,
    d.season,
    d.is_monsoon,
    d.is_weekend,
    sm.consumer_type,
    SUM(sm.consumption_kwh) AS total_kwh,
    AVG(sm.voltage_v)       AS avg_voltage,
    AVG(sm.power_factor)    AS avg_power_factor,
    MAX(sm.consumption_kwh) AS peak_kwh,
    COUNT(DISTINCT sm.meter_id) AS active_meters
FROM fact_smart_meter_hourly sm
JOIN dim_date d ON d.date_id = sm.date_id
GROUP BY d.full_date, d.fiscal_year, d.season, d.is_monsoon, d.is_weekend, sm.consumer_type;

-- Province performance summary
CREATE OR REPLACE VIEW vw_province_performance AS
SELECT
    p.province_name_en,
    p.headquarter,
    pe.report_year,
    pe.annual_consumption_gwh,
    pe.per_capita_kwh,
    pe.electrification_pct,
    pe.system_loss_pct,
    pe.rank_per_capita,
    pe.rank_electrification,
    pe.estimated_revenue_npr_mn
FROM fact_province_electricity pe
JOIN dim_province p ON p.province_id = pe.province_id;

-- Latest annual KPIs
CREATE OR REPLACE VIEW vw_latest_kpis AS
SELECT *
FROM fact_annual_consumption
ORDER BY fiscal_year DESC
LIMIT 1;

-- Monthly revenue trend
CREATE OR REPLACE VIEW vw_monthly_revenue AS
SELECT
    mc.year,
    mc.month,
    mc.month_name,
    mc.fiscal_year,
    mc.season,
    mc.total_gwh,
    mc.peak_demand_mw,
    mc.revenue_npr_mn,
    mc.growth_rate_pct,
    mc.seasonal_index
FROM fact_monthly_consumption mc
ORDER BY mc.year, mc.month;
