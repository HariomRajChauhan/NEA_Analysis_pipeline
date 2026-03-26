-- =============================================================================
-- NEA Electricity Analytics — Common Analytical Queries
-- Database: nea_electricity_db | PostgreSQL 15+
-- =============================================================================


-- =============================================================================
-- 1. NATIONAL CONSUMPTION TRENDS
-- =============================================================================

-- Annual consumption growth with YoY %
SELECT
    fiscal_year,
    total_energy_gwh,
    peak_demand_mw,
    per_capita_kwh,
    system_loss_pct,
    total_consumers,
    ROUND(
        (total_energy_gwh - LAG(total_energy_gwh) OVER (ORDER BY fiscal_year)) /
        LAG(total_energy_gwh) OVER (ORDER BY fiscal_year) * 100, 2
    ) AS yoy_growth_pct,
    ROUND(
        (peak_demand_mw - LAG(peak_demand_mw) OVER (ORDER BY fiscal_year)) /
        LAG(peak_demand_mw) OVER (ORDER BY fiscal_year) * 100, 2
    ) AS peak_yoy_pct
FROM fact_annual_consumption
ORDER BY fiscal_year;


-- Sector share evolution over fiscal years
SELECT
    fiscal_year,
    ROUND(domestic_gwh / total_energy_gwh * 100, 2)    AS domestic_pct,
    ROUND(industrial_gwh / total_energy_gwh * 100, 2)  AS industrial_pct,
    ROUND(commercial_gwh / total_energy_gwh * 100, 2)  AS commercial_pct,
    ROUND(irrigation_gwh / total_energy_gwh * 100, 2)  AS irrigation_pct,
    ROUND(other_gwh / total_energy_gwh * 100, 2)       AS other_pct
FROM fact_annual_consumption
ORDER BY fiscal_year;


-- Import/export trade balance
SELECT
    fiscal_year,
    energy_imported_gwh,
    energy_exported_gwh,
    (energy_exported_gwh - energy_imported_gwh) AS trade_balance_gwh,
    CASE
        WHEN energy_exported_gwh > energy_imported_gwh THEN 'Net Exporter'
        ELSE 'Net Importer'
    END AS trade_status,
    ROUND(energy_imported_gwh / total_energy_gwh * 100, 2) AS import_dependency_pct
FROM fact_annual_consumption
ORDER BY fiscal_year;


-- =============================================================================
-- 2. MONTHLY / SEASONAL ANALYSIS
-- =============================================================================

-- Monthly consumption with seasonal index
SELECT
    year, month, month_name, season, fiscal_year, fiscal_quarter,
    total_gwh,
    peak_demand_mw,
    load_factor_pct,
    seasonal_index,
    growth_rate_pct,
    total_gwh_3m_avg
FROM fact_monthly_consumption
ORDER BY year, month;


-- Average monthly profile across all years (seasonal pattern)
SELECT
    month,
    MAX(month_name) AS month_name,
    ROUND(AVG(total_gwh), 3)       AS avg_gwh,
    ROUND(MAX(total_gwh), 3)       AS max_gwh,
    ROUND(MIN(total_gwh), 3)       AS min_gwh,
    ROUND(AVG(peak_demand_mw), 2)  AS avg_peak_mw,
    ROUND(AVG(load_factor_pct), 2) AS avg_load_factor_pct
FROM fact_monthly_consumption
GROUP BY month
ORDER BY month;


-- Monsoon vs dry season comparison
SELECT
    CASE WHEN month BETWEEN 6 AND 9 THEN 'Monsoon (Jun-Sep)'
         ELSE 'Dry Season' END AS period,
    COUNT(*)                        AS months_count,
    ROUND(AVG(total_gwh), 3)        AS avg_monthly_gwh,
    ROUND(SUM(total_gwh), 3)        AS total_gwh,
    ROUND(AVG(peak_demand_mw), 2)   AS avg_peak_mw
FROM fact_monthly_consumption
GROUP BY 1
ORDER BY 1;


-- =============================================================================
-- 3. SMART METER / DEMAND ANALYSIS
-- =============================================================================

-- Average hourly load profile by consumer type (weekday vs weekend)
SELECT
    consumer_type,
    hour,
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    ROUND(AVG(consumption_kwh)::NUMERIC, 4)  AS avg_kwh,
    ROUND(MAX(consumption_kwh)::NUMERIC, 4)  AS peak_kwh,
    ROUND(
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY consumption_kwh)::NUMERIC, 4
    ) AS p90_kwh,
    COUNT(*)                                 AS sample_count
FROM fact_smart_meter_hourly
GROUP BY consumer_type, hour, is_weekend
ORDER BY consumer_type, hour, is_weekend;


-- Top 20 peak demand hours across all meters
SELECT
    timestamp,
    DATE(timestamp)                          AS date,
    EXTRACT(HOUR FROM timestamp)::INT        AS hour,
    consumer_type,
    ROUND(SUM(consumption_kwh)::NUMERIC, 2) AS total_kwh,
    ROUND(AVG(voltage_v)::NUMERIC, 2)        AS avg_voltage,
    ROUND(AVG(power_factor)::NUMERIC, 4)     AS avg_pf
FROM fact_smart_meter_hourly
GROUP BY timestamp, consumer_type
ORDER BY total_kwh DESC
LIMIT 20;


-- Daily load factor by consumer type
SELECT
    DATE(timestamp)                                             AS date,
    consumer_type,
    ROUND(AVG(consumption_kwh)::NUMERIC, 4)                    AS avg_kwh,
    ROUND(MAX(consumption_kwh)::NUMERIC, 4)                    AS peak_kwh,
    ROUND(
        AVG(consumption_kwh)::NUMERIC / NULLIF(MAX(consumption_kwh)::NUMERIC, 0) * 100,
        2
    )                                                          AS load_factor_pct
FROM fact_smart_meter_hourly
GROUP BY DATE(timestamp), consumer_type
ORDER BY date, consumer_type;


-- Meters with low power factor (below 0.85 target)
SELECT
    meter_id,
    consumer_type,
    ROUND(AVG(power_factor)::NUMERIC, 4)      AS avg_pf,
    COUNT(*) FILTER (WHERE power_factor < 0.85) AS low_pf_hours,
    COUNT(*)                                    AS total_hours,
    ROUND(
        COUNT(*) FILTER (WHERE power_factor < 0.85)::NUMERIC / COUNT(*) * 100, 2
    )                                           AS low_pf_pct
FROM fact_smart_meter_hourly
GROUP BY meter_id, consumer_type
HAVING AVG(power_factor) < 0.88
ORDER BY avg_pf;


-- Outage detection: meters with 2+ consecutive zero-consumption hours
WITH hourly AS (
    SELECT
        meter_id,
        timestamp,
        consumption_kwh,
        LEAD(consumption_kwh) OVER (PARTITION BY meter_id ORDER BY timestamp) AS next_kwh,
        LEAD(consumption_kwh, 2) OVER (PARTITION BY meter_id ORDER BY timestamp) AS next2_kwh
    FROM fact_smart_meter_hourly
)
SELECT
    meter_id,
    timestamp AS suspected_outage_start,
    'Possible 2-hour outage' AS flag
FROM hourly
WHERE consumption_kwh = 0
  AND next_kwh = 0
ORDER BY timestamp;


-- =============================================================================
-- 4. PROVINCE ANALYSIS
-- =============================================================================

-- Province performance ranking (latest year)
SELECT
    p.province_name_en,
    pe.electrification_pct,
    pe.per_capita_kwh,
    pe.annual_consumption_gwh,
    pe.system_loss_pct,
    pe.estimated_loss_gwh,
    pe.estimated_revenue_npr_mn,
    RANK() OVER (ORDER BY pe.per_capita_kwh DESC)          AS rank_per_capita,
    RANK() OVER (ORDER BY pe.electrification_pct DESC)     AS rank_electrification,
    RANK() OVER (ORDER BY pe.system_loss_pct ASC)          AS rank_efficiency
FROM fact_province_electricity pe
JOIN dim_province p ON p.province_id = pe.province_id
WHERE pe.report_year = 2023
ORDER BY pe.annual_consumption_gwh DESC;


-- Electrification gap: households without electricity
SELECT
    p.province_name_en,
    pe.electrification_pct,
    pe.households,
    ROUND(pe.households * (1 - pe.electrification_pct / 100))::INT AS unelectrified_hh,
    ROUND(pe.households * (1 - pe.electrification_pct / 100) * 25000 / 1000000, 2)
        AS est_connection_cost_npr_mn
FROM fact_province_electricity pe
JOIN dim_province p ON p.province_id = pe.province_id
WHERE pe.report_year = 2023
ORDER BY unelectrified_hh DESC;


-- System loss opportunity: revenue if all provinces reach 15% target
SELECT
    p.province_name_en,
    pe.system_loss_pct,
    pe.estimated_loss_gwh,
    pe.revenue_lost_npr_mn,
    GREATEST(0, pe.system_loss_pct - 15)                  AS excess_loss_pp,
    ROUND(pe.annual_consumption_gwh * GREATEST(0, pe.system_loss_pct - 15) / 100, 3)
                                                          AS recoverable_gwh,
    ROUND(pe.annual_consumption_gwh * GREATEST(0, pe.system_loss_pct - 15) / 100 * 9, 2)
                                                          AS recoverable_npr_mn
FROM fact_province_electricity pe
JOIN dim_province p ON p.province_id = pe.province_id
WHERE pe.report_year = 2023
ORDER BY pe.system_loss_pct DESC;


-- =============================================================================
-- 5. WEATHER-DEMAND CORRELATION
-- =============================================================================

-- Monthly consumption vs weather (Kathmandu station)
SELECT
    mc.year,
    mc.month,
    mc.month_name,
    mc.total_gwh,
    mc.peak_demand_mw,
    ROUND(AVG(wd.avg_temp_c)::NUMERIC, 2)    AS avg_temp_c,
    ROUND(SUM(wd.rainfall_mm)::NUMERIC, 1)   AS total_rainfall_mm,
    ROUND(AVG(wd.humidity_pct)::NUMERIC, 1)  AS avg_humidity_pct,
    ROUND(SUM(wd.hdd)::NUMERIC, 2)           AS total_hdd,
    ROUND(SUM(wd.cdd)::NUMERIC, 2)           AS total_cdd
FROM fact_monthly_consumption mc
LEFT JOIN fact_weather_daily wd
    ON wd.station_id = 'KTM001'
    AND EXTRACT(YEAR FROM wd.observation_date) = mc.year
    AND EXTRACT(MONTH FROM wd.observation_date) = mc.month
GROUP BY mc.year, mc.month, mc.month_name, mc.total_gwh, mc.peak_demand_mw
ORDER BY mc.year, mc.month;


-- =============================================================================
-- 6. REVENUE & FINANCIAL
-- =============================================================================

-- Estimated revenue by fiscal year and sector
SELECT
    fiscal_year,
    ROUND(domestic_gwh * 8.5 / 1, 2)    AS domestic_revenue_gwh_x_rate,
    ROUND(industrial_gwh * 8.0, 2)       AS industrial_gwh_x_rate,
    ROUND(commercial_gwh * 11.5, 2)      AS commercial_gwh_x_rate,
    ROUND(irrigation_gwh * 4.5, 2)       AS irrigation_gwh_x_rate,
    ROUND(
        domestic_gwh * 8.5 + industrial_gwh * 8.0 +
        commercial_gwh * 11.5 + irrigation_gwh * 4.5, 2
    )                                    AS total_est_revenue_gwh_x_rate
FROM fact_annual_consumption
ORDER BY fiscal_year;


-- =============================================================================
-- 7. PIPELINE MONITORING
-- =============================================================================

-- Recent pipeline runs
SELECT
    run_id,
    pipeline_name,
    start_time AT TIME ZONE 'Asia/Kathmandu' AS start_npt,
    end_time AT TIME ZONE 'Asia/Kathmandu'   AS end_npt,
    EXTRACT(EPOCH FROM (end_time - start_time)) / 60 AS duration_minutes,
    status,
    records_loaded,
    error_message
FROM etl_pipeline_runs
ORDER BY start_time DESC
LIMIT 20;
