"""
metrics.py
Pipeline health monitoring for Nepal Electricity Analytics Pipeline.
Exposes Prometheus-compatible metrics and sends alerts on threshold breaches.
"""

import logging
import time
from datetime import datetime
from pathlib import Path
import yaml

with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

logger = logging.getLogger(__name__)

THRESHOLDS = config["monitoring"]["thresholds"]


class PipelineMetrics:
    """Lightweight metrics collector compatible with Prometheus client."""

    def __init__(self):
        self.metrics = {}
        self.start_time = datetime.now()

    def set(self, name: str, value: float, labels: dict = None):
        key = name + (str(sorted(labels.items())) if labels else "")
        self.metrics[key] = {"name": name, "value": value, "labels": labels or {}, "ts": time.time()}

    def increment(self, name: str, amount: float = 1.0):
        current = self.metrics.get(name, {}).get("value", 0)
        self.set(name, current + amount)

    def check_thresholds(self) -> list:
        """Return list of threshold breach alerts."""
        alerts = []

        duration = (datetime.now() - self.start_time).total_seconds() / 60
        if duration > THRESHOLDS["pipeline_max_duration_minutes"]:
            alerts.append({
                "severity": "WARNING",
                "metric": "pipeline_duration_minutes",
                "value": round(duration, 1),
                "threshold": THRESHOLDS["pipeline_max_duration_minutes"],
                "message": f"Pipeline running for {duration:.1f}min (limit {THRESHOLDS['pipeline_max_duration_minutes']}min)",
            })

        for key, m in self.metrics.items():
            if m["name"] == "system_loss_pct" and m["value"] > THRESHOLDS["system_loss_alert_pct"]:
                alerts.append({
                    "severity": "WARNING",
                    "metric": "system_loss_pct",
                    "value": m["value"],
                    "threshold": THRESHOLDS["system_loss_alert_pct"],
                    "message": f"System loss {m['value']}% exceeds alert threshold",
                })
            if m["name"] == "peak_demand_mw" and m["value"] > THRESHOLDS["peak_demand_alert_mw"]:
                alerts.append({
                    "severity": "CRITICAL",
                    "metric": "peak_demand_mw",
                    "value": m["value"],
                    "threshold": THRESHOLDS["peak_demand_alert_mw"],
                    "message": f"Peak demand {m['value']} MW exceeds alert threshold {THRESHOLDS['peak_demand_alert_mw']} MW",
                })

        return alerts

    def to_prometheus_text(self) -> str:
        """Export metrics in Prometheus text format."""
        lines = []
        for key, m in self.metrics.items():
            name = f"nea_pipeline_{m['name']}"
            labels = ",".join(f'{k}="{v}"' for k, v in m["labels"].items())
            label_str = f"{{{labels}}}" if labels else ""
            lines.append(f"# TYPE {name} gauge")
            lines.append(f"{name}{label_str} {m['value']}")
        return "\n".join(lines)

    def log_summary(self):
        logger.info("=== Pipeline Metrics Summary ===")
        for key, m in self.metrics.items():
            label_info = " ".join(f"{k}={v}" for k, v in m["labels"].items())
            logger.info(f"  {m['name']} {label_info}: {m['value']}")

        alerts = self.check_thresholds()
        if alerts:
            for a in alerts:
                logger.warning(f"ALERT [{a['severity']}] {a['message']}")
        else:
            logger.info("All metrics within thresholds")


# Global metrics instance for use across pipeline
pipeline_metrics = PipelineMetrics()


def record_extraction_stats(source: str, rows: int, duration_s: float):
    pipeline_metrics.set("extraction_rows",     rows,       {"source": source})
    pipeline_metrics.set("extraction_duration", duration_s, {"source": source})
    logger.info(f"Metrics: extracted {rows} rows from {source} in {duration_s:.1f}s")


def record_load_stats(table: str, rows: int, errors: int = 0):
    pipeline_metrics.set("load_rows",   rows,   {"table": table})
    pipeline_metrics.set("load_errors", errors, {"table": table})
    if errors:
        logger.warning(f"Metrics: {errors} load errors for {table}")


def record_data_quality(dataset: str, total: int, valid: int):
    pct = valid / total * 100 if total > 0 else 0
    pipeline_metrics.set("data_quality_pct", pct, {"dataset": dataset})
    if pct < THRESHOLDS["data_quality_min_pct"]:
        logger.warning(
            f"DATA QUALITY ALERT: {dataset} quality {pct:.1f}% "
            f"below threshold {THRESHOLDS['data_quality_min_pct']}%"
        )


def record_peak_demand(mw: float):
    pipeline_metrics.set("peak_demand_mw", mw)
    if mw > THRESHOLDS["peak_demand_alert_mw"]:
        logger.critical(f"PEAK DEMAND ALERT: {mw} MW exceeds threshold {THRESHOLDS['peak_demand_alert_mw']} MW!")


if __name__ == "__main__":
    # Demo run
    record_extraction_stats("nea_annual", 9, 0.5)
    record_extraction_stats("smart_meter", 72000, 8.2)
    record_load_stats("fact_smart_meter_hourly", 72000, 0)
    record_data_quality("smart_meter", 72000, 71850)
    record_peak_demand(2212.0)
    pipeline_metrics.log_summary()
    print("\n=== Prometheus Export ===")
    print(pipeline_metrics.to_prometheus_text())
