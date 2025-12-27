import psycopg2
import json
import time
from datetime import datetime, timezone
import statistics
import os

# -------------------------------------------------
# Paths
# -------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

PIPELINE_REPORT_PATH = os.path.join(
    BASE_DIR, "data", "processed", "pipeline_execution_report.json"
)

MONITORING_SQL_PATH = os.path.join(
    BASE_DIR, "sql", "queries", "monitoring_queries.sql"
)

OUTPUT_PATH = os.path.join(
    BASE_DIR, "data", "processed", "monitoring_report.json"
)

# -------------------------------------------------
# DB Config
# -------------------------------------------------
DB_CONFIG = {
    "host": "localhost",
    "database": "ecommerce_db",
    "user": "admin",
    "password": "admin",
    "port": 5432
}

# -------------------------------------------------
# Helpers
# -------------------------------------------------
def load_pipeline_report():
    with open(PIPELINE_REPORT_PATH) as f:
        return json.load(f)


def run_sql_queries(cursor):
    """
    Executes monitoring SQL queries safely.
    Returns None for all DB-based results if DB is unavailable.
    """
    if cursor is None:
        return None, None, None, None

    with open(MONITORING_SQL_PATH) as f:
        queries = [q.strip() for q in f.read().split(";") if q.strip()]

    results = []
    for q in queries:
        cursor.execute(q)
        results.append(cursor.fetchall())

    return results


# -------------------------------------------------
# Main
# -------------------------------------------------
def main():
    monitoring_time = datetime.now(timezone.utc).isoformat()
    alerts = []

    # =================================================
    # 1. Pipeline Execution Health
    # =================================================
    pipeline_report = load_pipeline_report()

    last_run_time = datetime.fromisoformat(pipeline_report["end_time"])
    if last_run_time.tzinfo is None:
        last_run_time = last_run_time.replace(tzinfo=timezone.utc)

    hours_since_last_run = (
        datetime.now(timezone.utc) - last_run_time
    ).total_seconds() / 3600

    last_execution_status = "ok"
    if hours_since_last_run > 25:
        last_execution_status = "critical"
        alerts.append({
            "severity": "critical",
            "check": "last_execution",
            "message": "Pipeline has not run in last 25 hours",
            "timestamp": monitoring_time
        })

    # =================================================
    # 2. Database Connectivity
    # =================================================
    conn = None
    cursor = None
    db_status = "ok"
    response_time_ms = None
    active_connections = None

    try:
        start = time.time()
        conn = psycopg2.connect(**DB_CONFIG)
        response_time_ms = round((time.time() - start) * 1000, 2)
        cursor = conn.cursor()
    except Exception as e:
        db_status = "error"
        alerts.append({
            "severity": "critical",
            "check": "database_connectivity",
            "message": f"Database connection failed: {str(e)}",
            "timestamp": monitoring_time
        })

    # =================================================
    # 3. Run Monitoring SQL Queries
    # =================================================
    sql_results = run_sql_queries(cursor)

    # Default-safe values
    freshness_map = {}
    volume_rows = []
    quality_rows = [(0, 0, 0)]
    connections_rows = [(0,)]

    if sql_results and sql_results[0] is not None:
        freshness_rows, volume_rows, quality_rows, connections_rows = sql_results

        freshness_map = {row[0]: row[1] for row in freshness_rows}
        active_connections = connections_rows[0][0  ]
    else:
        freshness_map = {}
        volume_rows = []
        quality_rows = [(0, 0, 0)]
        active_connections = None

    now = datetime.now(timezone.utc)

    # =================================================
    # 4. Data Freshness
    # =================================================
    freshness_status = "ok"

    if freshness_map:
        staging_lag = (now - freshness_map["staging"]).total_seconds() / 3600
        production_lag = (now - freshness_map["production"]).total_seconds() / 3600
        warehouse_lag = (now - freshness_map["warehouse"]).total_seconds() / 3600

        if staging_lag > 24 or production_lag > 1 or warehouse_lag > 1:
            freshness_status = "warning"
            alerts.append({
                "severity": "warning",
                "check": "data_freshness",
                "message": "Data freshness lag detected",
                "timestamp": monitoring_time
            })
    else:
        staging_lag = production_lag = warehouse_lag = None

    # =================================================
    # 5. Volume Anomaly Detection
    # =================================================
    volume_status = "ok"
    anomaly_detected = False
    anomaly_type = None
    today_count = None
    mean = std_dev = 0

    if volume_rows:
        counts = [r[1] for r in volume_rows]
        today_count = counts[-1]
        mean = statistics.mean(counts)
        std_dev = statistics.stdev(counts) if len(counts) > 1 else 0

        anomaly_detected = (
            today_count > mean + (3 * std_dev) or
            today_count < mean - (3 * std_dev)
        )

        if anomaly_detected:
            volume_status = "anomaly_detected"
            anomaly_type = "spike" if today_count > mean else "drop"
            alerts.append({
                "severity": "warning",
                "check": "data_volume",
                "message": f"Volume anomaly detected: {anomaly_type}",
                "timestamp": monitoring_time
            })

    # =================================================
    # 6. Data Quality
    # =================================================
    orphan_products, orphan_customers, nulls = quality_rows[0]
    quality_score = max(0, 100 - (orphan_products + orphan_customers + nulls))

    quality_status = "ok"
    if quality_score < 95:
        quality_status = "degraded"
        alerts.append({
            "severity": "warning",
            "check": "data_quality",
            "message": "Data quality score below threshold",
            "timestamp": monitoring_time
        })

    # =================================================
    # 7. Overall Health
    # =================================================
    pipeline_health = "healthy"
    if any(a["severity"] == "critical" for a in alerts):
        pipeline_health = "critical"
    elif alerts:
        pipeline_health = "degraded"

    # =================================================
    # 8. Final Monitoring Report
    # =================================================
    monitoring_report = {
        "monitoring_timestamp": monitoring_time,
        "pipeline_health": pipeline_health,
        "checks": {
            "last_execution": {
                "status": last_execution_status,
                "last_run": last_run_time.isoformat(),
                "hours_since_last_run": round(hours_since_last_run, 2),
                "threshold_hours": 25
            },
            "data_freshness": {
                "status": freshness_status,
                "staging_latest_record": freshness_map.get("staging"),
                "production_latest_record": freshness_map.get("production"),
                "warehouse_latest_record": freshness_map.get("warehouse"),
                "max_lag_hours": (
                    round(max(staging_lag, production_lag, warehouse_lag), 2)
                    if freshness_map else None
                )
            },
            "data_volume_anomalies": {
                "status": volume_status,
                "expected_range": (
                    f"{int(mean - 3*std_dev)}-{int(mean + 3*std_dev)}"
                    if volume_rows else None
                ),
                "actual_count": today_count,
                "anomaly_detected": anomaly_detected,
                "anomaly_type": anomaly_type
            },
            "data_quality": {
                "status": quality_status,
                "quality_score": quality_score,
                "orphan_records": orphan_products + orphan_customers,
                "null_violations": nulls
            },
            "database_connectivity": {
                "status": db_status,
                "response_time_ms": response_time_ms,
                "connections_active": active_connections
            }
        },
        "alerts": alerts,
        "overall_health_score": quality_score
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(monitoring_report, f, indent=4)

    print("Monitoring report generated successfully")


if __name__ == "__main__":
    main()
