import subprocess
import time
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
import traceback
import sys

# -------------------------------
# Base paths
# -------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
REPORT_DIR = BASE_DIR / "data" / "processed"

LOG_DIR.mkdir(exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

MAIN_LOG_FILE = LOG_DIR / f"pipeline_orchestrator_{timestamp}.log"
ERROR_LOG_FILE = LOG_DIR / "pipeline_errors.log"
REPORT_FILE = REPORT_DIR / "pipeline_execution_report.json"

# -------------------------------
# Logging Configuration
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(MAIN_LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)

error_logger = logging.getLogger("pipeline_errors")
error_logger.setLevel(logging.ERROR)
error_handler = logging.FileHandler(ERROR_LOG_FILE)
error_logger.addHandler(error_handler)

# -------------------------------
# Pipeline Steps
# -------------------------------
PIPELINE_STEPS = [
    ("data_generation", "scripts/data_generation/generate_data.py"),
    ("data_ingestion", "scripts/ingestion/ingest_to_staging.py"),
    ("data_quality_checks", "scripts/quality_checks/data_quality_checks.py"),
    ("staging_to_production", "scripts/transformation/staging_to_production.py"),
    ("warehouse_load", "scripts/transformation/load_warehouse.py"),
    ("analytics_generation", "scripts/transformation/generate_analytics.py"),
]

MAX_RETRIES = 3
BACKOFF_SECONDS = [1, 2, 4]

# -------------------------------
# Helper: Run a step with retries
# -------------------------------
def run_step(step_name: str, script_path: str) -> dict:
    start_time = time.time()
    script_file = BASE_DIR / script_path

    if not script_file.exists():
        error_msg = f"Script not found: {script_file}"
        error_logger.error(error_msg)
        return {
            "status": "failed",
            "duration_seconds": 0,
            "retry_attempts": 0,
            "error_message": error_msg,
        }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logging.info(f"Starting step: {step_name} (Attempt {attempt})")

            result = subprocess.run(
                [sys.executable, str(script_file)],
                check=True,
                capture_output=True,
                text=True,
                cwd=BASE_DIR,
            )

            if result.stdout:
                logging.info(result.stdout.strip())

            duration = round(time.time() - start_time, 2)
            logging.info(f"Completed step: {step_name} in {duration}s")

            return {
                "status": "success",
                "duration_seconds": duration,
                "retry_attempts": attempt - 1,
            }

        except subprocess.CalledProcessError as e:
            error_logger.error(f"Error in step: {step_name}")
            error_logger.error(e.stderr)
            error_logger.error(traceback.format_exc())

            if attempt == MAX_RETRIES:
                duration = round(time.time() - start_time, 2)
                return {
                    "status": "failed",
                    "duration_seconds": duration,
                    "retry_attempts": attempt,
                    "error_message": e.stderr.strip(),
                }

            sleep_time = BACKOFF_SECONDS[attempt - 1]
            logging.warning(f"Retrying {step_name} after {sleep_time}s")
            time.sleep(sleep_time)

# -------------------------------
# Main Orchestrator
# -------------------------------
def main():
    pipeline_start = datetime.now(timezone.utc)
    execution_id = f"PIPE_{timestamp}"

    logging.info("========== PIPELINE STARTED ==========")

    steps_report = {}
    errors = []
    warnings = []

    for step_name, script_path in PIPELINE_STEPS:
        result = run_step(step_name, script_path)
        steps_report[step_name] = result

        if result["status"] != "success":
            errors.append(f"{step_name} failed")
            break

    pipeline_end = datetime.now(timezone.utc)
    total_duration = round(
        (pipeline_end - pipeline_start).total_seconds(), 2
    )

    final_status = (
        "success"
        if all(step["status"] == "success" for step in steps_report.values())
        else "failed"
    )

    report = {
        "pipeline_execution_id": execution_id,
        "start_time": pipeline_start.isoformat(),
        "end_time": pipeline_end.isoformat(),
        "total_duration_seconds": total_duration,
        "status": final_status,
        "steps_executed": steps_report,
        "errors": errors,
        "warnings": warnings,
    }

    with open(REPORT_FILE, "w") as f:
        json.dump(report, f, indent=4)

    logging.info("========== PIPELINE FINISHED ==========")
    logging.info(f"Final Status: {final_status}")
    logging.info(f"Execution Report written to: {REPORT_FILE}")

# -------------------------------
# Entry Point (CRITICAL)
# -------------------------------
if __name__ == "__main__":
    main()
