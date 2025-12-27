# scripts/scheduler.py

import schedule
import time
import subprocess
import yaml
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
import signal
import sys

# -------------------------------
# Base paths
# -------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

LOCK_FILE = BASE_DIR / "pipeline.lock"
LOG_FILE = LOG_DIR / "scheduler_activity.log"

# -------------------------------
# Logging setup
# -------------------------------
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# -------------------------------
# Load config
# -------------------------------
CONFIG_PATH = BASE_DIR / "config" / "config.yaml"

if not CONFIG_PATH.exists():
    raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

scheduler_cfg = config.get("scheduler", {})
RUN_TIME = scheduler_cfg.get("daily_run_time", "02:00")
TIMEZONE = config["scheduler"].get("timezone", "UTC")

logging.info(f"Scheduler timezone: {TIMEZONE} (documented, system-based)")

# -------------------------------
# Concurrency helpers
# -------------------------------
def is_pipeline_running() -> bool:
    return LOCK_FILE.exists()

def create_lock():
    LOCK_FILE.write_text(str(os.getpid()))

def remove_lock():
    if LOCK_FILE.exists():
        LOCK_FILE.unlink()

# -------------------------------
# Pipeline execution
# -------------------------------
def run_pipeline():
    if is_pipeline_running():
        logging.warning("Pipeline already running. Skipping execution.")
        return

    logging.info("Starting scheduled pipeline execution")
    create_lock()

    start_time = datetime.now(timezone.utc)

    try:
        result = subprocess.run(
            ["python", str(BASE_DIR / "scripts" / "pipeline_orchestrator.py")],
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode == 0:
            logging.info("Pipeline completed successfully")
        else:
            logging.error("Pipeline failed")
            logging.error(result.stderr)

    except Exception as e:
        logging.exception(f"Scheduler execution error: {e}")

    finally:
        remove_lock()
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        logging.info(f"Pipeline duration: {duration:.2f} seconds")

        # Cleanup runs regardless of success/failure
        subprocess.run(
            ["python", str(BASE_DIR / "scripts" / "cleanup_old_data.py")],
            capture_output=True,
            text=True,
        )

# -------------------------------
# Graceful shutdown
# -------------------------------
def shutdown_handler(signum, frame):
    logging.info("Scheduler stopped by user/system")
    remove_lock()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# -------------------------------
# Scheduler runner
# -------------------------------
def run_scheduler():
    schedule.every().day.at(RUN_TIME).do(run_pipeline)

    logging.info("========== SCHEDULER STARTED ==========")
    logging.info(f"Pipeline scheduled daily at {RUN_TIME} ({TIMEZONE})")

    while True:
        schedule.run_pending()
        time.sleep(30)

# -------------------------------
# Entry point
# -------------------------------
if __name__ == "__main__":
    run_scheduler()
