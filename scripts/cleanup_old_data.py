# scripts/cleanup_old_data.py

import os
import time
import yaml
import logging
from datetime import datetime, timezone
from pathlib import Path

# -------------------------------
# Base paths
# -------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

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
# Load config safely
# -------------------------------
CONFIG_PATH = BASE_DIR / "config" / "config.yaml"

if not CONFIG_PATH.exists():
    raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

RETENTION_DAYS = int(config["scheduler"].get("retention_days", 7))
RETENTION_SECONDS = RETENTION_DAYS * 24 * 60 * 60

# -------------------------------
# Cleanup targets (ABSOLUTE)
# -------------------------------
TARGET_DIRS = [
    BASE_DIR / "data" / "raw",
    BASE_DIR / "data" / "staging",
    BASE_DIR / "logs",
]

# -------------------------------
# Preservation rules
# -------------------------------
PRESERVE_KEYWORDS = ["summary", "report", "metadata"]

today_str = datetime.now(timezone.utc).strftime("%Y%m%d")

def should_preserve(filename: str) -> bool:
    filename_lower = filename.lower()

    # Preserve today's files
    if today_str in filename:
        return True

    # Preserve reports & metadata
    for keyword in PRESERVE_KEYWORDS:
        if keyword in filename_lower:
            return True

    return False

# -------------------------------
# Cleanup logic
# -------------------------------
def cleanup():
    now = time.time()
    logging.info("========== CLEANUP JOB STARTED ==========")
    logging.info(f"Retention policy: {RETENTION_DAYS} days")

    deleted_files = 0
    skipped_files = 0

    for directory in TARGET_DIRS:
        if not directory.exists():
            logging.warning(f"Directory does not exist: {directory}")
            continue

        for file_path in directory.iterdir():
            if not file_path.is_file():
                continue

            if should_preserve(file_path.name):
                skipped_files += 1
                continue

            file_age_seconds = now - file_path.stat().st_mtime

            if file_age_seconds > RETENTION_SECONDS:
                try:
                    file_path.unlink()
                    deleted_files += 1
                    logging.info(f"Deleted old file: {file_path}")
                except Exception as e:
                    logging.error(f"Failed to delete {file_path}: {e}")
            else:
                skipped_files += 1

    logging.info(
        f"Cleanup completed | Deleted: {deleted_files} | Skipped: {skipped_files}"
    )
    logging.info("========== CLEANUP JOB FINISHED ==========")

# -------------------------------
# Entry point
# -------------------------------
if __name__ == "__main__":
    cleanup()
