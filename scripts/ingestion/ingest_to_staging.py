import json
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
import yaml
from psycopg2.extras import execute_values


# --------------------------------------------------
# Utility: Load configuration
# --------------------------------------------------
def load_config():
    with open("config/config.yaml", "r") as f:
        return yaml.safe_load(f)


# --------------------------------------------------
# Utility: Get database connection
# --------------------------------------------------
def get_db_connection():
    config = load_config()
    db = config["database"]

    return psycopg2.connect(
        host=db.get("host"),
        port=db.get("port"),
        database=db.get("name"),
        user=db.get("user"),
        password=db.get("password")
    )


# --------------------------------------------------
# Bulk insert helper (NO row-by-row inserts)
# --------------------------------------------------
def bulk_insert_data(df: pd.DataFrame, table_name: str, connection) -> int:
    if df.empty:
        return 0

    columns = list(df.columns)
    values = [tuple(x) for x in df.to_numpy()]

    insert_sql = f"""
        INSERT INTO {table_name} ({",".join(columns)})
        VALUES %s
    """

    with connection.cursor() as cursor:
        execute_values(cursor, insert_sql, values, page_size=1000)

    return len(df)


# --------------------------------------------------
# Load single CSV into staging table
# --------------------------------------------------
def load_csv_to_staging(csv_path: str, table_name: str, connection) -> dict:
    result = {
        "rows_loaded": 0,
        "status": "failed",
        "error_message": None
    }

    try:
        df = pd.read_csv(csv_path)

        rows = bulk_insert_data(df, table_name, connection)

        result["rows_loaded"] = rows
        result["status"] = "success"

    except FileNotFoundError:
        result["error_message"] = f"CSV file not found: {csv_path}"
        raise

    except Exception as e:
        result["error_message"] = str(e)
        raise

    return result


# --------------------------------------------------
# Validate staging load (DB count vs CSV count)
# --------------------------------------------------
def validate_staging_load(connection) -> dict:
    validation = {}

    tables = {
        "staging.customers": "data/raw/customers.csv",
        "staging.products": "data/raw/products.csv",
        "staging.transactions": "data/raw/transactions.csv",
        "staging.transaction_items": "data/raw/transaction_items.csv"
    }

    with connection.cursor() as cursor:
        for table, csv_path in tables.items():
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            db_count = cursor.fetchone()[0]

            csv_count = len(pd.read_csv(csv_path))

            validation[table] = {
                "csv_rows": csv_count,
                "db_rows": db_count,
                "match": csv_count == db_count
            }

    validation["overall_status"] = all(
        v["match"] for v in validation.values() if isinstance(v, dict)
    )

    return validation


# --------------------------------------------------
# MAIN EXECUTION (Manual run)
# --------------------------------------------------
if __name__ == "__main__":
    start_time = time.time()
    summary = {
        "ingestion_timestamp": datetime.utcnow().isoformat(),
        "tables_loaded": {},
        "total_execution_time_seconds": 0.0
    }

    data_path = Path("data/raw")
    output_path = Path("data/staging")
    output_path.mkdir(parents=True, exist_ok=True)

    tables = [
        ("data/raw/customers.csv", "staging.customers"),
        ("data/raw/products.csv", "staging.products"),
        ("data/raw/transactions.csv", "staging.transactions"),
        ("data/raw/transaction_items.csv", "staging.transaction_items"),
    ]

    connection = None

    try:
        connection = get_db_connection()
        connection.autocommit = False

        with connection.cursor() as cursor:
            # Truncate staging tables (idempotent)
            cursor.execute("TRUNCATE staging.transaction_items")
            cursor.execute("TRUNCATE staging.transactions")
            cursor.execute("TRUNCATE staging.products")
            cursor.execute("TRUNCATE staging.customers")

        # Load tables
        for csv_file, table_name in tables:
            result = load_csv_to_staging(csv_file, table_name, connection)
            summary["tables_loaded"][table_name] = result

        # Validate load
        validation = validate_staging_load(connection)
        summary["validation"] = validation

        if not validation["overall_status"]:
            raise Exception("Row count validation failed")

        connection.commit()

    except Exception as e:
        if connection:
            connection.rollback()
        summary["error"] = str(e)

    finally:
        if connection:
            connection.close()

    end_time = time.time()
    summary["total_execution_time_seconds"] = round(end_time - start_time, 2)

    with open(output_path / "ingestion_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print("Staging ingestion completed.")
