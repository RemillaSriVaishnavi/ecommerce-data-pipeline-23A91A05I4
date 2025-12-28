import json
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
import yaml
from psycopg2.extras import execute_values


# --------------------------------------------------
# Utility: Load configuration (CI SAFE)
# --------------------------------------------------
def load_config():
    config_path = Path("config/config.yaml")

    config = {}

    if config_path.exists():
        with open(config_path, "r") as f:
            config = yaml.safe_load(f) or {}

    # Environment variables override config.yaml
    db_config = {
        "host": os.getenv("DB_HOST", config.get("database", {}).get("host", "localhost")),
        "port": int(os.getenv("DB_PORT", config.get("database", {}).get("port", 5432))),
        "name": os.getenv("DB_NAME", config.get("database", {}).get("name", "ecommerce_db_test")),
        "user": os.getenv("DB_USER", config.get("database", {}).get("user", "test_user")),
        "password": os.getenv("DB_PASSWORD", config.get("database", {}).get("password", "test_password")),
    }

    return {"database": db_config}


# --------------------------------------------------
# Utility: Get database connection
# --------------------------------------------------
def get_db_connection():
    config = load_config()
    db = config["database"]

    return psycopg2.connect(
        host=db["host"],
        port=db["port"],
        database=db["name"],
        user=db["user"],
        password=db["password"],
    )


# --------------------------------------------------
# Bulk insert helper
# --------------------------------------------------
def bulk_insert_data(df: pd.DataFrame, table_name: str, connection) -> int:
    if df.empty:
        print(f"No data to insert into {table_name}")
        return 0

    columns = list(df.columns)
    values = [tuple(x) for x in df.to_numpy()]

    insert_sql = f"""
        INSERT INTO {table_name} ({",".join(columns)})
        VALUES %s
    """

    with connection.cursor() as cursor:
        execute_values(cursor, insert_sql, values, page_size=1000)

    print(f"Inserted {len(df)} rows into {table_name}")
    return len(df)


# --------------------------------------------------
# Load CSV into staging
# --------------------------------------------------
def load_csv_to_staging(csv_path: str, table_name: str, connection) -> dict:
    df = pd.read_csv(csv_path)

    rows = bulk_insert_data(df, table_name, connection)

    return {
        "rows_loaded": rows,
        "status": "success" if rows > 0 else "empty"
    }


# --------------------------------------------------
# Validate staging load
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
        v["match"] for v in validation.values()
    )

    return validation


# --------------------------------------------------
# MAIN EXECUTION
# --------------------------------------------------
if __name__ == "__main__":
    start_time = time.time()

    summary = {
        "ingestion_timestamp": datetime.utcnow().isoformat(),
        "tables_loaded": {},
        "total_execution_time_seconds": 0.0
    }

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
            cursor.execute("TRUNCATE staging.transaction_items CASCADE")
            cursor.execute("TRUNCATE staging.transactions CASCADE")
            cursor.execute("TRUNCATE staging.products CASCADE")
            cursor.execute("TRUNCATE staging.customers CASCADE")

        for csv_file, table_name in tables:
            result = load_csv_to_staging(csv_file, table_name, connection)
            summary["tables_loaded"][table_name] = result

        validation = validate_staging_load(connection)
        summary["validation"] = validation

        if not validation["overall_status"]:
            raise Exception("Row count validation failed")

        connection.commit()
        print("Staging ingestion successful")

    except Exception as e:
        if connection:
            connection.rollback()
        summary["error"] = str(e)
        print("Staging ingestion failed:", e)

    finally:
        if connection:
            connection.close()

    summary["total_execution_time_seconds"] = round(time.time() - start_time, 2)

    with open(output_path / "ingestion_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
