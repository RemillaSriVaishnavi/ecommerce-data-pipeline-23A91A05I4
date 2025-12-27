import os
import re
import json
import logging
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# -------------------------------
# LOGGING SETUP
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# -------------------------------
# DATA CLEANSING FUNCTIONS
# -------------------------------

def cleanse_customer_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

    df["email"] = df["email"].apply(lambda x: x.lower() if isinstance(x, str) else x)
    df["phone"] = df["phone"].apply(
        lambda x: re.sub(r"\D", "", x) if isinstance(x, str) else x
    )

    df["first_name"] = df["first_name"].apply(lambda x: x.title() if isinstance(x, str) else x)
    df["last_name"] = df["last_name"].apply(lambda x: x.title() if isinstance(x, str) else x)

    return df


def cleanse_product_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    df["price"] = df["price"].round(2)
    df["cost"] = df["cost"].round(2)

    df["profit_margin"] = ((df["price"] - df["cost"]) / df["price"] * 100).round(2)

    def price_category(price):
        if price < 50:
            return "Budget"
        elif price < 200:
            return "Mid-range"
        return "Premium"

    df["price_category"] = df["price"].apply(price_category)

    return df


def cleanse_transaction_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["total_amount"] = df["total_amount"].round(2)
    df = df[df["total_amount"] > 0]
    return df


def cleanse_transaction_items(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df[df["quantity"] > 0]

    df["line_total"] = (
        df["quantity"]
        * df["unit_price"]
        * (1 - df["discount_percentage"] / 100)
    ).round(2)

    return df


# -------------------------------
# LOAD TO PRODUCTION
# -------------------------------

def load_to_production(df: pd.DataFrame, table_name: str, conn, strategy: str) -> dict:
    if df.empty:
        return {"inserted": 0, "status": "skipped"}

    cur = conn.cursor()
    cols = list(df.columns)
    values = [tuple(row) for row in df.to_numpy()]

    if strategy == "truncate":
        cur.execute(f"TRUNCATE TABLE {table_name} CASCADE")

    insert_sql = f"""
        INSERT INTO {table_name} ({','.join(cols)})
        VALUES %s
    """

    execute_values(cur, insert_sql, values)
    conn.commit()

    return {"inserted": len(df), "status": "success"}


# -------------------------------
# MAIN ETL PROCESS
# -------------------------------

def run_staging_to_production_etl():
    logging.info("Starting Staging → Production ETL")

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "ecommerce_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
    )

    summary = {
        "transformation_timestamp": datetime.utcnow().isoformat(),
        "records_processed": {},
        "transformations_applied": [
            "text_normalization",
            "email_standardization",
            "phone_standardization",
            "profit_margin_calculation",
            "price_categorization",
            "invalid_record_filtering",
            "line_total_recalculation",
        ],
    }

    customers = pd.read_sql("SELECT * FROM staging.customers", conn)
    products = pd.read_sql("SELECT * FROM staging.products", conn)
    transactions = pd.read_sql("SELECT * FROM staging.transactions", conn)
    items = pd.read_sql("SELECT * FROM staging.transaction_items", conn)

    for df in [customers, products, transactions, items]:
        if "loaded_at" in df.columns:
            df.drop(columns=["loaded_at"], inplace=True)

    customers_clean = cleanse_customer_data(customers)
    products_clean = cleanse_product_data(products)
    transactions_clean = cleanse_transaction_data(transactions)
    items_clean = cleanse_transaction_items(items)

    summary["records_processed"]["production.customers"] = load_to_production(
        customers_clean, "production.customers", conn, "truncate"
    )

    summary["records_processed"]["production.products"] = load_to_production(
        products_clean, "production.products", conn, "truncate"
    )

    cur = conn.cursor()
    cur.execute("SELECT transaction_id FROM production.transactions")
    existing_txns = {r[0] for r in cur.fetchall()}

    new_txns = transactions_clean[
        ~transactions_clean["transaction_id"].isin(existing_txns)
    ]

    summary["records_processed"]["production.transactions"] = load_to_production(
        new_txns, "production.transactions", conn, "incremental"
    )

    cur.execute("SELECT item_id FROM production.transaction_items")
    existing_items = {r[0] for r in cur.fetchall()}

    new_items = items_clean[
        ~items_clean["item_id"].isin(existing_items)
    ]

    summary["records_processed"]["production.transaction_items"] = load_to_production(
        new_items, "production.transaction_items", conn, "incremental"
    )

    os.makedirs("data/processed", exist_ok=True)
    with open("data/processed/transformation_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    conn.close()
    logging.info("Staging → Production ETL completed successfully")


# -------------------------------
# ENTRY POINT
# -------------------------------

if __name__ == "__main__":
    run_staging_to_production_etl()
