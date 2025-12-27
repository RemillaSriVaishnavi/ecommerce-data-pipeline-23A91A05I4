import os
import logging
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

# -------------------------------
# LOGGING
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# -------------------------------
# DATABASE CONNECTION
# -------------------------------

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "ecommerce_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
    )

# -------------------------------
# LOAD DIMENSIONS
# -------------------------------

def load_dimension(query, target_table, conn, truncate=True):
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]

    if not rows:
        logging.warning(f"No data found for {target_table}")
        return

    if truncate:
        cur.execute(f"TRUNCATE TABLE {target_table} CASCADE")

    insert_sql = f"""
        INSERT INTO {target_table} ({','.join(cols)})
        VALUES %s
    """

    execute_values(cur, insert_sql, rows)
    conn.commit()

    logging.info(f"Loaded {len(rows)} records into {target_table}")

# -------------------------------
# LOAD FACT TABLE
# -------------------------------

def load_fact(query, target_table, conn):
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]

    if not rows:
        logging.warning(f"No data found for {target_table}")
        return

    cur.execute(f"TRUNCATE TABLE {target_table}")

    insert_sql = f"""
        INSERT INTO {target_table} ({','.join(cols)})
        VALUES %s
    """

    execute_values(cur, insert_sql, rows)
    conn.commit()

    logging.info(f"Loaded {len(rows)} records into {target_table}")

# -------------------------------
# MAIN WAREHOUSE LOAD
# -------------------------------

def run_load_warehouse():
    logging.info("Starting Warehouse Load")

    conn = get_connection()

    # -------------------------------
    # DIMENSIONS
    # -------------------------------

    load_dimension(
        """
        SELECT DISTINCT
            customer_id,
            first_name,
            last_name,
            email,
            state
        FROM production.customers
        """,
        "warehouse.dim_customers",
        conn,
    )

    load_dimension(
        """
        SELECT DISTINCT
            product_id,
            product_name,
            category,
            price,
            price_category
        FROM production.products
        """,
        "warehouse.dim_products",
        conn,
    )

    load_dimension(
        """
        SELECT DISTINCT
            DATE(transaction_date) AS date,
            EXTRACT(YEAR FROM transaction_date) AS year,
            EXTRACT(MONTH FROM transaction_date) AS month,
            EXTRACT(DAY FROM transaction_date) AS day
        FROM production.transactions
        """,
        "warehouse.dim_date",
        conn,
    )

    load_dimension(
        """
        SELECT DISTINCT
            payment_method
        FROM production.transactions
        """,
        "warehouse.dim_payment_method",
        conn,
    )

    # -------------------------------
    # FACT TABLE
    # -------------------------------

    load_fact(
        """
        SELECT
            t.transaction_id,
            t.customer_id,
            i.product_id,
            DATE(t.transaction_date) AS date,
            t.payment_method,
            i.quantity,
            i.unit_price,
            i.line_total
        FROM production.transactions t
        JOIN production.transaction_items i
            ON t.transaction_id = i.transaction_id
        """,
        "warehouse.fact_sales",
        conn,
    )

    conn.close()
    logging.info("Warehouse Load Completed Successfully")

# -------------------------------
# ENTRY POINT
# -------------------------------

if __name__ == "__main__":
    run_load_warehouse()
