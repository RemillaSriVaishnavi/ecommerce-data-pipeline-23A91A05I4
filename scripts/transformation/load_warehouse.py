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
        dbname=os.getenv("DB_NAME", "ecommerce_db_test"),
        user=os.getenv("DB_USER", "test_user"),
        password=os.getenv("DB_PASSWORD", "test_password"),
    )

# -------------------------------
# HELPER: EXECUTE & LOAD
# -------------------------------

def load_table(select_sql, insert_sql, truncate, conn, table_name):
    cur = conn.cursor()
    cur.execute(select_sql)
    rows = cur.fetchall()

    if not rows:
        logging.warning(f"No data found for {table_name}")
        return

    if truncate:
        cur.execute(f"TRUNCATE TABLE {table_name} CASCADE")

    execute_values(cur, insert_sql, rows)
    conn.commit()

    logging.info(f"Loaded {len(rows)} records into {table_name}")

# -------------------------------
# MAIN WAREHOUSE LOAD
# -------------------------------

def run_load_warehouse():
    logging.info("Starting Warehouse Load")
    conn = get_connection()

    # -------------------------------------------------
    # DIM_CUSTOMERS (SCD TYPE 2)
    # -------------------------------------------------
    load_table(
        select_sql="""
            SELECT
                customer_id,
                first_name,
                last_name,
                email,
                city,
                state,
                country,
                age_group,
                'Standard' AS customer_segment,
                registration_date,
                CURRENT_DATE AS effective_date,
                NULL AS end_date,
                TRUE AS is_current
            FROM production.customers
        """,
        insert_sql="""
            INSERT INTO warehouse.dim_customers (
                customer_id,
                first_name,
                last_name,
                email,
                city,
                state,
                country,
                age_group,
                customer_segment,
                registration_date,
                effective_date,
                end_date,
                is_current
            ) VALUES %s
        """,
        truncate=True,
        conn=conn,
        table_name="warehouse.dim_customers"
    )

    # -------------------------------------------------
    # DIM_PRODUCTS (SCD TYPE 2)
    # -------------------------------------------------
    load_table(
        select_sql="""
            SELECT
                product_id,
                product_name,
                category,
                sub_category,
                brand,
                CASE
                    WHEN price < 500 THEN 'Low'
                    WHEN price < 2000 THEN 'Medium'
                    ELSE 'High'
                END AS price_category,
                CASE
                    WHEN price < 500 THEN '0-500'
                    WHEN price < 2000 THEN '500-2000'
                    ELSE '2000+'
                END AS price_range,
                CURRENT_DATE,
                NULL,
                TRUE
            FROM production.products
        """,
        insert_sql="""
            INSERT INTO warehouse.dim_products (
                product_id,
                product_name,
                category,
                sub_category,
                brand,
                price_category,
                price_range,
                effective_date,
                end_date,
                is_current
            ) VALUES %s
        """,
        truncate=True,
        conn=conn,
        table_name="warehouse.dim_products"
    )

    # -------------------------------------------------
    # DIM_DATE
    # -------------------------------------------------
    load_table(
        select_sql="""
            SELECT DISTINCT
                TO_CHAR(transaction_date, 'YYYYMMDD')::INT AS date_key,
                transaction_date,
                EXTRACT(YEAR FROM transaction_date),
                EXTRACT(QUARTER FROM transaction_date),
                EXTRACT(MONTH FROM transaction_date),
                EXTRACT(DAY FROM transaction_date),
                TO_CHAR(transaction_date, 'Month'),
                TO_CHAR(transaction_date, 'Day'),
                EXTRACT(WEEK FROM transaction_date),
                CASE WHEN EXTRACT(ISODOW FROM transaction_date) IN (6,7) THEN TRUE ELSE FALSE END
            FROM production.transactions
        """,
        insert_sql="""
            INSERT INTO warehouse.dim_date (
                date_key,
                full_date,
                year,
                quarter,
                month,
                day,
                month_name,
                day_name,
                week_of_year,
                is_weekend
            ) VALUES %s
        """,
        truncate=True,
        conn=conn,
        table_name="warehouse.dim_date"
    )

    # -------------------------------------------------
    # DIM_PAYMENT_METHOD
    # -------------------------------------------------
    load_table(
        select_sql="""
            SELECT DISTINCT
                payment_method,
                'Digital'
            FROM production.transactions
        """,
        insert_sql="""
            INSERT INTO warehouse.dim_payment_method (
                payment_method_name,
                payment_type
            ) VALUES %s
        """,
        truncate=True,
        conn=conn,
        table_name="warehouse.dim_payment_method"
    )

    # -------------------------------------------------
    # FACT_SALES
    # -------------------------------------------------
    load_table(
        select_sql="""
            SELECT
                d.date_key,
                dc.customer_key,
                dp.product_key,
                pm.payment_method_key,
                t.transaction_id,
                ti.quantity,
                ti.unit_price,
                (ti.unit_price * ti.quantity - ti.line_total) AS discount_amount,
                ti.line_total,
                ti.line_total - (ti.quantity * p.cost) AS profit
            FROM production.transaction_items ti
            JOIN production.transactions t ON ti.transaction_id = t.transaction_id
            JOIN production.products p ON ti.product_id = p.product_id
            JOIN warehouse.dim_customers dc ON dc.customer_id = t.customer_id AND dc.is_current = TRUE
            JOIN warehouse.dim_products dp ON dp.product_id = p.product_id AND dp.is_current = TRUE
            JOIN warehouse.dim_payment_method pm ON pm.payment_method_name = t.payment_method
            JOIN warehouse.dim_date d ON d.full_date = t.transaction_date
        """,
        insert_sql="""
            INSERT INTO warehouse.fact_sales (
                date_key,
                customer_key,
                product_key,
                payment_method_key,
                transaction_id,
                quantity,
                unit_price,
                discount_amount,
                line_total,
                profit
            ) VALUES %s
        """,
        truncate=True,
        conn=conn,
        table_name="warehouse.fact_sales"
    )

    conn.close()
    logging.info("Warehouse Load Completed Successfully")

# -------------------------------
# ENTRY POINT
# -------------------------------
if __name__ == "__main__":
    run_load_warehouse()
