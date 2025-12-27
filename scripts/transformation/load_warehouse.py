import psycopg2
from datetime import date, timedelta
import pandas as pd

# --------------------------------
# DATABASE CONNECTION
# --------------------------------
def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="ecommerce_db",
        user="postgres",
        password="postgres"
    )

# --------------------------------
# BUILD DATE DIMENSION
# --------------------------------
def build_dim_date(conn, start_date, end_date):
    cur = conn.cursor()
    d = start_date
    while d <= end_date:
        date_key = int(d.strftime("%Y%m%d"))
        cur.execute("""
            INSERT INTO warehouse.dim_date
            (date_key, full_date, year, quarter, month, day,
             month_name, day_name, week_of_year, is_weekend)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (date_key) DO NOTHING
        """, (
            date_key,
            d,
            d.year,
            (d.month - 1)//3 + 1,
            d.month,
            d.day,
            d.strftime("%B"),
            d.strftime("%A"),
            d.isocalendar()[1],
            d.weekday() >= 5
        ))
        d += timedelta(days=1)
    conn.commit()

# --------------------------------
# LOAD PAYMENT METHODS
# --------------------------------
def load_payment_methods(conn):
    cur = conn.cursor()
    methods = {
        "Credit Card": "Online",
        "Debit Card": "Online",
        "UPI": "Online",
        "Net Banking": "Online",
        "Cash on Delivery": "Offline"
    }
    for m, t in methods.items():
        cur.execute("""
            INSERT INTO warehouse.dim_payment_method
            (payment_method_name, payment_type)
            VALUES (%s,%s)
            ON CONFLICT (payment_method_name) DO NOTHING
        """, (m, t))
    conn.commit()

# --------------------------------
# LOAD SCD TYPE 2 CUSTOMERS
# --------------------------------
def load_dim_customers(conn):
    df = pd.read_sql("SELECT * FROM production.customers", conn)
    cur = conn.cursor()

    for _, r in df.iterrows():
        cur.execute("""
            INSERT INTO warehouse.dim_customers
            (customer_id, full_name, email, city, state, country,
             age_group, customer_segment, registration_date,
             effective_date, end_date, is_current)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_DATE,NULL,TRUE)
        """, (
            r.customer_id,
            f"{r.first_name} {r.last_name}",
            r.email,
            r.city,
            r.state,
            r.country,
            r.age_group,
            "Regular",
            r.registration_date
        ))
    conn.commit()

# --------------------------------
# LOAD SCD TYPE 2 PRODUCTS
# --------------------------------
def load_dim_products(conn):
    df = pd.read_sql("SELECT * FROM production.products", conn)
    cur = conn.cursor()

    for _, r in df.iterrows():
        if r.price < 50:
            price_range = "Budget"
        elif r.price < 200:
            price_range = "Mid-range"
        else:
            price_range = "Premium"

        cur.execute("""
            INSERT INTO warehouse.dim_products
            (product_id, product_name, category, sub_category,
             brand, price_range, effective_date, end_date, is_current)
            VALUES (%s,%s,%s,%s,%s,%s,CURRENT_DATE,NULL,TRUE)
        """, (
            r.product_id,
            r.product_name,
            r.category,
            r.sub_category,
            r.brand,
            price_range
        ))
    conn.commit()

# --------------------------------
# LOAD FACT SALES
# --------------------------------
def load_fact_sales(conn):
    query = """
    SELECT
        t.transaction_id,
        t.transaction_date,
        ti.quantity,
        ti.unit_price,
        ti.discount_percentage,
        ti.line_total,
        p.cost,
        c.customer_id,
        p.product_id,
        t.payment_method
    FROM production.transaction_items ti
    JOIN production.transactions t ON ti.transaction_id = t.transaction_id
    JOIN production.products p ON ti.product_id = p.product_id
    JOIN production.customers c ON t.customer_id = c.customer_id
    """

    df = pd.read_sql(query, conn)
    cur = conn.cursor()

    for _, r in df.iterrows():
        date_key = int(r.transaction_date.strftime("%Y%m%d"))

        cur.execute("SELECT customer_key FROM warehouse.dim_customers WHERE customer_id=%s AND is_current=TRUE",
                    (r.customer_id,))
        customer_key = cur.fetchone()[0]

        cur.execute("SELECT product_key FROM warehouse.dim_products WHERE product_id=%s AND is_current=TRUE",
                    (r.product_id,))
        product_key = cur.fetchone()[0]

        cur.execute("SELECT payment_method_key FROM warehouse.dim_payment_method WHERE payment_method_name=%s",
                    (r.payment_method,))
        payment_key = cur.fetchone()[0]

        discount_amt = (r.unit_price * r.quantity) * (r.discount_percentage / 100)
        profit = r.line_total - (r.cost * r.quantity)

        cur.execute("""
            INSERT INTO warehouse.fact_sales
            (date_key, customer_key, product_key, payment_method_key,
             transaction_id, quantity, unit_price,
             discount_amount, line_total, profit)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            date_key,
            customer_key,
            product_key,
            payment_key,
            r.transaction_id,
            r.quantity,
            r.unit_price,
            discount_amt,
            r.line_total,
            profit
        ))

    conn.commit()

# --------------------------------
# BUILD AGGREGATES
# --------------------------------
def build_aggregates(conn):
    cur = conn.cursor()

    cur.execute("TRUNCATE warehouse.agg_daily_sales")
    cur.execute("""
        INSERT INTO warehouse.agg_daily_sales
        SELECT
            date_key,
            COUNT(DISTINCT transaction_id),
            SUM(line_total),
            SUM(profit),
            COUNT(DISTINCT customer_key)
        FROM warehouse.fact_sales
        GROUP BY date_key
    """)

    cur.execute("TRUNCATE warehouse.agg_product_performance")
    cur.execute("""
        INSERT INTO warehouse.agg_product_performance
        (product_key, total_quantity_sold, total_revenue, total_profit, avg_discount_amount)
        SELECT
            product_key,
            SUM(quantity),
            SUM(line_total),
            SUM(profit),
            AVG(discount_amount)
        FROM warehouse.fact_sales
        GROUP BY product_key
    """)

    cur.execute("TRUNCATE warehouse.agg_customer_metrics")
    cur.execute("""
        INSERT INTO warehouse.agg_customer_metrics
        SELECT
            customer_key,
            COUNT(DISTINCT transaction_id),
            SUM(line_total),
            AVG(line_total),
            MAX(date_key::TEXT)::DATE
        FROM warehouse.fact_sales
        GROUP BY customer_key
    """)

    conn.commit()

# --------------------------------
# MAIN
# --------------------------------
if __name__ == "__main__":
    conn = get_connection()

    build_dim_date(conn, date(2023,1,1), date(2025,12,31))
    load_payment_methods(conn)
    load_dim_customers(conn)
    load_dim_products(conn)
    load_fact_sales(conn)
    build_aggregates(conn)

    conn.close()
    print("Warehouse loaded successfully")
