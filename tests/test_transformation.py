def test_production_tables_populated(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM production.customers")
    assert cursor.fetchone()[0] > 0

def test_no_orphan_transactions(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("""
        SELECT COUNT(*)
        FROM production.transactions t
        LEFT JOIN production.customers c
        ON t.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """)
    assert cursor.fetchone()[0] == 0

def test_email_lowercase(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM production.customers
        WHERE email <> LOWER(email)
    """)
    assert cursor.fetchone()[0] == 0
