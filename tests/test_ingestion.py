def test_staging_tables_exist(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'staging'
    """)
    tables = {r[0] for r in cursor.fetchall()}
    expected = {
        "customers", "products", "transactions", "transaction_items"
    }
    assert expected.issubset(tables)

def test_loaded_at_exists(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM staging.customers WHERE loaded_at IS NULL
    """)
    assert cursor.fetchone()[0] == 0
