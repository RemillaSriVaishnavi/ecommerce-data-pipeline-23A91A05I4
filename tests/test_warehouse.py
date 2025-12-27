def test_dimension_tables_exist(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'warehouse'
    """)
    tables = {r[0] for r in cursor.fetchall()}
    expected = {"dim_customers", "dim_products", "fact_sales"}
    assert expected.issubset(tables)

def test_fact_grain(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM warehouse.fact_sales
    """)
    assert cursor.fetchone()[0] > 0
