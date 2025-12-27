import psycopg2
import pytest
import os

@pytest.fixture(scope="session")
def db_conn():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_NAME", "ecommerce_db"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres"),
            port=int(os.getenv("DB_PORT", 5432))
        )
        yield conn
        conn.close()
    except psycopg2.OperationalError as e:
        pytest.skip(f"Database not available for tests: {e}")
