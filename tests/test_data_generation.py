import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")

FILES = {
    "customers.csv": ["customer_id", "email"],
    "products.csv": ["product_id", "price"],
    "transactions.csv": ["transaction_id", "customer_id"],
    "transaction_items.csv": ["transaction_id", "product_id", "quantity", "line_total"]
}

def test_csv_files_exist():
    for file in FILES:
        assert os.path.exists(os.path.join(RAW_DIR, file)), f"{file} missing"

def test_required_columns_exist():
    for file, cols in FILES.items():
        df = pd.read_csv(os.path.join(RAW_DIR, file))
        for col in cols:
            assert col in df.columns, f"{col} missing in {file}"

def test_no_null_ids():
    df = pd.read_csv(os.path.join(RAW_DIR, "customers.csv"))
    assert df["customer_id"].isnull().sum() == 0
