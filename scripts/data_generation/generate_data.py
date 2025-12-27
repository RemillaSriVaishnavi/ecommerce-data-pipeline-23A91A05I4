import json
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml
from faker import Faker

fake = Faker()


# --------------------------------------------------
# Utility: Load configuration
# --------------------------------------------------
def load_config():
    with open("config/config.yaml", "r") as f:
        return yaml.safe_load(f)


# --------------------------------------------------
# 1. Generate Customers
# --------------------------------------------------
def generate_customers(num_customers: int) -> pd.DataFrame:
    customers = []
    used_emails = set()

    age_groups = ["18-25", "26-35", "36-45", "46-60", "60+"]

    for i in range(1, num_customers + 1):
        email = fake.email()
        while email in used_emails:
            email = fake.email()
        used_emails.add(email)

        customers.append({
            "customer_id": f"CUST{i:04d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": email,
            "phone": fake.phone_number(),
            "registration_date": fake.date_between(start_date="-3y", end_date="today"),
            "city": fake.city(),
            "state": fake.state(),
            "country": "India",
            "age_group": random.choice(age_groups)
        })

    return pd.DataFrame(customers)


# --------------------------------------------------
# 2. Generate Products
# --------------------------------------------------
def generate_products(num_products: int) -> pd.DataFrame:
    categories = {
        "Electronics": ["Mobiles", "Laptops", "Accessories"],
        "Clothing": ["Men", "Women", "Kids"],
        "Home & Kitchen": ["Furniture", "Appliances", "Decor"],
        "Books": ["Fiction", "Education", "Comics"],
        "Sports": ["Outdoor", "Indoor", "Fitness"],
        "Beauty": ["Skincare", "Makeup", "Haircare"]
    }

    products = []

    for i in range(1, num_products + 1):
        category = random.choice(list(categories.keys()))
        sub_category = random.choice(categories[category])

        price = round(random.uniform(200, 5000), 2)
        cost = round(price * random.uniform(0.5, 0.8), 2)  # cost < price

        products.append({
            "product_id": f"PROD{i:04d}",
            "product_name": fake.word().capitalize(),
            "category": category,
            "sub_category": sub_category,
            "price": price,
            "cost": cost,
            "brand": fake.company(),
            "stock_quantity": random.randint(10, 500),
            "supplier_id": f"SUP{random.randint(1, 100):03d}"
        })

    return pd.DataFrame(products)


# --------------------------------------------------
# 3. Generate Transactions
# --------------------------------------------------
def generate_transactions(num_transactions: int, customers_df: pd.DataFrame) -> pd.DataFrame:
    payment_methods = [
        "Credit Card", "Debit Card", "UPI",
        "Cash on Delivery", "Net Banking"
    ]

    customer_ids = customers_df["customer_id"].tolist()
    transactions = []

    base_date = datetime.strptime("2023-01-01", "%Y-%m-%d")

    for i in range(1, num_transactions + 1):
        txn_date = base_date + timedelta(days=random.randint(0, 364))
        txn_time = fake.time()

        transactions.append({
            "transaction_id": f"TXN{i:06d}",
            "customer_id": random.choice(customer_ids),
            "transaction_date": txn_date.date(),
            "transaction_time": txn_time,
            "payment_method": random.choice(payment_methods),
            "shipping_address": fake.address().replace("\n", ", "),
            "total_amount": 0.0  # calculated later
        })

    return pd.DataFrame(transactions)


# --------------------------------------------------
# 4. Generate Transaction Items
# --------------------------------------------------
def generate_transaction_items(
    transactions_df: pd.DataFrame,
    products_df: pd.DataFrame
) -> pd.DataFrame:

    items = []
    item_id_counter = 1

    product_lookup = products_df.set_index("product_id")["price"].to_dict()

    transaction_totals = {}

    for _, txn in transactions_df.iterrows():
        num_items = random.randint(1, 5)
        chosen_products = random.sample(
            list(product_lookup.keys()), num_items
        )

        txn_total = 0.0

        for prod_id in chosen_products:
            quantity = random.randint(1, 4)
            unit_price = round(product_lookup[prod_id], 2)
            discount = random.choice([0, 5, 10, 15])

            line_total = round(
                quantity * unit_price * (1 - discount / 100), 2
            )

            txn_total += line_total

            items.append({
                "item_id": f"ITEM{item_id_counter:06d}",
                "transaction_id": txn["transaction_id"],
                "product_id": prod_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_percentage": discount,
                "line_total": line_total
            })

            item_id_counter += 1

        transaction_totals[txn["transaction_id"]] = round(txn_total, 2)

    # Update transaction totals
    transactions_df["total_amount"] = transactions_df["transaction_id"].map(
        transaction_totals
    )

    return pd.DataFrame(items)


# --------------------------------------------------
# 5. Referential Integrity Validation
# --------------------------------------------------
def validate_referential_integrity(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    transactions: pd.DataFrame,
    items: pd.DataFrame
) -> dict:

    orphan_customers = ~transactions["customer_id"].isin(customers["customer_id"])
    orphan_products = ~items["product_id"].isin(products["product_id"])
    orphan_transactions = ~items["transaction_id"].isin(transactions["transaction_id"])

    violations = (
        orphan_customers.sum()
        + orphan_products.sum()
        + orphan_transactions.sum()
    )

    score = 100 if violations == 0 else max(0, 100 - violations)

    return {
        "orphan_customer_ids": int(orphan_customers.sum()),
        "orphan_product_ids": int(orphan_products.sum()),
        "orphan_transaction_ids": int(orphan_transactions.sum()),
        "constraint_violations": int(violations),
        "data_quality_score": score
    }


# --------------------------------------------------
# MAIN EXECUTION (Manual run only)
# --------------------------------------------------
if __name__ == "__main__":
    config = load_config()

    raw_path = Path("data/raw")
    raw_path.mkdir(parents=True, exist_ok=True)

    customers_df = generate_customers(config["data_generation"]["customers"])
    products_df = generate_products(config["data_generation"]["products"])
    transactions_df = generate_transactions(
        config["data_generation"]["transactions"], customers_df
    )
    items_df = generate_transaction_items(transactions_df, products_df)

    # Save CSVs
    customers_df.to_csv(raw_path / "customers.csv", index=False)
    products_df.to_csv(raw_path / "products.csv", index=False)
    transactions_df.to_csv(raw_path / "transactions.csv", index=False)
    items_df.to_csv(raw_path / "transaction_items.csv", index=False)

    # Metadata
    metadata = {
        "generated_at": datetime.utcnow().isoformat(),
        "record_counts": {
            "customers": len(customers_df),
            "products": len(products_df),
            "transactions": len(transactions_df),
            "transaction_items": len(items_df)
        },
        "date_range": {
            "start": str(transactions_df["transaction_date"].min()),
            "end": str(transactions_df["transaction_date"].max())
        },
        "quality": validate_referential_integrity(
            customers_df, products_df, transactions_df, items_df
        )
    }

    with open(raw_path / "generation_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print("Data generation completed successfully.")
