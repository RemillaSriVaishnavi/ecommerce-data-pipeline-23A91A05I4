import json
import psycopg2
from datetime import datetime
from pathlib import Path


def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="ecommerce_db",
        user="postgres",
        password="postgres"
    )


def fetch_single_value(cursor, query):
    cursor.execute(query)
    return cursor.fetchone()[0]


def calculate_score(violations, total):
    if total == 0:
        return 100
    return max(0, round((1 - violations / total) * 100, 2))


def run_quality_checks():
    conn = get_connection()
    cur = conn.cursor()

    report = {
        "check_timestamp": datetime.utcnow().isoformat(),
        "checks_performed": {},
    }

    # -----------------------------
    # COMPLETENESS
    # -----------------------------
    null_email = fetch_single_value(cur,
        "SELECT COUNT(*) FROM staging.customers WHERE email IS NULL OR email = ''")
    missing_items = fetch_single_value(cur,
        """SELECT COUNT(*) FROM staging.transactions t
           LEFT JOIN staging.transaction_items ti
           ON t.transaction_id = ti.transaction_id
           WHERE ti.transaction_id IS NULL""")

    completeness_violations = null_email + missing_items

    report["checks_performed"]["null_checks"] = {
        "status": "passed" if completeness_violations == 0 else "failed",
        "tables_checked": ["customers", "transactions"],
        "null_violations": completeness_violations,
        "details": {
            "customers.email": null_email,
            "transactions_without_items": missing_items
        }
    }

    # -----------------------------
    # UNIQUENESS
    # -----------------------------
    dup_customers = fetch_single_value(cur,
        "SELECT COUNT(*) FROM (SELECT customer_id FROM staging.customers GROUP BY customer_id HAVING COUNT(*) > 1) x")
    dup_emails = fetch_single_value(cur,
        "SELECT COUNT(*) FROM (SELECT email FROM staging.customers GROUP BY email HAVING COUNT(*) > 1) x")

    uniqueness_violations = dup_customers + dup_emails

    report["checks_performed"]["duplicate_checks"] = {
        "status": "passed" if uniqueness_violations == 0 else "failed",
        "duplicates_found": uniqueness_violations,
        "details": {
            "duplicate_customer_ids": dup_customers,
            "duplicate_emails": dup_emails
        }
    }

    # -----------------------------
    # VALIDITY / RANGE
    # -----------------------------
    invalid_price = fetch_single_value(cur,
        "SELECT COUNT(*) FROM staging.products WHERE price <= 0")
    invalid_discount = fetch_single_value(cur,
        "SELECT COUNT(*) FROM staging.transaction_items WHERE discount_percentage < 0 OR discount_percentage > 100")
    invalid_qty = fetch_single_value(cur,
        "SELECT COUNT(*) FROM staging.transaction_items WHERE quantity <= 0")

    range_violations = invalid_price + invalid_discount + invalid_qty

    report["checks_performed"]["range_checks"] = {
        "status": "passed" if range_violations == 0 else "failed",
        "violations": range_violations,
        "details": {
            "invalid_price": invalid_price,
            "invalid_discount": invalid_discount,
            "invalid_quantity": invalid_qty
        }
    }

    # -----------------------------
    # CONSISTENCY
    # -----------------------------
    line_mismatch = fetch_single_value(cur,
        """SELECT COUNT(*) FROM staging.transaction_items
           WHERE ABS(line_total - (quantity * unit_price * (1 - discount_percentage/100.0))) > 0.01""")

    total_mismatch = fetch_single_value(cur,
        """SELECT COUNT(*) FROM staging.transactions t
           JOIN (SELECT transaction_id, SUM(line_total) s FROM staging.transaction_items GROUP BY transaction_id) ti
           ON t.transaction_id = ti.transaction_id
           WHERE ABS(t.total_amount - ti.s) > 0.01""")

    cost_price = fetch_single_value(cur,
        "SELECT COUNT(*) FROM staging.products WHERE cost >= price")

    consistency_violations = line_mismatch + total_mismatch + cost_price

    report["checks_performed"]["data_consistency"] = {
        "status": "passed" if consistency_violations == 0 else "failed",
        "mismatches": consistency_violations,
        "details": {
            "line_total_mismatch": line_mismatch,
            "transaction_total_mismatch": total_mismatch,
            "cost_greater_than_price": cost_price
        }
    }

    # -----------------------------
    # REFERENTIAL INTEGRITY
    # -----------------------------
    orphan_tx = fetch_single_value(cur,
        """SELECT COUNT(*) FROM staging.transactions t
           LEFT JOIN staging.customers c ON t.customer_id = c.customer_id
           WHERE c.customer_id IS NULL""")

    orphan_item_tx = fetch_single_value(cur,
        """SELECT COUNT(*) FROM staging.transaction_items ti
           LEFT JOIN staging.transactions t ON ti.transaction_id = t.transaction_id
           WHERE t.transaction_id IS NULL""")

    orphan_item_prod = fetch_single_value(cur,
        """SELECT COUNT(*) FROM staging.transaction_items ti
           LEFT JOIN staging.products p ON ti.product_id = p.product_id
           WHERE p.product_id IS NULL""")

    ref_violations = orphan_tx + orphan_item_tx + orphan_item_prod

    report["checks_performed"]["referential_integrity"] = {
        "status": "passed" if ref_violations == 0 else "failed",
        "orphan_records": ref_violations,
        "details": {
            "orphan_transactions": orphan_tx,
            "orphan_items_transaction": orphan_item_tx,
            "orphan_items_product": orphan_item_prod
        }
    }

    # -----------------------------
    # OVERALL SCORE
    # -----------------------------
    total_violations = (
        completeness_violations +
        uniqueness_violations +
        range_violations +
        consistency_violations +
        ref_violations
    )

    report["overall_quality_score"] = calculate_score(total_violations, 50000)

    report["quality_grade"] = (
        "A" if report["overall_quality_score"] >= 95 else
        "B" if report["overall_quality_score"] >= 85 else
        "C" if report["overall_quality_score"] >= 70 else
        "D"
    )

    conn.close()

    Path("data/staging").mkdir(parents=True, exist_ok=True)
    with open("data/staging/quality_report.json", "w") as f:
        json.dump(report, f, indent=2)

    print("Data Quality Checks Completed")


if __name__ == "__main__":
    run_quality_checks()
