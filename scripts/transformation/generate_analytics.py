import psycopg2
import pandas as pd
import json
import time
from datetime import datetime
from pathlib import Path

OUTPUT_DIR = Path("data/processed/analytics")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="ecommerce_db",
        user="postgres",
        password="postgres"
    )

def execute_query(conn, query_name, sql):
    start = time.time()
    df = pd.read_sql(sql, conn)
    elapsed_ms = round((time.time() - start) * 1000, 2)
    return df, elapsed_ms

def export_to_csv(df, filename):
    df.to_csv(OUTPUT_DIR / filename, index=False)

def generate_summary(results, total_time):
    return {
        "generation_timestamp": datetime.utcnow().isoformat(),
        "queries_executed": len(results),
        "query_results": results,
        "total_execution_time_seconds": round(total_time, 2)
    }

def main():
    conn = get_connection()

    with open("sql/queries/analytical_queries.sql") as f:
        sql_text = f.read()

    queries = [q.strip() for q in sql_text.split(";") if q.strip()]
    results = {}
    total_start = time.time()

    for i, query in enumerate(queries, start=1):
        query_name = f"query{i}"
        df, exec_time = execute_query(conn, query_name, query)
        export_to_csv(df, f"{query_name}.csv")

        results[query_name] = {
            "rows": len(df),
            "columns": len(df.columns),
            "execution_time_ms": exec_time
        }

    summary = generate_summary(results, time.time() - total_start)

    with open(OUTPUT_DIR / "analytics_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    conn.close()
    print("Analytics generation completed successfully")

if __name__ == "__main__":
    main()
