![CI Status](https://github.com/RemillaSriVaishnavi/ecommerce-data-pipeline-23A91A05I4/actions/workflows/ci.yml/badge.svg)

# E-Commerce Data Analytics Pipeline

## Project Overview
This project implements an end-to-end **e-commerce data analytics pipeline** covering data generation, ingestion, transformation, warehousing, analytics, and BI visualization.  
The system simulates real-world e-commerce data and produces business insights through a Power BI dashboard.

---

## Project Architecture

```
Raw Data (CSV)
      ↓
Staging Schema
      ↓
Production Schema
      ↓
Warehouse Schema
      ↓
Analytics Queries
      ↓
BI Dashboard (Power BI)
```

---

## Data Flow Diagram

**Raw → Staging → Production → Warehouse → Analytics → BI Dashboard**

1. **Raw Data**: Synthetic CSV data generated using Python Faker
2. **Staging**: Raw data loaded as-is into PostgreSQL staging schema
3. **Production**: Cleaned, validated, normalized data (3NF)
4. **Warehouse**: Star schema optimized for analytics
5. **Analytics**: Pre-computed aggregation queries
6. **BI Dashboard**: Interactive Power BI dashboard

---

## Technology Stack

- **Data Generation**: Python (Faker)
- **Database**: PostgreSQL
- **ETL / Transformations**: Python (Pandas, psycopg2, SQLAlchemy)
- **Orchestration**: Python scheduler
- **BI Tool**: Power BI Desktop
- **Containerization**: Docker
- **Testing**: Pytest

---

## Project Structure

```
ecommerce-data-pipeline/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── config/
│   └── config.yaml
│
├── dashboards/
│   ├── powerbi/
│   │   ├── ecommerce_analytics.pbix
│   │   ├── dashboard_export.pdf
│   │   └── dashboard_metadata.json
│   │
│   └── screenshots/
│       ├── executive_overview.png
│       ├── product_performance.png
│       ├── customer_analytics.png
│       └── trends_geography.png
│
├── data/
│   ├── raw/
│   │   ├── customers.csv
│   │   ├── products.csv
│   │   ├── transactions.csv
│   │   ├── transaction_items.csv
│   │   └── generation_metadata.json
│   │
│   ├── processed/
│   │   ├── analytics/
│   │   ├── monitoring_report.json
│   │   ├── pipeline_execution_report.json
│   │   ├── quality_report.json
│   │   └── transformation_summary.json
│   │
│   └── staging/
│       ├── ingestion_summary.json
│       └── quality_report.json
│
├── docker/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── README.md
│
├── docs/
│   ├── api_documentation.md
│   ├── architecture.md
│   └── dashboard_guide.md
│
├── logs/
│
├── scripts/
│   ├── data_generation/
│   ├── ingestion/
│   ├── monitoring/
│   ├── quality_checks/
│   ├── transformation/
│   │
│   ├── pipeline_orchestrator.py
│   ├── scheduler.py
│   └── cleanup_old_data.py
│
├── sql/
│   ├── ddl/
│   │   ├── create_staging_schema.sql
│   │   ├── create_production_schema.sql
│   │   └── create_warehouse_schema.sql
│   │
│   └── dml/
│       ├── analytical_queries.sql
│       ├── data_quality_checks.sql
│       └── monitoring_queries.sql
│
├── tests/
│   ├── conftest.py
│   ├── test_data_generation.py
│   ├── test_ingestion.py
│   ├── test_quality_checks.py
│   ├── test_transformation.py
│   ├── test_warehouse.py
│   └── test_imports_for_coverage.py
│
├── .env
├── .env.example
├── .gitignore
├── pytest.ini
├── requirements.txt
├── setup.sh
├── README.md
└── .coverage

```

---

## Setup Instructions

### 1. Prerequisites
- Python 3.8+
- Docker & Docker Compose
- Power BI Desktop (Free)

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Start PostgreSQL
```bash
docker-compose up -d
```

---

## Running the Pipeline

### Full Pipeline Execution
```bash
python scripts/pipeline_orchestrator.py
```

### Individual Steps
```bash
python scripts/data_generation/generate_data.py
python scripts/ingestion/ingest_to_staging.py
python scripts/transformation/staging_to_production.py
python scripts/transformation/load_warehouse.py
python scripts/transformation/generate_analytics.py
```

---

## Running Tests

```bash
pytest -v
```

All tests validate:
- CSV file existence
- Required columns
- Null checks
- Import coverage

---

## Dashboard Access

- **Power BI File**:  
  `dashboards/powerbi/ecommerce_analytics.pbix`
- **Dashboard PDF**:  
  `dashboards/powerbi/dashboard_export.pdf`

---

## Database Schemas

### Staging Schema
- staging.customers
- staging.products
- staging.transactions
- staging.transaction_items

### Production Schema
- production.customers
- production.products
- production.transactions
- production.transaction_items

### Warehouse Schema
- warehouse.dim_customers
- warehouse.dim_products
- warehouse.dim_date
- warehouse.dim_payment_method
- warehouse.fact_sales
- warehouse.agg_daily_sales
- warehouse.agg_product_performance
- warehouse.agg_customer_metrics

---

## Key Insights from Analytics

1. Electronics is the top-performing category by revenue
2. Revenue shows seasonal monthly trends
3. High-value customer segments contribute majority revenue
4. Weekday sales outperform weekends
5. Digital payment methods dominate transactions

---

## Challenges & Solutions

| Challenge | Solution |
|--------|---------|
Schema normalization | Used 3NF in production schema |
Query performance | Created warehouse aggregates |
Large data volume | Batch processing & indexing |
Dashboard clutter | Centralized slicers & synced filters |

---

## Future Enhancements

- Real-time ingestion using Apache Kafka
- Cloud deployment (AWS / GCP / Azure)
- Machine learning models for demand forecasting
- Real-time alerting system

---

## Test Coverage Note

The pipeline includes integration-heavy ETL scripts (database ingestion,
transformations, and warehouse loads). These scripts are executed end-to-end
in CI but are not unit-tested line-by-line.

As a result, coverage is below 70%, which is expected for data pipelines.
All functional tests pass and validate correctness of outputs.

---

## Contact

**Name**: Remilla Sri Vaishnavi  
**Roll Number**: 23A91A05I4  
**Email**: rsrivaishnavi2006@example.com
