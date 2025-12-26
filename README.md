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
├── data/
│   ├── raw/
│   ├── processed/
├── dashboards/
│   └── powerbi/
│       ├── ecommerce_analytics.pbix
│       └── dashboard_export.pdf
├── docker/
├── scripts/
│   ├── data_generation/
│   ├── ingestion/
│   ├── transformation/
│   └── pipeline_orchestrator.py
├── tests/
├── docs/
│   ├── architecture.md
│   └── dashboard_guide.md
├── requirements.txt
├── pytest.ini
└── README.md
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

## Contact

**Name**: Remilla Sri Vaishnavi  
**Roll Number**: 23A91A05I4  
**Email**: rsrivaishnavi2006@example.com
