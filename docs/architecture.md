# E-Commerce Data Pipeline Architecture

## Overview
This document describes the technical architecture and design decisions of the e-commerce data analytics platform.

---

## System Components

### 1. Data Generation Layer
- Generates synthetic e-commerce data using Python Faker
- Outputs CSV files:
  - customers
  - products
  - transactions
  - transaction_items

---

### 2. Data Ingestion Layer
- Loads CSV data into PostgreSQL staging schema
- Technology: Python + psycopg2
- Pattern: Batch ingestion

---

### 3. Data Storage Layer

#### Staging Schema
- Raw replica of CSV structure
- Minimal validation
- Temporary storage

#### Production Schema
- Cleaned and normalized (3NF)
- Enforces constraints
- Business rules applied

#### Warehouse Schema
- Star schema optimized for analytics
- Supports SCD Type 2

---

### 4. Data Processing Layer
- Data quality checks
- Data cleansing and enrichment
- SCD Type 2 implementation
- Aggregation tables

---

### 5. Data Serving Layer
- Analytical SQL queries
- Precomputed aggregates
- BI tool connectivity

---

### 6. Visualization Layer
- Power BI / Tableau Public
- 16+ visuals across 4 pages

---

### 7. Orchestration Layer
- Pipeline orchestrator
- Scheduler for daily runs
- Logging and retry mechanism

---

## Data Models

### Staging Model
- Exact CSV replica
- No transformations

### Production Model
- 3NF normalized
- FK constraints
- Data integrity enforced

### Warehouse Model (Star Schema)
- 4 Dimension tables
- 1 Fact table
- 3 Aggregate tables

---

## Design Decisions

- PostgreSQL chosen for reliability and SQL support
- Star schema for fast analytics
- SCD Type 2 to track historical changes
- Batch processing for simplicity and consistency

---

## Deployment Architecture

- Local execution using Python
- Optional Docker deployment
- Future cloud readiness
