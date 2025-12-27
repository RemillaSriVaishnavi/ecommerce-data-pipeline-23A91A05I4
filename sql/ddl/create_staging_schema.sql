-- =========================================================
-- File: create_staging_schema.sql
-- Purpose: Create staging schema and raw landing tables
-- =========================================================

-- ---------------------------------------------------------
-- 1. Create schemas (MANDATORY)
-- ---------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS production;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- ---------------------------------------------------------
-- 2. Staging: Customers
-- Mirrors customers.csv exactly
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id        VARCHAR(20) PRIMARY KEY,
    first_name         VARCHAR(100),
    last_name          VARCHAR(100),
    email              VARCHAR(255),
    phone              VARCHAR(50),
    registration_date  DATE,
    city               VARCHAR(100),
    state              VARCHAR(100),
    country             VARCHAR(100),
    age_group          VARCHAR(20),
    loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------
-- 3. Staging: Products
-- Mirrors products.csv exactly
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.products (
    product_id        VARCHAR(20) PRIMARY KEY,
    product_name      VARCHAR(255),
    category          VARCHAR(100),
    sub_category      VARCHAR(100),
    price             DECIMAL(10,2),
    cost              DECIMAL(10,2),
    brand             VARCHAR(100),
    stock_quantity    INTEGER,
    supplier_id       VARCHAR(20),
    loaded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------
-- 4. Staging: Transactions
-- Mirrors transactions.csv exactly
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.transactions (
    transaction_id     VARCHAR(20) PRIMARY KEY,
    customer_id        VARCHAR(20),
    transaction_date   DATE,
    transaction_time   TIME,
    payment_method     VARCHAR(50),
    shipping_address   VARCHAR(255),
    total_amount       DECIMAL(12,2),
    loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------
-- 5. Staging: Transaction Items
-- Mirrors transaction_items.csv exactly
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.transaction_items (
    item_id              VARCHAR(20) PRIMARY KEY,
    transaction_id       VARCHAR(20),
    product_id           VARCHAR(20),
    quantity             INTEGER,
    unit_price           DECIMAL(10,2),
    discount_percentage  DECIMAL(5,2),
    line_total           DECIMAL(12,2),
    loaded_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
