-- ==========================================
-- Query 1: Data Freshness Check
-- ==========================================
SELECT
    'staging' AS layer,
    MAX(loaded_at) AS latest_timestamp
FROM staging.transactions

UNION ALL

SELECT
    'production' AS layer,
    MAX(created_at) AS latest_timestamp
FROM production.transactions

UNION ALL

SELECT
    'warehouse' AS layer,
    MAX(created_at) AS latest_timestamp
FROM warehouse.fact_sales;


-- ==========================================
-- Query 2: Volume Trend (Last 30 Days)
-- ==========================================
SELECT
    d.full_date AS date,
    COUNT(*) AS transaction_count
FROM warehouse.fact_sales f
JOIN warehouse.dim_date d
    ON f.date_key = d.date_key
WHERE d.full_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY d.full_date
ORDER BY d.full_date;


-- ==========================================
-- Query 3: Data Quality Issues
-- ==========================================
SELECT
    (SELECT COUNT(*) FROM warehouse.fact_sales WHERE product_key IS NULL) AS orphan_products,
    (SELECT COUNT(*) FROM warehouse.fact_sales WHERE customer_key IS NULL) AS orphan_customers,
    (SELECT COUNT(*) FROM warehouse.fact_sales WHERE line_total IS NULL) AS null_line_total;


-- ==========================================
-- Query 4: Database Statistics
-- ==========================================
SELECT
    COUNT(*) AS active_connections
FROM pg_stat_activity;


-- ==========================================
-- Query 5: Table Row Counts
-- ==========================================
SELECT
    'fact_sales' AS table_name,
    COUNT(*) AS row_count
FROM warehouse.fact_sales;
