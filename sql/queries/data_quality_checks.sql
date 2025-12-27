-- ================================
-- COMPLETENESS CHECKS
-- ================================

-- NULL checks
SELECT 'staging.customers.email' AS field, COUNT(*) AS violations
FROM staging.customers
WHERE email IS NULL OR email = '';

SELECT 'staging.products.price' AS field, COUNT(*) AS violations
FROM staging.products
WHERE price IS NULL;

-- Transactions without items
SELECT COUNT(*) AS violations
FROM staging.transactions t
LEFT JOIN staging.transaction_items ti
ON t.transaction_id = ti.transaction_id
WHERE ti.transaction_id IS NULL;


-- ================================
-- UNIQUENESS CHECKS
-- ================================

-- Duplicate customer IDs
SELECT customer_id, COUNT(*) 
FROM staging.customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Duplicate emails
SELECT email, COUNT(*) 
FROM staging.customers
GROUP BY email
HAVING COUNT(*) > 1;


-- ================================
-- VALIDITY / RANGE CHECKS
-- ================================

-- Invalid prices
SELECT COUNT(*) 
FROM staging.products
WHERE price <= 0;

-- Invalid discount
SELECT COUNT(*) 
FROM staging.transaction_items
WHERE discount_percentage < 0 OR discount_percentage > 100;

-- Invalid quantity
SELECT COUNT(*) 
FROM staging.transaction_items
WHERE quantity <= 0;


-- ================================
-- CONSISTENCY CHECKS
-- ================================

-- line_total mismatch
SELECT COUNT(*) 
FROM staging.transaction_items
WHERE ABS(line_total - (quantity * unit_price * (1 - discount_percentage/100.0))) > 0.01;

-- transaction total mismatch
SELECT COUNT(*) 
FROM staging.transactions t
JOIN (
  SELECT transaction_id, SUM(line_total) AS sum_items
  FROM staging.transaction_items
  GROUP BY transaction_id
) ti ON t.transaction_id = ti.transaction_id
WHERE ABS(t.total_amount - ti.sum_items) > 0.01;

-- product cost vs price
SELECT COUNT(*) 
FROM staging.products
WHERE cost >= price;


-- ================================
-- REFERENTIAL INTEGRITY
-- ================================

-- Orphan transactions
SELECT COUNT(*) 
FROM staging.transactions t
LEFT JOIN staging.customers c
ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Orphan transaction_items (transaction)
SELECT COUNT(*) 
FROM staging.transaction_items ti
LEFT JOIN staging.transactions t
ON ti.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

-- Orphan transaction_items (product)
SELECT COUNT(*) 
FROM staging.transaction_items ti
LEFT JOIN staging.products p
ON ti.product_id = p.product_id
WHERE p.product_id IS NULL;
