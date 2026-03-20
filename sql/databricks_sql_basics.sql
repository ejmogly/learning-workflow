-- =========================================================
-- File: sql/databricks_sql_basics.sql
-- Topic: Databricks SQL Basics for Analytics Engineering
-- Purpose:
--   1) Practice basic Databricks SQL workflow
--   2) Understand schema/table handling
--   3) Build transformation + validation examples
-- Notes:
--   Replace catalog/schema/table names with your own environment
-- =========================================================


-- =========================================================
-- Example 1. Check current catalogs / schemas / tables
-- Goal:
--   Explore the workspace before querying data
-- =========================================================

SHOW CATALOGS;
SHOW SCHEMAS;
SHOW TABLES;


-- =========================================================
-- Example 2. Select a catalog and schema
-- Goal:
--   Set working context
-- =========================================================

USE CATALOG main;
USE SCHEMA default;


-- =========================================================
-- Example 3. Preview raw data
-- Goal:
--   Inspect source table structure and sample rows
-- =========================================================

SELECT *
FROM raw_orders
LIMIT 100;


-- =========================================================
-- Example 4. Basic filtering and projection
-- Goal:
--   Pull only needed columns and rows
-- =========================================================

SELECT
    order_id,
    customer_id,
    order_date,
    order_status,
    order_amount
FROM raw_orders
WHERE order_date >= DATE '2026-01-01'
  AND order_status IN ('completed', 'paid')
ORDER BY order_date DESC
LIMIT 100;


-- =========================================================
-- Example 5. Basic aggregation
-- Goal:
--   Build simple daily order metrics
-- =========================================================

SELECT
    order_date,
    COUNT(*) AS order_cnt,
    COUNT(DISTINCT customer_id) AS customer_cnt,
    SUM(order_amount) AS gross_order_amount
FROM raw_orders
GROUP BY order_date
ORDER BY order_date;


-- =========================================================
-- Example 6. Create or replace a clean mart-style table
-- Goal:
--   Store transformed results as a reusable table
-- =========================================================

CREATE OR REPLACE TABLE mart_daily_orders AS
SELECT
    order_date,
    COUNT(*) AS order_cnt,
    COUNT(DISTINCT customer_id) AS customer_cnt,
    SUM(order_amount) AS gross_order_amount,
    AVG(order_amount) AS avg_order_amount
FROM raw_orders
WHERE order_status IN ('completed', 'paid')
GROUP BY order_date;


-- =========================================================
-- Example 7. Query the mart table
-- Goal:
--   Reuse transformed output downstream
-- =========================================================

SELECT
    order_date,
    order_cnt,
    customer_cnt,
    gross_order_amount,
    avg_order_amount
FROM mart_daily_orders
ORDER BY order_date;


-- =========================================================
-- Example 8. Validation check: duplicates in source
-- Goal:
--   Check for duplicate order_id values
-- =========================================================

SELECT
    order_id,
    COUNT(*) AS row_count
FROM raw_orders
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY row_count DESC, order_id;


-- =========================================================
-- Example 9. Validation check: null values
-- Goal:
--   Validate required columns before mart creation
-- =========================================================

SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id_cnt,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id_cnt,
    SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS null_order_date_cnt,
    SUM(CASE WHEN order_amount IS NULL THEN 1 ELSE 0 END) AS null_order_amount_cnt
FROM raw_orders;


-- =========================================================
-- Example 10. Reconciliation between source and mart
-- Goal:
--   Compare daily totals for sanity check
-- =========================================================

WITH source_daily AS (
    SELECT
        order_date,
        COUNT(*) AS source_order_cnt,
        SUM(order_amount) AS source_gross_amount
    FROM raw_orders
    WHERE order_status IN ('completed', 'paid')
    GROUP BY order_date
),
mart_daily AS (
    SELECT
        order_date,
        order_cnt AS mart_order_cnt,
        gross_order_amount AS mart_gross_amount
    FROM mart_daily_orders
)
SELECT
    s.order_date,
    s.source_order_cnt,
    m.mart_order_cnt,
    s.source_gross_amount,
    m.mart_gross_amount,
    s.source_order_cnt - m.mart_order_cnt AS order_cnt_diff,
    s.source_gross_amount - m.mart_gross_amount AS amount_diff
FROM source_daily s
LEFT JOIN mart_daily m
    ON s.order_date = m.order_date
ORDER BY s.order_date;


-- =========================================================
-- Example 11. Customer-level mart logic
-- Goal:
--   Build reusable customer daily activity table
-- =========================================================

CREATE OR REPLACE TABLE mart_customer_daily_activity AS
SELECT
    order_date,
    customer_id,
    COUNT(*) AS daily_order_cnt,
    SUM(order_amount) AS daily_order_amount,
    MAX(order_amount) AS max_single_order_amount
FROM raw_orders
WHERE order_status IN ('completed', 'paid')
GROUP BY
    order_date,
    customer_id;


-- =========================================================
-- Example 12. Use window functions inside Databricks SQL
-- Goal:
--   Rank customers by daily order amount
-- =========================================================

WITH ranked_customers AS (
    SELECT
        order_date,
        customer_id,
        daily_order_amount,
        RANK() OVER (
            PARTITION BY order_date
            ORDER BY daily_order_amount DESC
        ) AS amount_rank
    FROM mart_customer_daily_activity
)
SELECT
    order_date,
    customer_id,
    daily_order_amount,
    amount_rank
FROM ranked_customers
WHERE amount_rank <= 10
ORDER BY order_date, amount_rank, customer_id;


-- =========================================================
-- Example 13. Simple date-based incremental logic
-- Goal:
--   Simulate loading only recent data
-- =========================================================

SELECT
    order_id,
    customer_id,
    order_date,
    order_amount
FROM raw_orders
WHERE order_date >= date_sub(current_date(), 7)
ORDER BY order_date DESC;


-- =========================================================
-- Example 14. Table metadata inspection
-- Goal:
--   Understand table definition and columns
-- =========================================================

DESCRIBE TABLE raw_orders;
DESCRIBE TABLE mart_daily_orders;


-- =========================================================
-- Example 15. Comment for documentation mindset
-- Goal:
--   Keep transformation logic understandable and reusable
-- =========================================================

-- Recommended practice in Databricks SQL:
-- 1) Separate raw/source query, transformation query, and validation query
-- 2) Name mart tables clearly
-- 3) Store reusable logic in tables/views instead of rewriting ad hoc SQL
-- 4) Add documentation in README / summaries / notebook markdown
