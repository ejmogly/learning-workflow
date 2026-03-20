-- =========================================================
-- File: sql/cte_and_window_functions.sql
-- Topic: CTE and Window Functions for Data Analytics Engineering
-- Purpose:
--   1) Practice readable multi-step SQL using CTEs
--   2) Learn common window function patterns
--   3) Build reusable SQL logic for marts and validation
-- =========================================================


-- =========================================================
-- Example 1. Daily KPI aggregation with CTEs
-- Goal:
--   Build a simple daily metrics table from event-level data
-- =========================================================

WITH base_events AS (
    SELECT
        user_id,
        CAST(event_timestamp AS DATE) AS event_date,
        event_type,
        revenue
    FROM source_events
),
daily_metrics AS (
    SELECT
        event_date,
        COUNT(DISTINCT user_id) AS active_users,
        COUNT(*) AS total_events,
        SUM(COALESCE(revenue, 0)) AS total_revenue
    FROM base_events
    GROUP BY event_date
)
SELECT
    event_date,
    active_users,
    total_events,
    total_revenue
FROM daily_metrics
ORDER BY event_date;


-- =========================================================
-- Example 2. Find duplicate records using GROUP BY
-- Goal:
--   Detect duplicated user-event combinations for DQ checks
-- =========================================================

SELECT
    user_id,
    event_timestamp,
    event_type,
    COUNT(*) AS row_count
FROM source_events
GROUP BY
    user_id,
    event_timestamp,
    event_type
HAVING COUNT(*) > 1
ORDER BY row_count DESC;


-- =========================================================
-- Example 3. Deduplicate using ROW_NUMBER()
-- Goal:
--   Keep only the latest row per user_id
-- =========================================================

WITH ranked_rows AS (
    SELECT
        user_id,
        event_timestamp,
        event_type,
        revenue,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY event_timestamp DESC
        ) AS rn
    FROM source_events
)
SELECT
    user_id,
    event_timestamp,
    event_type,
    revenue
FROM ranked_rows
WHERE rn = 1
ORDER BY user_id;


-- =========================================================
-- Example 4. Rank top revenue users by date
-- Goal:
--   Find high-value users per day
-- =========================================================

WITH user_daily_revenue AS (
    SELECT
        CAST(event_timestamp AS DATE) AS event_date,
        user_id,
        SUM(COALESCE(revenue, 0)) AS daily_revenue
    FROM source_events
    GROUP BY
        CAST(event_timestamp AS DATE),
        user_id
),
ranked_users AS (
    SELECT
        event_date,
        user_id,
        daily_revenue,
        RANK() OVER (
            PARTITION BY event_date
            ORDER BY daily_revenue DESC
        ) AS revenue_rank
    FROM user_daily_revenue
)
SELECT
    event_date,
    user_id,
    daily_revenue,
    revenue_rank
FROM ranked_users
WHERE revenue_rank <= 3
ORDER BY event_date, revenue_rank, user_id;


-- =========================================================
-- Example 5. Running total using SUM() OVER()
-- Goal:
--   Build cumulative revenue trend over time
-- =========================================================

WITH daily_revenue AS (
    SELECT
        CAST(event_timestamp AS DATE) AS event_date,
        SUM(COALESCE(revenue, 0)) AS total_revenue
    FROM source_events
    GROUP BY CAST(event_timestamp AS DATE)
)
SELECT
    event_date,
    total_revenue,
    SUM(total_revenue) OVER (
        ORDER BY event_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_revenue
FROM daily_revenue
ORDER BY event_date;


-- =========================================================
-- Example 6. Rolling 7-day average
-- Goal:
--   Smooth daily volatility in KPI reporting
-- =========================================================

WITH daily_metrics AS (
    SELECT
        CAST(event_timestamp AS DATE) AS event_date,
        COUNT(DISTINCT user_id) AS active_users
    FROM source_events
    GROUP BY CAST(event_timestamp AS DATE)
)
SELECT
    event_date,
    active_users,
    AVG(active_users) OVER (
        ORDER BY event_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_avg_active_users
FROM daily_metrics
ORDER BY event_date;


-- =========================================================
-- Example 7. Previous-day comparison using LAG()
-- Goal:
--   Compare current value with previous period
-- =========================================================

WITH daily_revenue AS (
    SELECT
        CAST(event_timestamp AS DATE) AS event_date,
        SUM(COALESCE(revenue, 0)) AS total_revenue
    FROM source_events
    GROUP BY CAST(event_timestamp AS DATE)
)
SELECT
    event_date,
    total_revenue,
    LAG(total_revenue) OVER (
        ORDER BY event_date
    ) AS previous_day_revenue,
    total_revenue
        - LAG(total_revenue) OVER (ORDER BY event_date) AS revenue_diff
FROM daily_revenue
ORDER BY event_date;


-- =========================================================
-- Example 8. First event date per user using MIN() OVER()
-- Goal:
--   Mark first-seen date while keeping row-level detail
-- =========================================================

SELECT
    user_id,
    event_timestamp,
    event_type,
    MIN(CAST(event_timestamp AS DATE)) OVER (
        PARTITION BY user_id
    ) AS first_seen_date
FROM source_events
ORDER BY user_id, event_timestamp;


-- =========================================================
-- Example 9. Session/order sequencing using ROW_NUMBER()
-- Goal:
--   Identify nth activity/order per customer
-- =========================================================

SELECT
    customer_id,
    order_id,
    order_timestamp,
    ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY order_timestamp
    ) AS order_sequence
FROM raw_orders
ORDER BY customer_id, order_sequence;


-- =========================================================
-- Example 10. Latest status per order
-- Goal:
--   Keep the most recent status update from status history table
-- =========================================================

WITH latest_status AS (
    SELECT
        order_id,
        status,
        status_updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY status_updated_at DESC
        ) AS rn
    FROM order_status_history
)
SELECT
    order_id,
    status,
    status_updated_at
FROM latest_status
WHERE rn = 1
ORDER BY order_id;


-- =========================================================
-- Example 11. Null validation check
-- Goal:
--   Basic DQ validation for required fields
-- =========================================================

SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS null_user_id_cnt,
    SUM(CASE WHEN event_timestamp IS NULL THEN 1 ELSE 0 END) AS null_event_timestamp_cnt,
    SUM(CASE WHEN event_type IS NULL THEN 1 ELSE 0 END) AS null_event_type_cnt
FROM source_events;


-- =========================================================
-- Example 12. Reconciliation check between source and mart
-- Goal:
--   Compare source counts vs transformed counts
-- =========================================================

WITH source_count AS (
    SELECT COUNT(*) AS cnt
    FROM source_events
),
mart_count AS (
    SELECT COUNT(*) AS cnt
    FROM mart_user_events
)
SELECT
    s.cnt AS source_cnt,
    m.cnt AS mart_cnt,
    s.cnt - m.cnt AS count_diff
FROM source_count s
CROSS JOIN mart_count m;
