-- SQL Foundations for Data Analytics Engineering
-- Focus: transformation, aggregation, validation, and readability

-- Example 1: Basic aggregation
WITH base AS (
    SELECT
        user_id,
        event_date,
        event_type,
        revenue
    FROM source_events
),
daily_metrics AS (
    SELECT
        event_date,
        COUNT(DISTINCT user_id) AS active_users,
        COUNT(*) AS total_events,
        SUM(revenue) AS total_revenue
    FROM base
    GROUP BY event_date
)
SELECT *
FROM daily_metrics
ORDER BY event_date;

-- Example 2: Duplicate check
SELECT
    user_id,
    event_date,
    COUNT(*) AS row_count
FROM source_events
GROUP BY user_id, event_date
HAVING COUNT(*) > 1;

-- Example 3: Null validation check
SELECT
    COUNT(*) AS null_user_id_count
FROM source_events
WHERE user_id IS NULL;
