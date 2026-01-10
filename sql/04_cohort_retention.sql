-- 04_cohort_retention.sql
-- Business Question:
-- How well are customers retained over time based on their acquisition (first purchase) month?

-- Output is designed for a cohort retention heatmap in Looker Studio:
-- Rows = cohort_month, Columns = months_since_cohort, Values = retention_pct

-- Dataset: bigquery-public-data.thelook_ecommerce
-- Table: orders
-- Techniques: cohorting logic, time-based aggregation, CTEs, SAFE_DIVIDE

WITH orders_clean AS (
  SELECT
    user_id,
    DATE(created_at) AS order_date
  FROM `bigquery-public-data.thelook_ecommerce.orders`
  WHERE created_at IS NOT NULL
),

first_purchase AS (
  SELECT
    user_id,
    MIN(order_date) AS first_order_date,
    DATE_TRUNC(MIN(order_date), MONTH) AS cohort_month
  FROM orders_clean
  GROUP BY 1
),

user_month_activity AS (
  -- One row per user per active month (deduped)
  SELECT
    o.user_id,
    fp.cohort_month,
    DATE_TRUNC(o.order_date, MONTH) AS active_month
  FROM orders_clean o
  JOIN first_purchase fp
    ON fp.user_id = o.user_id
  GROUP BY 1,2,3
),

cohort_activity AS (
  SELECT
    cohort_month,
    DATE_DIFF(active_month, cohort_month, MONTH) AS months_since_cohort,
    COUNT(DISTINCT user_id) AS active_users
  FROM user_month_activity
  GROUP BY 1,2
),

cohort_sizes AS (
  SELECT
    cohort_month,
    MAX(IF(months_since_cohort = 0, active_users, NULL)) AS cohort_size
  FROM cohort_activity
  GROUP BY 1
),

final AS (
  SELECT
    ca.cohort_month,
    ca.months_since_cohort,
    ca.active_users,
    cs.cohort_size,
    SAFE_DIVIDE(ca.active_users, cs.cohort_size) * 100 AS retention_pct
  FROM cohort_activity ca
  JOIN cohort_sizes cs
    USING (cohort_month)
)

SELECT *
FROM final
WHERE cohort_month >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 12 MONTH)
  AND months_since_cohort BETWEEN 0 AND 11
ORDER BY cohort_month, months_since_cohort;
