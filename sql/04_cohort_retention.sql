CREATE OR REPLACE VIEW `dataviz-portfolio-483720.portfolio_thelook.v_cohort_retention` AS
WITH first_order AS (
  SELECT
    user_id,
    DATE_TRUNC(MIN(DATE(created_at)), MONTH) AS cohort_month
  FROM `bigquery-public-data.thelook_ecommerce.orders`
  GROUP BY 1
),
user_months AS (
  SELECT
    o.user_id,
    f.cohort_month,
    DATE_TRUNC(DATE(o.created_at), MONTH) AS order_month
  FROM `bigquery-public-data.thelook_ecommerce.orders` o
  JOIN first_order f
    ON f.user_id = o.user_id
  GROUP BY 1,2,3
),
cohort_indexed AS (
  SELECT
    cohort_month,
    order_month,
    DATE_DIFF(order_month, cohort_month, MONTH) AS months_since_cohort,
    COUNT(DISTINCT user_id) AS active_users
  FROM user_months
  GROUP BY 1,2,3
),
cohort_sizes AS (
  SELECT
    cohort_month,
    MAX(IF(months_since_cohort = 0, active_users, NULL)) AS cohort_size
  FROM cohort_indexed
  GROUP BY 1
)
SELECT
  c.cohort_month,
  c.months_since_cohort,
  c.active_users,
  s.cohort_size,
  SAFE_DIVIDE(c.active_users, s.cohort_size) * 100 AS retention_pct
FROM cohort_indexed c
JOIN cohort_sizes s
  USING (cohort_month)
WHERE c.cohort_month >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 12 MONTH)
  AND c.months_since_cohort BETWEEN 0 AND 11
ORDER BY cohort_month, months_since_cohort;
