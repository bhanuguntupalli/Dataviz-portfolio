-- 05_customer_ltv.sql
-- Business Question:
-- What is the lifetime value (LTV) and purchasing behavior of customers?

-- Dataset: bigquery-public-data.thelook_ecommerce
-- Tables: orders, order_items
-- Techniques: CTEs, joins, aggregations, window functions (DENSE_RANK), date logic

WITH joined AS (
  SELECT
    o.user_id,
    o.order_id,
    DATE(o.created_at) AS order_date,
    oi.sale_price
  FROM `bigquery-public-data.thelook_ecommerce.orders` o
  JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi
    ON oi.order_id = o.order_id
  WHERE o.created_at IS NOT NULL
    AND oi.created_at IS NOT NULL
),

customer_agg AS (
  SELECT
    user_id,
    SUM(sale_price) AS ltv_revenue,
    COUNT(DISTINCT order_id) AS total_orders,
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date
  FROM joined
  GROUP BY 1
),

enriched AS (
  SELECT
    user_id,
    ltv_revenue,
    total_orders,
    SAFE_DIVIDE(ltv_revenue, total_orders) AS avg_order_value,
    first_order_date,
    last_order_date,

    DATE_DIFF(last_order_date, first_order_date, DAY) AS customer_lifetime_days,

    -- Active months (at least 1 to avoid divide-by-zero)
    GREATEST(DATE_DIFF(DATE_TRUNC(last_order_date, MONTH), DATE_TRUNC(first_order_date, MONTH), MONTH) + 1, 1) AS active_months,

    DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) AS recency_days
  FROM customer_agg
),

final AS (
  SELECT
    *,
    SAFE_DIVIDE(total_orders, active_months) AS orders_per_active_month,
    DENSE_RANK() OVER (ORDER BY ltv_revenue DESC) AS ltv_rank
  FROM enriched
)

SELECT *
FROM final
ORDER BY ltv_revenue DESC;
