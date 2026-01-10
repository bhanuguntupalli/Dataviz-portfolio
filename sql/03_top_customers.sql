-- 03_top_customers.sql
-- Business Question:
-- Who are the top revenue-generating customers, and how concentrated is revenue among them?

-- Dataset: bigquery-public-data.thelook_ecommerce
-- Tables: orders, order_items
-- Techniques: CTEs, joins, aggregations, window functions (DENSE_RANK, SUM OVER)

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
    AND DATE(o.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
),

customer_agg AS (
  SELECT
    user_id,
    SUM(sale_price) AS revenue,
    COUNT(DISTINCT order_id) AS orders,
    SAFE_DIVIDE(SUM(sale_price), COUNT(DISTINCT order_id)) AS aov,
    MAX(order_date) AS last_order_date
  FROM joined
  GROUP BY 1
),

with_totals AS (
  SELECT
    *,
    SUM(revenue) OVER () AS total_revenue
  FROM customer_agg
),

ranked AS (
  SELECT
    user_id,
    revenue,
    orders,
    aov,
    last_order_date,
    DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) AS recency_days,

    DENSE_RANK() OVER (ORDER BY revenue DESC) AS revenue_rank,
    SAFE_DIVIDE(revenue, total_revenue) * 100 AS revenue_contribution_pct,

    -- Pareto-style cumulative contribution
    SUM(SAFE_DIVIDE(revenue, total_revenue)) OVER (
      ORDER BY revenue DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) * 100 AS cumulative_revenue_contribution_pct
  FROM with_totals
)

SELECT *
FROM ranked
WHERE revenue_rank <= 50
ORDER BY revenue_rank;
