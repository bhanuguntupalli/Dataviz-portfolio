CREATE OR REPLACE VIEW `dataviz-portfolio-483720.portfolio_thelook.v_top_customers` AS
WITH customer_sales AS (
  SELECT
    o.user_id,
    SUM(oi.sale_price) AS revenue,
    COUNT(DISTINCT oi.order_id) AS orders
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  JOIN `bigquery-public-data.thelook_ecommerce.orders` o
    ON o.order_id = oi.order_id
  GROUP BY 1
),
ranked AS (
  SELECT
    user_id,
    revenue,
    orders,
    SAFE_DIVIDE(revenue, orders) AS aov,
    RANK() OVER (ORDER BY revenue DESC) AS revenue_rank
  FROM customer_sales
),
tot AS (
  SELECT SUM(revenue) AS total_revenue FROM customer_sales
)
SELECT
  r.*,
  SAFE_DIVIDE(r.revenue, t.total_revenue) * 100 AS revenue_contribution_pct
FROM ranked r
CROSS JOIN tot t
WHERE revenue_rank <= 50
ORDER BY revenue_rank;
