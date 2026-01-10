-- 02_category_revenue.sql
-- Business Question:
-- Which product categories drive the most revenue, and how concentrated is revenue across categories?

-- Dataset: bigquery-public-data.thelook_ecommerce
-- Tables: order_items, products
-- Techniques: CTEs, joins, aggregations, window functions (RANK, SUM OVER)

WITH base AS (
  SELECT
    DATE(oi.created_at) AS order_item_date,
    p.category,
    oi.sale_price
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  JOIN `bigquery-public-data.thelook_ecommerce.products` p
    ON p.id = oi.product_id
  WHERE oi.created_at IS NOT NULL
    AND DATE(oi.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
),

category_rev AS (
  SELECT
    category,
    SUM(sale_price) AS revenue
  FROM base
  GROUP BY 1
),

with_totals AS (
  SELECT
    category,
    revenue,
    SUM(revenue) OVER () AS total_revenue
  FROM category_rev
),

final AS (
  SELECT
    category,
    revenue,
    SAFE_DIVIDE(revenue, total_revenue) * 100 AS revenue_share_pct,
    RANK() OVER (ORDER BY revenue DESC) AS category_rank,

    
    SUM(SAFE_DIVIDE(revenue, total_revenue)) OVER (
      ORDER BY revenue DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) * 100 AS cumulative_revenue_share_pct
  FROM with_totals
)

SELECT *
FROM final
ORDER BY revenue DESC;
