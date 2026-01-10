CREATE OR REPLACE VIEW `dataviz-portfolio-483720.portfolio_thelook.v_category_revenue` AS
WITH cat AS (
  SELECT
    p.category,
    SUM(oi.sale_price) AS revenue
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  JOIN `bigquery-public-data.thelook_ecommerce.products` p
    ON p.id = oi.product_id
  WHERE oi.created_at IS NOT NULL
  GROUP BY 1
),
tot AS (
  SELECT SUM(revenue) AS total_revenue FROM cat
)
SELECT
  c.category,
  c.revenue,
  SAFE_DIVIDE(c.revenue, t.total_revenue) * 100 AS revenue_share_pct
FROM cat c
CROSS JOIN tot t
ORDER BY c.revenue DESC;
