CREATE OR REPLACE VIEW `dataviz-portfolio-483720.portfolio_thelook.v_customer_ltv` AS
WITH customer_rev AS (
  SELECT
    o.user_id,
    SUM(oi.sale_price) AS ltv_revenue,
    COUNT(DISTINCT oi.order_id) AS total_orders,
    MIN(DATE(o.created_at)) AS first_order_date,
    MAX(DATE(o.created_at)) AS last_order_date
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  JOIN `bigquery-public-data.thelook_ecommerce.orders` o
    ON o.order_id = oi.order_id
  GROUP BY 1
)
SELECT
  user_id,
  ltv_revenue,
  total_orders,
  SAFE_DIVIDE(ltv_revenue, total_orders) AS avg_order_value,
  first_order_date,
  last_order_date,
  DATE_DIFF(last_order_date, first_order_date, DAY) AS customer_lifetime_days
FROM customer_rev;
