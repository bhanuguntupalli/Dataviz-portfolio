CREATE OR REPLACE VIEW `dataviz-portfolio-483720.portfolio_thelook.v_monthly_kpis` AS
WITH monthly AS (
  SELECT
    DATE_TRUNC(DATE(oi.created_at), MONTH) AS month,
    SUM(oi.sale_price) AS revenue,
    COUNT(DISTINCT oi.order_id) AS orders,
    COUNT(DISTINCT o.user_id) AS active_customers
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  JOIN `bigquery-public-data.thelook_ecommerce.orders` o
    ON o.order_id = oi.order_id
  WHERE oi.created_at IS NOT NULL
  GROUP BY 1
),
final AS (
  SELECT
    month,
    revenue,
    orders,
    active_customers,
    SAFE_DIVIDE(revenue, orders) AS aov,
    LAG(revenue) OVER (ORDER BY month) AS prev_month_revenue,
    SAFE_DIVIDE(revenue - LAG(revenue) OVER (ORDER BY month),
                LAG(revenue) OVER (ORDER BY month)) * 100 AS mom_growth_pct
  FROM monthly
)
SELECT *
FROM final
WHERE month >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH);
