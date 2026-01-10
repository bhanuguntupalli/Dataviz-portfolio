-- 01_monthly_kpis.sql
-- Business Question:
-- How is revenue performing over time, and what are the key monthly KPIs
-- (revenue, orders, active customers, AOV) along with month-over-month (MoM) growth?

-- Dataset: bigquery-public-data.thelook_ecommerce
-- Tables: orders, order_items
-- Techniques: CTEs, joins, time-based aggregation, window functions (LAG), rolling averages

WITH base_order_items AS (
  SELECT
    oi.order_id,
    DATE(oi.created_at) AS order_item_date,
    oi.sale_price
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  WHERE oi.created_at IS NOT NULL
),

base_orders AS (
  SELECT
    o.order_id,
    o.user_id,
    DATE(o.created_at) AS order_date
  FROM `bigquery-public-data.thelook_ecommerce.orders` o
  WHERE o.created_at IS NOT NULL
),

joined AS (
  SELECT
    DATE_TRUNC(bo.order_date, MONTH) AS month,
    bo.user_id,
    bo.order_id,
    boi.sale_price
  FROM base_orders bo
  JOIN base_order_items boi
    ON bo.order_id = boi.order_id
  -- Keep only the last 24 months for dashboard performance and relevance
  WHERE bo.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
),

monthly_kpis AS (
  SELECT
    month,
    SUM(sale_price) AS revenue,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(DISTINCT user_id) AS active_customers,
    SAFE_DIVIDE(SUM(sale_price), COUNT(DISTINCT order_id)) AS aov
  FROM joined
  GROUP BY 1
),

final AS (
  SELECT
    month,
    revenue,
    orders,
    active_customers,
    aov,

    -- Month-over-month comparisons (window functions)
    LAG(revenue) OVER (ORDER BY month) AS prev_month_revenue,
    SAFE_DIVIDE(
      revenue - LAG(revenue) OVER (ORDER BY month),
      LAG(revenue) OVER (ORDER BY month)
    ) * 100 AS mom_revenue_growth_pct,

    LAG(orders) OVER (ORDER BY month) AS prev_month_orders,
    SAFE_DIVIDE(
      orders - LAG(orders) OVER (ORDER BY month),
      LAG(orders) OVER (ORDER BY month)
    ) * 100 AS mom_orders_growth_pct,

    -- Rolling average to smooth volatility (adds “experienced analyst” signal)
    AVG(revenue) OVER (
      ORDER BY month
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS revenue_3mo_rolling_avg
  FROM monthly_kpis
)

SELECT *
FROM final
ORDER BY month;
