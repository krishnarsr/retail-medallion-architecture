-- Run these expressions from Spark SQL after registering the Gold Delta paths.

-- 1. Daily revenue and margin trend
SELECT order_date, SUM(net_revenue) AS revenue, SUM(gross_margin) AS margin
FROM gold_daily_sales
GROUP BY order_date
ORDER BY order_date;

-- 2. Highest-value customers
SELECT customer_id, full_name, segment, lifetime_orders, lifetime_value, value_band
FROM gold_customer_360
ORDER BY lifetime_value DESC
LIMIT 20;

-- 3. Best products inside each category
SELECT category, product_name, units_sold, net_revenue, gross_margin
FROM gold_product_performance
WHERE revenue_rank_in_category <= 5
ORDER BY category, revenue_rank_in_category;

-- 4. Channel mix
SELECT channel, SUM(orders) AS orders, SUM(net_revenue) AS revenue
FROM gold_daily_sales
GROUP BY channel
ORDER BY revenue DESC;
