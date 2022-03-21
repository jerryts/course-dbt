SELECT
  promo_id
, promo_discount_usd
, COUNT(DISTINCT order_id) AS 
  num_orders
, AVG(order_cost_usd) AS 
  avg_order_cost_usd_per_order
, AVG(num_distinct_items_ordered) AS 
  avg_num_distinct_items_per_order
, AVG(num_items_ordered) AS
  avg_num_items_per_order
FROM
  {{ ref('int_orders_promo_info') }}
WHERE
  promo_id IS NOT NULL
GROUP BY 1,2
