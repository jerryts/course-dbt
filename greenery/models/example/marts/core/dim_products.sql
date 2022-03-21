WITH
  stg_products AS 
  (
  SELECT
    product_id
  , product_name
  , product_price_usd
  , inventory
  FROM
    {{ ref('stg_products') }}
  )
, int_orders_w_items AS 
  (
  SELECT
    order_id
  , promo_id
  , user_id
  , address_id
  , order_created_at_utc
  , order_cost_usd
  , shipping_cost_usd
  , order_total_usd
  , tracking_id
  , shipping_service
  , estimated_delivery_at_utc
  , delivered_at_utc
  , order_status
  , product_id
  , quantity
  , product_name
  , product_price_usd
  FROM
    {{ ref('int_orders_w_items') }}
  )

SELECT
  stg_products.product_id
, stg_products.product_name
, stg_products.product_price_usd
, stg_products.inventory
, COALESCE(COUNT(DISTINCT order_id), 0) AS 
  num_orders
, COALESCE(SUM(quantity), 0) AS 
  quantity_ordered
FROM
  stg_products
LEFT JOIN
  int_orders_w_items USING (product_id)
GROUP BY
  1,2,3,4