{{
  config(
    materialized='table'
  )
}}

WITH
  int_orders_w_items AS
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
, stg_promos AS
  (
  SELECT 
    promo_id
  , promo_discount_usd
  , promo_status
  FROM 
    {{ ref('stg_promos') }}
  )
, orders_summarized AS
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
  , ROUND(SUM(product_price_usd * quantity)::NUMERIC, 2) AS 
    putative_order_cost_usd
  , ARRAY_AGG(product_name ORDER BY product_name) AS 
    product_names_alphabetical
  FROM
    int_orders_w_items
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
  )

SELECT
  orders_summarized.order_id
, orders_summarized.promo_id
, orders_summarized.user_id
, orders_summarized.address_id
, orders_summarized.order_created_at_utc
, orders_summarized.order_cost_usd
, orders_summarized.putative_order_cost_usd
, orders_summarized.order_cost_usd - orders_summarized.putative_order_cost_usd AS 
  order_cost_diff
, stg_promos.promo_discount_usd
, orders_summarized.shipping_cost_usd
, orders_summarized.order_total_usd
, ROUND((orders_summarized.putative_order_cost_usd - COALESCE(stg_promos.promo_discount_usd, 0) + COALESCE(orders_summarized.shipping_cost_usd, 0))::NUMERIC, 2) AS 
  putative_order_total_usd
, orders_summarized.order_total_usd - ROUND((orders_summarized.putative_order_cost_usd - COALESCE(stg_promos.promo_discount_usd, 0) + COALESCE(orders_summarized.shipping_cost_usd, 0))::NUMERIC, 2) AS 
  order_total_diff
, orders_summarized.tracking_id
, orders_summarized.shipping_service
, orders_summarized.estimated_delivery_at_utc
, orders_summarized.delivered_at_utc
, orders_summarized.order_status
, orders_summarized.product_names_alphabetical
FROM
  orders_summarized
LEFT JOIN
  stg_promos USING (promo_id)

