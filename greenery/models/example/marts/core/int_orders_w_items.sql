{{
  config(
    materialized='table'
  )
}}

WITH
  stg_orders AS
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
  FROM 
    {{ ref('stg_orders') }}
  )
, stg_order_items AS
  (
  SELECT 
    order_id
  , product_id
  , quantity
  FROM 
    {{ ref('stg_order_items') }}
  )
, stg_products AS
  (
  SELECT 
    product_id
  , product_name
  , product_price_usd
  , inventory
  FROM 
    {{ ref('stg_products') }}
  )

SELECT
  stg_orders.order_id
, stg_orders.promo_id
, stg_orders.user_id
, stg_orders.address_id
, stg_orders.order_created_at_utc
, stg_orders.order_cost_usd
, stg_orders.shipping_cost_usd
, stg_orders.order_total_usd
, stg_orders.tracking_id
, stg_orders.shipping_service
, stg_orders.estimated_delivery_at_utc
, stg_orders.delivered_at_utc
, stg_orders.order_status
, stg_order_items.quantity
, stg_products.product_name
, stg_products.product_price_usd
FROM
  stg_orders
LEFT JOIN
  stg_order_items USING (order_id)
LEFT JOIN
  stg_products USING (product_id)

