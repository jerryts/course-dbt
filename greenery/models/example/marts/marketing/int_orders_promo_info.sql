{{
  config(
    materialized='table'
  )
}}

WITH
  fct_orders AS 
  (
  SELECT
    order_id
  , promo_id
  , user_id
  , address_id
  , order_created_at_utc
  , order_cost_usd
  , putative_order_cost_usd
  , promo_discount_usd
  , shipping_cost_usd
  , order_total_usd
  , putative_order_total_usd
  , tracking_id
  , shipping_service
  , estimated_delivery_at_utc
  , delivered_at_utc
  , order_status
  , num_distinct_items_ordered
  , num_items_ordered
  , product_names_alphabetical
  FROM
    {{ ref('fct_orders') }}
  )
, stg_promos AS 
  (
  SELECT 
    promo_id
  , promo_discount_usd
  , promo_status
  FROM {{ ref('stg_promos') }}
  )

SELECT 
  fct_orders.order_id
, fct_orders.promo_id
, fct_orders.user_id
, fct_orders.address_id
, fct_orders.order_created_at_utc
, fct_orders.order_cost_usd
, fct_orders.order_total_usd
, fct_orders.num_distinct_items_ordered
, fct_orders.num_items_ordered
, fct_orders.product_names_alphabetical
, stg_promos.promo_discount_usd
FROM 
  fct_orders
LEFT JOIN 
  stg_promos USING (promo_id)
