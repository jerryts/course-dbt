{{
  config(
    materialized='table'
  )
}}

SELECT 
  order_id
, promo_id
, user_id
, address_id
, created_at AS 
  order_created_at_utc
, ROUND(order_cost::NUMERIC, 2) AS 
  order_cost_usd
, ROUND(shipping_cost::NUMERIC, 2) AS 
  shipping_cost_usd
, ROUND(order_total::NUMERIC, 2) AS 
  order_total_usd
, tracking_id
, shipping_service
, estimated_delivery_at AS 
  estimated_delivery_at_utc
, delivered_at AS 
  delivered_at_utc
, status AS 
  order_status
FROM {{ source('business', 'orders') }}
