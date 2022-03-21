{{
  config(
    materialized='table'
  )
}}

SELECT 
    order_id
  , product_id
  , quantity
FROM {{ source('business', 'order_items') }}
