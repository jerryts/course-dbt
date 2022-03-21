{{
  config(
    materialized='table'
  )
}}

SELECT 
  product_id
, name AS 
  product_name
, ROUND(price::NUMERIC, 2) AS 
  product_price_usd
, inventory
FROM {{ source('business', 'products') }}
