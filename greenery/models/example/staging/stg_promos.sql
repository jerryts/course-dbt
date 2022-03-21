{{
  config(
    materialized='table'
  )
}}

SELECT 
    promo_id
  , ROUND(discount::NUMERIC, 2) AS 
    promo_discount_usd
  , status AS 
    promo_status
FROM {{ source('business', 'promos') }}
