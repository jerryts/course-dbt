{{
  config(
    materialized='table'
  )
}}

SELECT 
    promo_id,
    discount,
    status
FROM {{ source('business', 'promos') }}
