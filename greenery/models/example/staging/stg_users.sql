{{
  config(
    materialized='table'
  )
}}

SELECT 
  user_id
, first_name
, last_name
, email
, phone_number
, created_at AS 
  user_created_at_utc
, updated_at AS 
  user_updated_at_utc
, address_id
FROM {{ source('business', 'users') }}
