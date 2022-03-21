WITH
  stg_users AS 
  (
  SELECT
    user_id
  , first_name
  , last_name
  , email
  , phone_number
  , user_created_at_utc
  , user_updated_at_utc
  , address_id
  FROM
    {{ ref('stg_users') }}
  )
, stg_addresses AS 
  (
  SELECT
    address_id
  , address
  , zipcode
  , state
  , country
  FROM
    {{ ref('stg_addresses') }}
  )
, stg_events AS 
  (
  SELECT
    event_id
  , session_id
  , user_id
  , event_type
  , event_page_url
  , event_created_at_utc
  , order_id
  , product_id
  FROM
    {{ ref('stg_events') }}
  )
, fct_orders AS 
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
, users_orders_info AS 
  (
  SELECT
    user_id
  , COUNT(order_id) AS 
    num_orders_ever
  , MIN(order_created_at_utc) AS 
    first_order_created_at_utc
  , SUM(order_cost_usd) AS 
    total_orders_cost_usd
  , SUM(order_total_usd) AS 
    total_orders_total_usd
  , SUM(num_items_ordered) AS 
    total_items_ordered_ever
  FROM
    fct_orders
  GROUP BY 1
  )
, users_events_info AS 
  (
  SELECT
    user_id
  , MIN(event_created_at_utc) AS 
    first_event_created_at_utc
  , COUNT(DISTINCT session_id) AS 
    num_distinct_sessions
  , BOOL_OR(CASE WHEN event_type = 'page_view' THEN TRUE ELSE FALSE END) AS 
    ever_viewed_page
  , BOOL_OR(CASE WHEN event_type = 'add_to_cart' THEN TRUE ELSE FALSE END) AS 
    ever_added_product_to_cart
  , BOOL_OR(CASE WHEN event_type = 'checkout' THEN TRUE ELSE FALSE END) AS 
    ever_checked_out
  FROM
    stg_events
  GROUP BY 1
  )

SELECT
  stg_users.user_id
, stg_users.first_name
, stg_users.last_name
, stg_users.email
, stg_users.phone_number
, stg_users.user_created_at_utc
, stg_users.user_updated_at_utc
, stg_users.address_id
, stg_addresses.address
, stg_addresses.zipcode
, stg_addresses.state
, stg_addresses.country
, COALESCE(users_orders_info.num_orders_ever, 0) AS 
  num_orders_ever
, users_orders_info.first_order_created_at_utc
, users_orders_info.total_orders_cost_usd
, users_orders_info.total_orders_total_usd
, COALESCE(users_orders_info.total_items_ordered_ever, 0) AS 
  total_items_ordered_ever
, users_events_info.first_event_created_at_utc
, COALESCE(users_events_info.num_distinct_sessions, 0) AS 
  num_distinct_sessions
, COALESCE(users_events_info.ever_viewed_page, FALSE) AS 
  ever_viewed_page
, COALESCE(users_events_info.ever_added_product_to_cart, FALSE) AS 
  ever_added_product_to_cart
, COALESCE(users_events_info.ever_checked_out, FALSE) AS 
  ever_checked_out
FROM
  stg_users
LEFT JOIN
  stg_addresses USING (address_id)
LEFT JOIN
  users_orders_info USING (user_id)
LEFT JOIN
  users_events_info USING (user_id)
