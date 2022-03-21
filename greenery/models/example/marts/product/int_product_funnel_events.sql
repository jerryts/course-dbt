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

SELECT
  stg_events.event_id
, stg_events.session_id
, stg_events.user_id
, stg_events.event_type
, stg_events.event_page_url
, stg_events.event_created_at_utc
, COALESCE(stg_events.product_id, int_orders_w_items.product_id) AS 
  product_id
FROM
  stg_events
LEFT JOIN
  int_orders_w_items USING (order_id)
WHERE
  stg_events.event_type IN('page_view', 'add_to_cart', 'checkout')
