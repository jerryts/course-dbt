WITH
  int_product_funnel_events AS 
  (
  SELECT
    event_id
  , session_id
  , user_id
  , event_type
  , event_page_url
  , event_created_at_utc
  , product_id
  FROM 
    {{ ref('int_product_funnel_events') }}
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
, session_product_cross_sectional_funnel_events AS 
  (
  SELECT
    session_id
  , product_id
  , BOOL_OR(CASE WHEN event_type = 'page_view' THEN TRUE ELSE FALSE END) AS 
    viewed_page
  , BOOL_OR(CASE WHEN event_type = 'add_to_cart' THEN TRUE ELSE FALSE END) AS 
    added_to_cart
  , BOOL_OR(CASE WHEN event_type = 'checkout' THEN TRUE ELSE FALSE END) AS 
    checked_out
  FROM
    int_product_funnel_events
  GROUP BY 1,2
  )
, product_cross_sectional_funnel_stats AS 
  (
  SELECT
    product_id
  , SUM(CASE WHEN viewed_page THEN 1 ELSE 0 END) AS 
    num_sessions_viewed_page
  , SUM(CASE WHEN added_to_cart THEN 1 ELSE 0 END) AS 
    num_sessions_added_to_cart
  , SUM(CASE WHEN checked_out THEN 1 ELSE 0 END) AS 
    num_sessions_checked_out
  FROM
    session_product_cross_sectional_funnel_events
  GROUP BY 1
  )
, product_cross_sectional_funnel_more_stats AS 
  (
  SELECT
    product_id
  , num_sessions_viewed_page
  , num_sessions_added_to_cart
  , num_sessions_checked_out
  , CASE WHEN num_sessions_viewed_page > 0 THEN 1.0 * num_sessions_added_to_cart / num_sessions_viewed_page ELSE NULL END AS 
    added_to_cart_rate
  , CASE WHEN num_sessions_added_to_cart > 0 THEN 1.0 * num_sessions_checked_out / num_sessions_added_to_cart ELSE NULL END AS 
    checked_out_rate
  FROM
    product_cross_sectional_funnel_stats
  )

SELECT
  product_cross_sectional_funnel_more_stats.product_id
, stg_products.product_name
, stg_products.product_price_usd
, product_cross_sectional_funnel_more_stats.num_sessions_viewed_page
, product_cross_sectional_funnel_more_stats.num_sessions_added_to_cart
, product_cross_sectional_funnel_more_stats.num_sessions_checked_out
, product_cross_sectional_funnel_more_stats.added_to_cart_rate
, product_cross_sectional_funnel_more_stats.checked_out_rate
FROM
  product_cross_sectional_funnel_more_stats
LEFT JOIN
  stg_products USING (product_id)

