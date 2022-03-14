## Jerry Tsai is enjoying corise's dbt course

### Week 1 answers to questions

#### (1) How many users do we have?

130
``` sql
SELECT COUNT(1) FROM dbt_jerry_t.stg_users;
```

#### (2) On average, how many orders do we receive per hour?

7.5208333333333333

``` sql
SELECT
  AVG(count)
FROM
  (
  SELECT 
    DATE_TRUNC('hour', created_at) AS date_hour
  , COUNT(1) AS count
  FROM dbt_jerry_t.stg_orders
  GROUP BY 1
  ) AS order_count_by_hour
;
```

#### (3) On average, how long does an order take from being placed to being delivered?

3.891803278688524

``` sql
SELECT
  AVG(seconds_between_order_placed_and_delivery) / 60.0 / 60.0 / 24.0 AS avg_days_for_delivery
FROM
  (
  SELECT
    delivered_at
  , 1.0 * EXTRACT(EPOCH FROM (delivered_at - created_at)) AS seconds_between_order_placed_and_delivery
  FROM
    dbt_jerry_t.stg_orders
  WHERE
    delivered_at IS NOT NULL
  ) AS delivered_orders
;
```

#### (4) How many users have only made one purchase? Two purchases? Three+ purchases?

```
num_purchases_labeled  count
1                       25
2                       28
3+                      71
```

``` sql
SELECT
  num_purchases_labeled
, COUNT(1) AS count
FROM
  (
  SELECT
    user_id
  , CASE 
    WHEN COUNT(1)  = 1 THEN '1'
    WHEN COUNT(1)  = 2 THEN '2'
    WHEN COUNT(1) >= 3 THEN '3+'
    ELSE NULL 
    END AS 
    num_purchases_labeled
  , COUNT(1) AS num_purchases
  FROM
    dbt_jerry_t.stg_orders
  GROUP BY 
    user_id
  HAVING
    COUNT(1) >= 1
  ) AS users_w_at_least_one_purchase
GROUP BY 1
ORDER BY 1
;
```

#### (5) On average, how many unique sessions do we have per hour?

16.3275862068965517

``` sql
SELECT
  AVG(num_distinct_sessions) AS avg_num_unique_sessions_per_hour
FROM
  (
  SELECT
    date_hour
  , COUNT(1) AS num_distinct_sessions
  FROM
    (
    SELECT DISTINCT
      DATE_TRUNC('hour', created_at) AS date_hour
    , session_id
    FROM
      dbt_jerry_t.stg_events
    ) AS unique_sessions_in_each_date_hour
  GROUP BY 1
  ) AS num_unique_sessions_in_each_date_hour
;
```
