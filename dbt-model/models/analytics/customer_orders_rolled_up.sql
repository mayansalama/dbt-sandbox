{{config(materialized = 'table')}}

SELECT customer_id
      , full_name
      , COUNT(DISTINCT orders_id) as num_orders
      , COUNT(*) as num_items
      , SUM(ammount_aud) as total_spend
FROM {{ref('order_items_joined')}}
GROUP BY customer_id, full_name
