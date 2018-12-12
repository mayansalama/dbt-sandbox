SELECT name
       ,count(distinct order_item_id) as num_orders
       ,avg(ammount_aud) as avg_price
FROM {{ ref('order_items_joined') }}
GROUP BY name
