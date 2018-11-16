SELECT i.order_item_id,
       o.orders_id,
       o.customer_id,
       CONCAT(c.first_name, ' ', c.last_name) as full_name,
       o.order_time,
       p.name,
       cu.currency as source_currency,
       i.ammount * cu.to_aud as ammount_aud
FROM source.orders o
  LEFT JOIN source.customer c on o.customer_id = c.customer_id
  LEFT JOIN source.order_item i on o.orders_id = i.orders_id
  LEFT JOIN source.product p on i.product_id = p.product_id
  LEFT JOIN {{ref('currency_conversions_fixed')}} cu on cu.currency_id = o.currency_id
    and DATE_PART('day', cu.day_value) = DATE_PART('day', o.order_time)
