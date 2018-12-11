{{config(schema='custom_schema')}}

SELECT i.order_item_id,
       o.orders_id,
       o.customer_id,
       CONCAT(c.first_name, ' ', c.last_name) as full_name,
       o.order_time,
       p.name,
       cu.currency as source_currency,
       i.ammount * cu.to_aud as ammount_aud
FROM {{ source(var('base.orders')) }} o
  LEFT JOIN {{ source(var('base.customer')) }} c on o.customer_id = c.customer_id
    -- and o.order_time between c.valid_from_timestamp and c.valid_to_timestamp
  LEFT JOIN {{ source(var('base.order_item')) }} i on o.orders_id = i.orders_id
  LEFT JOIN {{ source(var('base.product')) }} p on i.product_id = p.product_id
  LEFT JOIN {{ ref('currency_conversions_fixed')}} cu on cu.currency_id = o.currency_id
    and {{ date_part('day', 'cu.day_value') }} =  {{ date_part('day', 'o.order_time') }}
