--with v1 as (SELECT order_item_id, count(*) as ct FROM "dbtsandbox_custom_schema"."order_items_joined" group by order_item_id)
select * from "dbtsandbox_custom_schema"."order_items_joined" where order_item_id = 'b286a3ccb80f'

SELECT * FROM source.customer where customer_id = 'defbc85dbf1d'


select * from source.order_item where order_item_id = 'b286a3ccb80f'

select * from source.currency_conversion