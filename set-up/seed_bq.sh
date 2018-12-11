#!/usr/bin/env bash

bq --location=US load --skip_leading_rows=1 --source_format=CSV source.customer customer.csv customer_id,first_name,last_name,gender,address,valid_from_timestamp,valid_to_timestamp
bq --location=US load --skip_leading_rows=1 --source_format=CSV source.currency currency.csv currency_id,currency
bq --location=US load --skip_leading_rows=1 --source_format=CSV source.currency_conversion currency_conversion.csv currency_conversion_id,currency_id,day_value,to_aud
bq --location=US load --skip_leading_rows=1 --source_format=CSV source.order_item order_item.csv order_item_id,orders_id,product_id,ammount
bq --location=US load --skip_leading_rows=1 --source_format=CSV source.orders orders.csv orders_id,customer_id,currency_id,order_time
bq --location=US load --skip_leading_rows=1 --source_format=CSV source.product product.csv product_id,name,long_desc