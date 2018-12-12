#!/usr/bin/env bash

echo "Seeding local postgres"
python example_data.py 1000
python create_dbt_project.py psql
dbt seed

echo "Seeding remote postgres"
python example_data.py 10000
python create_dbt_project.py psql
dbt seed --target psql

echo "Seeding bigquery"
python example_data.py 100000
python create_dbt_project.py bq
dbt seed --target bq
