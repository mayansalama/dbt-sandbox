#!/usr/bin/env bash

echo "Creating local postgres data (1000 base its)"
python example_data.py 1000
python create_dbt_project.py psql
echo "Seeding via dbt..."
dbt seed --full-refresh

echo "Creating remote postgres data (10000 base its)"
python example_data.py 10000
python create_dbt_project.py psql
echo "Seeding via dbt..."
dbt seed --target psql --full-refresh

echo "Creating bq data (100000 base its)"
python example_data.py 100000
python create_dbt_project.py bq
echo "Seeding via dbt..."
dbt seed --target bq --full-refresh
