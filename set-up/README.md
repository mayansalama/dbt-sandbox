# DBT SetUp

Collection of utilities to generate data and load into my-sql
database running locally. Could centralise constants and 
parameterise example_data and seed_database but they're just
one offs.

### how2run
```bash
pip install -r requirements.txt
python example_data.py
sh start_database.sh  # Only if running locally 
python seed_database.py
```

Then point dbt at local host with admin^2 credentials. 

### Mild Doco
Spicy doco is reading the comments...
#### Example Data
Creates a simple star schema dataset to test against. Size of the
dataset is modified via constants in Script.

#### Star Schema Generator
Supply a schema definition to generate sample data, with linked
entities in fact and dim tables.

To make:
- csv: to_csv
- json: to_json
- pickled schema: to_schema (see seed_database for usage)

#### Start Postgres
Script to initiate postgresql database locally.

#### Seed Postgres
Loads all files from sample-data directory. Assumes:

- Table name is same as file basename
- For each file basename there is a corresponding `.csv` and `.schema` file.

Change the conn_string to hit a remote postgres instance.