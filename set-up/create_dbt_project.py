import os
import sys

import yaml

seed_data_name = "data"
seed_schema_name = "source"
model_name = "seed_from_labgrownsheets"

default_postgres_schema_file = os.path.join("schema", "PostgresSchema.yml")
default_bigquery_file = os.path.join("schema", "BigquerySchema.yml")


def main(f):
    with open(f, "r+") as f:
        cnts = yaml.load(f)

    full_project = {
        "name": model_name,
        "version": "1.0",
        "profile": "default",
        "data-paths": [seed_data_name],
        "target-path": "target",
        "seeds": {
            'schema': seed_schema_name,
            model_name: {
                "enabled": True,
                **cnts
            }
        }
    }

    with open("dbt_project.yml", "w+") as f:
        yaml.dump(full_project, f, default_flow_style=False)


if __name__ == "__main__":
    args = sys.argv
    err = ValueError("Input must be either `bq` or `postgres`")

    if len(args) == 1:
        raise err
    elif args[1].lower() in ["bq", "bigquery"]:
        fname = default_bigquery_file
    elif args[1].lower() in ['psql', 'postgres']:
        fname = default_postgres_schema_file
    else:
        raise err

    main(fname)
