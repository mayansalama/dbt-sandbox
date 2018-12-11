import os
import pickle
import datetime

from google.cloud import bigquery


def pytype2bq(dtype):
    type_mappings = (
        (str, "string"),
        (float, "float"),
        (int, "integer"),
        (datetime.datetime, "timestamp")
    )

    return [t[1] for t in type_mappings if dtype == t[0]][0]


def seed(data_folder, cl):
    tables = {os.path.basename(f).split('.')[0] for f in os.listdir(data_folder)}
    for table_name in tables:

        tbl_ref = bigquery.TableReference.from_string("source.{}".format(table_name),
                                                      default_project=cl.project)
        try:
            cl.delete_table(tbl_ref)
        except Exception as e:
            print("Got error {} - assuming table didn't already exist - continuing".format(str(e)))
            pass

        with open(os.path.join(data_folder, table_name + ".schema"), "rb") as f:
            schema = pickle.load(f)
        bq_schema = [bigquery.SchemaField(col, pytype2bq(dtype)) for col, dtype in schema.items()]

        table = bigquery.Table(tbl_ref, bq_schema)
        table = client.create_table(table)  # API request
        print("Created table source.{}".format(table_name))

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = 'text/csv'
        job_config.skip_leading_rows = 1

        print("Loading table source.{} ...".format(table_name), flush=True, end="")

        # Load data
        with open(os.path.join(data_folder, table_name + ".csv"), 'rb') as f:
            cl.load_table_from_file(f, tbl_ref, job_config=job_config)

        print("DONE")


if __name__ == "__main__":
    client = bigquery.Client()

    data_folder = "sample-data"
    seed(data_folder, client)
