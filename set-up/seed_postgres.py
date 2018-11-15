import os
import pickle
import datetime

import psycopg2


def pytype2psql(dtype):
    type_mappings = (
        (str, "text"),
        (float, "real"),
        (int, "int"),
        (datetime.datetime, "timestamp")
    )

    return [t[1] for t in type_mappings if dtype == t[0]][0]


def run_sql(cur, sql):
    print("Running sql " + sql)
    cur.execute(sql)


def seed(data_folder, conn_string):
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    def sql(sql):
        return run_sql(cur, sql)

    tables = {os.path.basename(f).split('.')[0] for f in os.listdir(data_folder)}
    for table in tables:
        # Table setup
        sql('DROP TABLE IF EXISTS {}'.format(table))
        with open(os.path.join(data_folder, table + ".schema"), "rb") as f:
            schema = pickle.load(f)
        postgres_schema = ["{} {}".format(col, pytype2psql(dtype)) for col, dtype in schema.items()]
        postgres_schema[0] += " PRIMARY KEY"
        sql('CREATE TABLE {}({})'.format(table, ', '.join(postgres_schema)))

        # Load data
        with open(os.path.join(data_folder, table + ".csv"), 'r') as f:
            next(f)  # Skip the header row.
            print("Copying data...")
            cur.copy_expert("COPY {} FROM STDIN WITH (FORMAT CSV)".format(table), f)

    conn.commit()


if __name__ == "__main__":
    conn_string = "host=localhost dbname=dbtsandbox user=admin"
    data_folder = "sample-data"
    seed(data_folder, conn_string)
