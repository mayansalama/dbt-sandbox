# DBT SetUp

Collection of utilities to generate relational data and load to local
and remote postgres, as well as bigquery. Note that this generates
dynamic dbt_project.yml files, hence why it has been isolated
from the original table.

This will seed:
1. Local postgres with ~20k records
1. Remote postgres with ~200k records
1. BigQuery with ~2m records.

Note that currently dbt will infer the schema from a CSV when seeding.
Until overwrites stop this behaviour, this can be a slow process for 
large files.

### how2run
```bash
pip install -r requirements.txt
sh seed_targets.sh
```

### Example Data
This utilises lab-grown-sheets to generate sample data to seed 
into the databases. For documentation on how to configure or 
modify this see [here](git+https://github.com/mayansalama/lab-grown-sheets.git) 
