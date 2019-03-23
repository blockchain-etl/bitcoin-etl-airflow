from __future__ import print_function

from bitcoinetl.build_export_dag import build_export_dag
from bitcoinetl.variables import read_export_dag_vars

# When searching for DAGs, Airflow will only consider files where the string "airflow" and "DAG" both appear in the
# contents of the .py file.
DAG = build_export_dag(
    dag_id='dash_export_dag',
    chain='dash',
    **read_export_dag_vars(
        var_prefix='dash_',
        export_schedule_interval='0 15 * * *',
        export_start_date='2014-01-19',
        export_max_workers=3,
        export_batch_size=10,
    )
)
