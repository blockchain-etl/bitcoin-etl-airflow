from __future__ import print_function

from bitcoinetl.build_export_dag import build_export_dag
from bitcoinetl.variables import read_export_dag_vars

# When searching for DAGs, Airflow will only consider files where the string "airflow" and "DAG" both appear in the
# contents of the .py file.
DAG = build_export_dag(
    dag_id='zcash_export_dag',
    chain='zcash',
    **read_export_dag_vars(
        var_prefix='zcash_',
        export_schedule_interval='0 16 * * *',
        export_start_date='2016-10-28',
        export_max_workers=3,
        export_batch_size=1,
    )
)
