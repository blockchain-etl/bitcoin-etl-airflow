from __future__ import print_function

from bitcoinetl.build_export_dag import build_export_dag
from bitcoinetl.variables import read_export_dag_vars

# When searching for DAGs, Airflow will only consider files where the string "airflow" and "DAG" both appear in the
# contents of the .py file.
DAG = build_export_dag(
    dag_id='dogecoin_export_dag',
    chain='dogecoin',
    **read_export_dag_vars(
        var_prefix='dogecoin_',
        export_schedule_interval='0 13 * * *',
        export_start_date='2013-12-06',
        export_max_workers=3,
        export_batch_size=10,
    )
)
