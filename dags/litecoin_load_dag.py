from __future__ import print_function

import logging

from bitcoinetl.build_load_dag import build_load_dag
from bitcoinetl.variables import read_load_dag_vars

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# When searching for DAGs, Airflow will only consider files where the string "airflow" and "DAG" both appear in the
# contents of the .py file.
DAG = build_load_dag(
    dag_id='litecoin_load_dag',
    chain='litecoin',
    **read_load_dag_vars(
        var_prefix='litecoin_',
        schedule_interval='30 14 * * *'
    )
)
