from __future__ import print_function

import logging

from bitcoinetl.build_verify_streaming_dag import build_verify_streaming_dag
from bitcoinetl.variables import read_verify_streaming_dag_vars

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# When searching for DAGs, Airflow will only consider files where the string "airflow" and "DAG" both appear in the
# contents of the .py file.
DAG = build_verify_streaming_dag(
    dag_id='bitcoin_cash_verify_streaming_dag',
    chain='bitcoin_cash',
    **read_verify_streaming_dag_vars(
        var_prefix='bitcoin_cash_',
        max_lag_in_minutes=500,
    )
)
