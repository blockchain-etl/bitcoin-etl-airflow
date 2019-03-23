from __future__ import print_function

import logging

from airflow.models import Variable

from bitcoinetl.build_verify_streaming_dag import build_verify_streaming_dag

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# When searching for DAGs, Airflow will only consider files where the string "airflow" and "DAG" both appear in the
# contents of the .py file.
DAG = build_verify_streaming_dag(
    dag_id='bitcoin_verify_streaming_dag',
    destination_dataset_project_id=Variable.get('bitcoin_destination_dataset_project_id'),
    chain='bitcoin',
    notification_emails=Variable.get('notification_emails', '')
)
