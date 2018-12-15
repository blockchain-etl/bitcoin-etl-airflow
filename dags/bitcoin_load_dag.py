from __future__ import print_function

import logging

from airflow.models import Variable

from bitcoinetl.build_load_dag import build_load_dag

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

dag = build_load_dag(
    dag_id='bitcoin_load_dag',
    output_bucket=Variable.get('bitcoin_output_bucket'),
    destination_dataset_project_id=Variable.get('destination_dataset_project_id'),
    chain='bitcoin',
    notification_emails=Variable.get('notification_emails', ''),
    schedule_interval='30 3 * * *'
)
