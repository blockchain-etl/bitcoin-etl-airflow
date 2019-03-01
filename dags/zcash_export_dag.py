from __future__ import print_function

from datetime import datetime

from airflow.models import Variable

from bitcoinetl.build_export_dag import build_export_dag

start_date = Variable.get('zcash_export_start_date', '2016-10-28')
# When searching for DAGs, Airflow will only consider files where the string "airflow" and "DAG" both appear in the
# contents of the .py file.
DAG = build_export_dag(
    dag_id='zcash_export_dag',
    provider_uri=Variable.get('zcash_provider_uri'),
    output_bucket=Variable.get('zcash_output_bucket'),
    start_date=datetime.strptime(start_date, '%Y-%m-%d'),
    chain='zcash',
    notification_emails=Variable.get('notification_emails', ''),
    schedule_interval='0 16 * * *',
    export_max_workers=3,
    export_batch_size=1
)
