from __future__ import print_function

from datetime import datetime

from airflow.models import Variable, DAG

from bitcoinetl.build_export_dag import build_export_dag

dag = DAG('bitcoin_export_dag')

dag = build_export_dag(
    dag_id='bitcoin_export_dag',
    provider_uri=Variable.get('bitcoin_provider_uri'),
    output_bucket=Variable.get('bitcoin_output_bucket'),
    start_date=datetime(2018, 12, 14),
    chain='bitcoin',
    notification_emails=Variable.get('notification_emails', ''),
    schedule_interval='0 3 * * *',
    export_max_workers=4,
    export_batch_size=1
)
