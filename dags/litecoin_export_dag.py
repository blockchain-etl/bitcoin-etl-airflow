from __future__ import print_function

from datetime import datetime

from airflow.models import Variable, DAG

from bitcoinetl.build_export_dag import build_export_dag

dag = DAG('litecoin_export_dag')

dag = build_export_dag(
    dag_id='litecoin_export_dag',
    provider_uri=Variable.get('litecoin_provider_uri'),
    output_bucket=Variable.get('litecoin_output_bucket'),
    start_date=datetime(2011, 10, 7),
    chain='litecoin',
    notification_emails=Variable.get('notification_emails', ''),
    schedule_interval='0 5 * * *',
    export_max_workers=4,
    export_batch_size=1
)
