from __future__ import print_function

from datetime import datetime

from airflow.models import Variable

from bitcoinetl.build_export_dag import build_export_dag

# When searching for DAGs, Airflow will only consider files where the string “airflow” and “DAG” both appear in the
# contents of the .py file.
DAG = build_export_dag(
    dag_id='dogecoin_export_dag',
    provider_uri=Variable.get('dogecoin_provider_uri'),
    output_bucket=Variable.get('dogecoin_output_bucket'),
    start_date=datetime(2013, 12, 6),
    chain='dogecoin',
    notification_emails=Variable.get('notification_emails', ''),
    schedule_interval='0 4 * * *',
    export_max_workers=1,
    export_batch_size=10,
    max_active_runs=15
)
