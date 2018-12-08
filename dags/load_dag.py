from __future__ import print_function

import json
import logging
import os
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud.bigquery import TimePartitioning, SchemaField, Client, LoadJobConfig
from google.cloud.bigquery.job import SourceFormat

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# The following datasets must be created in BigQuery:
# - bitcoin_blockchain_raw
# - bitcoin_blockchain_temp
# - bitcoin_blockchain
# Environment variable OUTPUT_BUCKET must be set and point to the GCS bucket
# where files exported by export_dag.py are located

dataset_name = os.environ.get('DATASET_NAME', 'bitcoin_blockchain')
dataset_name_raw = os.environ.get('DATASET_NAME_RAW', 'bitcoin_blockchain_raw')
# dataset_name_temp = os.environ.get('DATASET_NAME_TEMP', 'bitcoin_blockchain_temp')
# destination_dataset_project_id = os.environ.get('DESTINATION_DATASET_PROJECT_ID', None)
# if destination_dataset_project_id is None:
#     raise ValueError('DESTINATION_DATASET_PROJECT_ID is required')

# environment = {
#     'DATASET_NAME': dataset_name,
#     'DATASET_NAME_RAW': dataset_name_raw,
#     'DATASET_NAME_TEMP': dataset_name_temp,
#     'DESTINATION_DATASET_PROJECT_ID': destination_dataset_project_id
# }


def read_bigquery_schema_from_file(filepath):
    file_content = read_file(filepath)
    json_content = json.loads(file_content)
    return read_bigquery_schema_from_json(json_content)


def read_bigquery_schema_from_json(json_schema):
    """
    CAUTION: Recursive function
    This method can generate BQ schemas for nested records
    """
    result = []
    for field in json_schema:
        if field.get('type').lower() == 'record' and field.get('fields'):
            schema = SchemaField(
                name=field.get('name'),
                field_type=field.get('type', 'STRING'),
                mode=field.get('mode', 'NULLABLE'),
                description=field.get('description'),
                fields=read_bigquery_schema_from_json(field.get('fields'))
                )
        else:
            schema = SchemaField(
                name=field.get('name'),
                field_type=field.get('type', 'STRING'),
                mode=field.get('mode', 'NULLABLE'),
                description=field.get('description')
                )
        result.append(schema)
    return result


def read_file(filepath):
    with open(filepath) as file_handle:
        content = file_handle.read()
        for key, value in {}.items():
            content.replace('{{{key}}}'.format(key=key), value)
        return content


def submit_bigquery_job(job, configuration):
    try:
        logging.info('Creating a job: ' + json.dumps(configuration.to_api_repr()))
        result = job.result()
        logging.info(result)
        assert job.errors is None or len(job.errors) == 0
        return result
    except Exception:
        logging.info(job.errors)
        raise


default_dag_args = {
    'depends_on_past': False,
    'start_date': datetime(2018, 7, 1),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

notification_emails = os.environ.get('NOTIFICATION_EMAILS')
if notification_emails and len(notification_emails) > 0:
    default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

# Define a DAG (directed acyclic graph) of tasks.
dag = DAG(
    'bitcoinetl_load_dag',
    catchup=False,
    # Daily at 1:30am
    schedule_interval='30 1 * * *',
    default_args=default_dag_args)

dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')


def add_load_tasks(task, file_format, allow_quoted_newlines=False):
    output_bucket = os.environ.get('OUTPUT_BUCKET')
    if output_bucket is None:
        raise ValueError('You must set OUTPUT_BUCKET environment variable')

    wait_sensor = GoogleCloudStorageObjectSensor(
        task_id='wait_latest_{task}'.format(task=task),
        timeout=60 * 60,
        poke_interval=60,
        bucket=output_bucket,
        object='export/{task}/block_date={datestamp}/{task}.{file_format}'.format(
            task=task, datestamp='{{ds}}', file_format=file_format),
        dag=dag
    )

    def load_task():
        client = Client()
        job_config = LoadJobConfig()
        schema_path = os.path.join(dags_folder, 'resources/stages/raw/schemas/{task}.json'.format(task=task))
        job_config.schema = read_bigquery_schema_from_file(schema_path)
        job_config.source_format = SourceFormat.CSV if file_format == 'csv' else SourceFormat.NEWLINE_DELIMITED_JSON
        if file_format == 'csv':
            job_config.skip_leading_rows = 1
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job_config.allow_quoted_newlines = allow_quoted_newlines
        job_config.ignore_unknown_values = True

        export_location_uri = 'gs://{bucket}/export'.format(bucket=output_bucket)
        uri = '{export_location_uri}/{task}/*.{file_format}'.format(
            export_location_uri=export_location_uri, task=task, file_format=file_format)
        table_ref = client.dataset(dataset_name_raw).table(task)
        load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
        submit_bigquery_job(load_job, job_config)
        assert load_job.state == 'DONE'

    load_operator = PythonOperator(
        task_id='load_{task}'.format(task=task),
        python_callable=load_task,
        execution_timeout=timedelta(minutes=30),
        dag=dag
    )

    # wait_sensor >> load_operator
    return load_operator


load_blocks_task = add_load_tasks('blocks', 'json')
load_transactions_task = add_load_tasks('transactions_raw', 'json')

# enrich_blocks_task = add_enrich_tasks(
#     'blocks', time_partitioning_field='timestamp', dependencies=[load_blocks_task])
# enrich_transactions_task = add_enrich_tasks(
#     'transactions', dependencies=[load_blocks_task, load_transactions_task, load_receipts_task])

# verify_blocks_count_task = add_verify_tasks('blocks_count', [enrich_blocks_task])
# verify_blocks_have_latest_task = add_verify_tasks('blocks_have_latest', [enrich_blocks_task])
# verify_transactions_count_task = add_verify_tasks('transactions_count', [enrich_blocks_task, enrich_transactions_task])
# verify_transactions_have_latest_task = add_verify_tasks('transactions_have_latest', [enrich_transactions_task])


# if notification_emails and len(notification_emails) > 0:
#     send_email_task = EmailOperator(
#         task_id='send_email',
#         to=[email.strip() for email in notification_emails.split(',')],
#         subject='Bitcoin ETL Airflow Load DAG Succeeded',
#         html_content='Bitcoin ETL Airflow Load DAG Succeeded',
#         dag=dag
#     )
    # verify_blocks_count_task >> send_email_task
    # verify_blocks_have_latest_task >> send_email_task
    # verify_transactions_count_task >> send_email_task
    # verify_transactions_have_latest_task >> send_email_task
