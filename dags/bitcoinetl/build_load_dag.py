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
from google.api_core.exceptions import Conflict
from google.cloud.bigquery import TimePartitioning, SchemaField, Client, LoadJobConfig, Table, QueryJobConfig, \
    QueryPriority, CopyJobConfig
from google.cloud.bigquery.job import SourceFormat

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


# The following datasets must be created in BigQuery:
# - crypto_{chain}_raw
# - crypto_{chain}_temp
# - crypto_{chain}

def build_load_dag(
        dag_id,
        output_bucket,
        destination_dataset_project_id,
        chain='bitcoin',
        notification_emails=None,
        load_start_date=datetime(2018, 7, 1),
        schedule_interval='0 0 * * *',
        load_all_partitions=True):
    dataset_name = 'crypto_{}'.format(chain)
    dataset_name_raw = 'crypto_{}_raw'.format(chain)
    dataset_name_temp = 'crypto_{}_temp'.format(chain)

    environment = {
        'dataset_name': dataset_name,
        'dataset_name_raw': dataset_name_raw,
        'dataset_name_temp': dataset_name_temp,
        'destination_dataset_project_id': destination_dataset_project_id,
        'load_all_partitions': load_all_partitions
    }

    default_dag_args = {
        'depends_on_past': False,
        'start_date': load_start_date,
        'email_on_failure': True,
        'email_on_retry': True,
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    # Define a DAG (directed acyclic graph) of tasks.
    dag = DAG(
        dag_id,
        catchup=False,
        schedule_interval=schedule_interval,
        default_args=default_dag_args)

    dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')

    def add_load_tasks(task, file_format, allow_quoted_newlines=False):

        wait_sensor = GoogleCloudStorageObjectSensor(
            task_id='wait_latest_{task}'.format(task=task),
            timeout=60 * 60,
            poke_interval=60,
            bucket=output_bucket,
            object='export/{task}/block_date={datestamp}/{task}.{file_format}'.format(
                task=task, datestamp='{{ds}}', file_format=file_format),
            dag=dag
        )

        def load_task(ds, **kwargs):
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

            # Load from
            export_location_uri = 'gs://{bucket}/export'.format(bucket=output_bucket)
            date_glob = '*'
            uri = '{export_location_uri}/{task}/{date_glob}.{file_format}'.format(
                export_location_uri=export_location_uri, task=task, date_glob=date_glob,file_format=file_format)
            logging.info('Load from uri: ' + uri)

            # Table name
            table_name = task
            logging.info('Table name: ' + table_name)
            table_ref = client.dataset(dataset_name_raw).table(table_name)

            # Load job
            load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
            submit_bigquery_job(load_job, job_config)
            assert load_job.state == 'DONE'

        load_operator = PythonOperator(
            task_id='load_{task}'.format(task=task),
            python_callable=load_task,
            provide_context=True,
            execution_timeout=timedelta(minutes=30),
            dag=dag
        )

        wait_sensor >> load_operator
        return load_operator

    def add_enrich_tasks(task, time_partitioning_field='timestamp_month', dependencies=None):

        def enrich_task(ds, **kwargs):
            template_context = kwargs.copy()
            template_context['ds'] = ds
            template_context['params'] = environment

            client = Client()

            # Need to use a temporary table because bq query sets field modes to NULLABLE and descriptions to null
            # when writeDisposition is WRITE_TRUNCATE

            # Create a temporary table
            temp_table_name = '{task}_{milliseconds}'.format(task=task, milliseconds=int(round(time.time() * 1000)))
            temp_table_ref = client.dataset(dataset_name_temp).table(temp_table_name)
            table = Table(temp_table_ref)

            description_path = os.path.join(
                dags_folder, 'resources/stages/enrich/descriptions/{task}.txt'.format(task=task))
            table.description = read_file(description_path)
            table.time_partitioning = TimePartitioning(field=time_partitioning_field)
            logging.info('Creating table: ' + json.dumps(table.to_api_repr()))

            schema_path = os.path.join(dags_folder, 'resources/stages/enrich/schemas/{task}.json'.format(task=task))
            schema = read_bigquery_schema_from_file(schema_path)
            table.schema = schema

            table = client.create_table(table)
            assert table.table_id == temp_table_name

            # Query from raw to temporary table
            query_job_config = QueryJobConfig()
            # Finishes faster, query limit for concurrent interactive queries is 50
            query_job_config.priority = QueryPriority.INTERACTIVE
            query_job_config.destination = temp_table_ref

            sql_path = os.path.join(dags_folder, 'resources/stages/enrich/sqls/{task}.sql'.format(task=task))
            sql_template = read_file(sql_path)
            sql = kwargs['task'].render_template('', sql_template, template_context)
            print('Enrichment sql:')
            print(sql)

            query_job = client.query(sql, location='US', job_config=query_job_config)
            submit_bigquery_job(query_job, query_job_config)
            assert query_job.state == 'DONE'

            if load_all_partitions:
                # Copy temporary table to destination
                copy_job_config = CopyJobConfig()
                copy_job_config.write_disposition = 'WRITE_TRUNCATE'

                dest_table_name = '{task}'.format(task=task)
                dest_table_ref = client.dataset(dataset_name, project=destination_dataset_project_id).table(
                    dest_table_name)
                copy_job = client.copy_table(temp_table_ref, dest_table_ref, location='US', job_config=copy_job_config)
                submit_bigquery_job(copy_job, copy_job_config)
                assert copy_job.state == 'DONE'
            else:
                # Merge
                # https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
                merge_job_config = QueryJobConfig()
                # Finishes faster, query limit for concurrent interactive queries is 50
                merge_job_config.priority = QueryPriority.INTERACTIVE

                merge_sql_path = os.path.join(dags_folder, 'resources/stages/enrich/sqls/merge_{task}.sql'.format(task=task))
                merge_sql_template = read_file(merge_sql_path)
                template_context['params']['source_table'] = temp_table_name
                merge_sql = kwargs['task'].render_template('', merge_sql_template, template_context)
                print('Merge sql:')
                print(merge_sql)
                merge_job = client.query(merge_sql, location='US', job_config=merge_job_config)
                submit_bigquery_job(merge_job, merge_job_config)
                assert merge_job.state == 'DONE'

            # Delete temp table
            client.delete_table(temp_table_ref)

        enrich_operator = PythonOperator(
            task_id='enrich_{task}'.format(task=task),
            python_callable=enrich_task,
            provide_context=True,
            execution_timeout=timedelta(minutes=60),
            dag=dag
        )

        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> enrich_operator
        return enrich_operator

    def add_create_view_tasks(task, dependencies=None):
        def create_view_task(ds, **kwargs):

            template_context = kwargs.copy()
            template_context['ds'] = ds
            template_context['params'] = environment

            client = Client()

            dest_table_name = '{task}'.format(task=task)
            dest_table_ref = client.dataset(dataset_name, project=destination_dataset_project_id).table(dest_table_name)
            table = Table(dest_table_ref)

            sql_path = os.path.join(dags_folder, 'resources/stages/enrich/sqls/{task}.sql'.format(task=task))
            sql_template = read_file(sql_path)
            sql = kwargs['task'].render_template('', sql_template, template_context)
            table.view_query = sql

            description_path = os.path.join(
                dags_folder, 'resources/stages/enrich/descriptions/{task}.txt'.format(task=task))
            table.description = read_file(description_path)
            logging.info('Creating view: ' + json.dumps(table.to_api_repr()))

            try:
                table = client.create_table(table)
            except Conflict:
                # https://cloud.google.com/bigquery/docs/managing-views
                table = client.update_table(table, ['view_query'])
            assert table.table_id == dest_table_name

        enrich_operator = PythonOperator(
            task_id='create_view_{task}'.format(task=task),
            python_callable=create_view_task,
            provide_context=True,
            execution_timeout=timedelta(minutes=60),
            dag=dag
        )

        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> enrich_operator
        return enrich_operator

    def add_verify_tasks(task, dependencies=None):
        # The queries in verify/sqls will fail when the condition is not met
        # Have to use this trick since the Python 2 version of BigQueryCheckOperator doesn't support standard SQL
        # and legacy SQL can't be used to query partitioned tables.
        sql_path = os.path.join(dags_folder, 'resources/stages/verify/sqls/{task}.sql'.format(task=task))
        sql = read_file(sql_path)
        verify_task = BigQueryOperator(
            task_id='verify_{task}'.format(task=task),
            sql=sql,
            use_legacy_sql=False,
            params=environment,
            dag=dag)
        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> verify_task
        return verify_task

    load_blocks_task = add_load_tasks('blocks', 'json')
    load_transactions_task = add_load_tasks('transactions', 'json')

    enrich_blocks_task = add_enrich_tasks(
        'blocks', time_partitioning_field='timestamp_month', dependencies=[load_blocks_task])
    enrich_transactions_task = add_enrich_tasks(
        'transactions', time_partitioning_field='block_timestamp_month', dependencies=[load_transactions_task])

    create_view_inputs_task = add_create_view_tasks('inputs', dependencies=[enrich_transactions_task])
    create_view_outputs_task = add_create_view_tasks('outputs', dependencies=[enrich_transactions_task])

    verify_blocks_count_task = add_verify_tasks('blocks_count', [enrich_blocks_task])
    verify_blocks_have_latest_task = add_verify_tasks('blocks_have_latest', [enrich_blocks_task])
    verify_transactions_count_task = add_verify_tasks('transactions_count',
                                                      [enrich_blocks_task, enrich_transactions_task])
    verify_transactions_have_latest_task = add_verify_tasks('transactions_have_latest', [enrich_transactions_task])
    verify_transactions_fees_task = add_verify_tasks('transactions_fees', [enrich_transactions_task])

    verify_coinbase_transactions_count_task = add_verify_tasks('coinbase_transactions_count',
                                                               [enrich_blocks_task, enrich_transactions_task])
    verify_transaction_inputs_count_task = add_verify_tasks('transaction_inputs_count',
                                                            [enrich_transactions_task])
    verify_transaction_outputs_count_task = add_verify_tasks('transaction_outputs_count',
                                                             [enrich_transactions_task])

    verify_transaction_inputs_count_empty_task = None
    verify_transaction_outputs_count_empty_task = None
    # Zcash and Dash can have empty inputs and outputs if transaction has join-splits
    if chain != 'zcash' and chain != 'dash':
        verify_transaction_inputs_count_empty_task = add_verify_tasks('transaction_inputs_count_empty',
                                                                      [enrich_transactions_task])
        verify_transaction_outputs_count_empty_task = add_verify_tasks('transaction_outputs_count_empty',
                                                                       [enrich_transactions_task])

    return dag


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


def read_bigquery_schema_from_file(filepath):
    file_content = read_file(filepath)
    json_content = json.loads(file_content)
    return read_bigquery_schema_from_json_recursive(json_content)


def read_file(filepath):
    with open(filepath) as file_handle:
        content = file_handle.read()
        return content


def read_bigquery_schema_from_json_recursive(json_schema):
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
                fields=read_bigquery_schema_from_json_recursive(field.get('fields'))
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
