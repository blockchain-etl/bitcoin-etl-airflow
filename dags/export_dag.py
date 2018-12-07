from __future__ import print_function

import os
from datetime import datetime, timedelta

from airflow import models
from airflow.operators import bash_operator


def get_boolean_env_variable(env_variable_name, default=True):
    raw_env = os.environ.get(env_variable_name)
    if raw_env is None or len(raw_env) == 0:
        return default
    else:
        return raw_env.lower() in ['true', 'yes']


default_dag_args = {
    'depends_on_past': False,
    'start_date': datetime(2009, 1, 3),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

notification_emails = os.environ.get('NOTIFICATION_EMAILS')
if notification_emails and len(notification_emails) > 0:
    default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

# Define a DAG (directed acyclic graph) of tasks.
dag = models.DAG(
    'bitcoinetl_export_dag',
    # Daily at 1am
    schedule_interval='0 1 * * *',
    default_args=default_dag_args)
# miniconda.tar contains Python home directory with bitcoin-etl dependencies install via pip
# Will get rid of this once Google Cloud Composer supports Python 3
install_python3_command = \
    'cp $DAGS_FOLDER/resources/miniconda.tar . && ' \
    'tar xvf miniconda.tar > untar_miniconda.log && ' \
    'PYTHON3=$PWD/miniconda/bin/python3'

setup_command = \
    'set -o xtrace && set -o pipefail && ' + install_python3_command + \
    ' && ' \
    'git clone --branch $BITCOINETL_REPO_BRANCH http://github.com/blockchain-etl/bitcoin-etl && cd bitcoin-etl && ' \
    'export LC_ALL=C.UTF-8 && ' \
    'export LANG=C.UTF-8 && ' \
    'BLOCK_RANGE=$($PYTHON3 bitcoinetl.py get_block_range_for_date -d $EXECUTION_DATE -p $PROVIDER_URI) && ' \
    'BLOCK_RANGE_ARRAY=(${BLOCK_RANGE//,/ }) && START_BLOCK=${BLOCK_RANGE_ARRAY[0]} && END_BLOCK=${BLOCK_RANGE_ARRAY[1]} && ' \
    'EXPORT_LOCATION_URI=gs://$OUTPUT_BUCKET/export && ' \
    'export CLOUDSDK_PYTHON=/usr/local/bin/python'

export_blocks_and_transactions_command = \
    setup_command + ' && ' + \
    'echo $BLOCK_RANGE > blocks_meta.txt && ' \
    '$PYTHON3 bitcoinetl.py export_blocks_and_transactions -b $EXPORT_BATCH_SIZE -w $EXPORT_MAX_WORKERS -s $START_BLOCK -e $END_BLOCK ' \
    '-p $PROVIDER_URI --blocks-output blocks.json --transactions-output transactions_raw.json && ' \
    'gsutil cp blocks.json $EXPORT_LOCATION_URI/blocks/block_date=$EXECUTION_DATE/blocks.json && ' \
    'gsutil cp transactions_raw.json $EXPORT_LOCATION_URI/transactions_raw/block_date=$EXECUTION_DATE/transactions_raw.json && ' \
    'gsutil cp blocks_meta.txt $EXPORT_LOCATION_URI/blocks_meta/block_date=$EXECUTION_DATE/blocks_meta.txt '


output_bucket = os.environ.get('OUTPUT_BUCKET')
if output_bucket is None:
    raise ValueError('You must set OUTPUT_BUCKET environment variable')
provider_uri = os.environ.get('PROVIDER_URI', 'https://mainnet.infura.io/')
provider_uri_archival = os.environ.get('PROVIDER_URI_ARCHIVAL', provider_uri)
BITCOINETL_REPO_BRANCH = os.environ.get('BITCOINETL_REPO_BRANCH', 'master')
dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')
export_max_workers = os.environ.get('EXPORT_MAX_WORKERS', '5')
export_batch_size = os.environ.get('EXPORT_BATCH_SIZE', '10')

# ds is 1 day behind the date on which the run is scheduled, e.g. if the dag is scheduled to run at
# 1am on January 2, ds will be January 1.
environment = {
    'EXECUTION_DATE': '{{ ds }}',
    'BITCOINETL_REPO_BRANCH': BITCOINETL_REPO_BRANCH,
    'PROVIDER_URI': provider_uri,
    'PROVIDER_URI_ARCHIVAL': provider_uri_archival,
    'OUTPUT_BUCKET': output_bucket,
    'DAGS_FOLDER': dags_folder,
    'EXPORT_MAX_WORKERS': export_max_workers,
    'EXPORT_BATCH_SIZE': export_batch_size
}

def add_export_task(toggle, task_id, bash_command, dependencies=None):
    if toggle:
        operator = bash_operator.BashOperator(
            task_id=task_id,
            bash_command=bash_command,
            execution_timeout=timedelta(hours=15),
            env=environment,
            dag=dag
        )
        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                if dependency is not None:
                    dependency >> operator
        return operator
    else:
        return None


export_blocks_and_transactions = get_boolean_env_variable('EXPORT_BLOCKS_AND_TRANSACTIONS', True)

export_blocks_and_transactions_operator = add_export_task(
    export_blocks_and_transactions, 'export_blocks_and_transactions', export_blocks_and_transactions_command)

