from __future__ import print_function

import os
from datetime import timedelta

from airflow import DAG
from airflow.operators import bash_operator


def build_export_dag(
        dag_id,
        provider_uri,
        output_bucket,
        export_start_date,
        chain='bitcoin',
        notification_emails=None,
        export_schedule_interval='0 0 * * *',
        bitcoinetl_repo_branch='master',
        export_max_workers=5,
        export_batch_size=10,
        export_max_active_runs=15
):
    export_max_workers = str(export_max_workers)
    export_batch_size = str(export_batch_size)
    default_dag_args = {
        'depends_on_past': False,
        'start_date': export_start_date,
        'email_on_failure': True,
        'email_on_retry': True,
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    dag = DAG(
        dag_id,
        schedule_interval=export_schedule_interval,
        max_active_runs=export_max_active_runs,
        default_args=default_dag_args)

    setup_command = \
        'set -o xtrace && set -o pipefail &&' \
        'git clone --branch $BITCOINETL_REPO_BRANCH http://github.com/blockchain-etl/bitcoin-etl && cd bitcoin-etl && ' \
        'export LC_ALL=C.UTF-8 && ' \
        'export LANG=C.UTF-8 && ' \
        'BLOCK_RANGE=$(python3 bitcoinetl.py get_block_range_for_date -d $EXECUTION_DATE -p $PROVIDER_URI) && ' \
        'BLOCK_RANGE_ARRAY=(${BLOCK_RANGE//,/ }) && START_BLOCK=${BLOCK_RANGE_ARRAY[0]} && END_BLOCK=${BLOCK_RANGE_ARRAY[1]} && ' \
        'EXPORT_LOCATION_URI=gs://$OUTPUT_BUCKET/export && ' \
        'export CLOUDSDK_PYTHON=/usr/bin/python2'

    export_blocks_and_transactions_command = \
        setup_command + ' && ' + \
        'echo $BLOCK_RANGE > blocks_meta.txt && ' \
        'python3 bitcoinetl.py export_blocks_and_transactions -c $CHAIN -b $EXPORT_BATCH_SIZE -w $EXPORT_MAX_WORKERS -s $START_BLOCK -e $END_BLOCK ' \
        '-p $PROVIDER_URI --blocks-output blocks.json --transactions-output transactions.json && ' \
        'python3 bitcoinetl.py filter_items -i blocks.json -o blocks_filtered.json ' \
        '-p "datetime.datetime.fromtimestamp(item[\'timestamp\']).astimezone(datetime.timezone.utc).strftime(\'%Y-%m-%d\') == \'$EXECUTION_DATE\'" && ' \
        'python3 bitcoinetl.py filter_items -i transactions.json -o transactions_filtered.json ' \
        '-p "datetime.datetime.fromtimestamp(item[\'block_timestamp\']).astimezone(datetime.timezone.utc).strftime(\'%Y-%m-%d\') == \'$EXECUTION_DATE\'" && ' \
        'gsutil cp blocks_filtered.json $EXPORT_LOCATION_URI/blocks/block_date=$EXECUTION_DATE/blocks.json && ' \
        'gsutil cp transactions_filtered.json $EXPORT_LOCATION_URI/transactions/block_date=$EXECUTION_DATE/transactions.json && ' \
        'gsutil cp blocks_meta.txt $EXPORT_LOCATION_URI/blocks_meta/block_date=$EXECUTION_DATE/blocks_meta.txt '

    dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')

    # ds is 1 day behind the date on which the run is scheduled, e.g. if the dag is scheduled to run at
    # 1am on January 2, ds will be January 1.
    environment = {
        'CHAIN': chain,
        'EXECUTION_DATE': '{{ ds }}',
        'BITCOINETL_REPO_BRANCH': bitcoinetl_repo_branch,
        'PROVIDER_URI': provider_uri,
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

    add_export_task(True, 'export_blocks_and_transactions', export_blocks_and_transactions_command)

    return dag
