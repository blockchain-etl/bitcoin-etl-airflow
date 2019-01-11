from datetime import timedelta

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

default_dag_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': airflow.utils.dates.days_ago(0),
    'email_on_failure': True,
}

notification_emails = Variable.get('notification_emails', '')
if notification_emails and len(notification_emails) > 0:
    default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

dag = DAG(
    'liveness_dag',
    default_args=default_dag_args,
    description='liveness monitoring dag',
    schedule_interval=timedelta(minutes=10))

BashOperator(task_id='echo', bash_command='echo test', dag=dag, depends_on_past=False)
