import os
import tempfile
import pandas as pd
import ring
import logging

from functools import reduce
from datetime import timedelta, datetime
from diskcache import Cache

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

log = logging.getLogger(__name__)
storage = Cache(os.path.join(tempfile.gettempdir(), "_my_cache"))
SLACK_CONN_ID = 'slack'

def task_fail_slack_alert(context):
    log.info("Running Slack Alert " + context.get('task_instance').task_id)
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
                task=context.get('task_instance').task_id,
                dag=context.get('task_instance').dag_id,
                ti=context.get('task_instance'),
                exec_date=context.get('execution_date'),
                log_url=context.get('task_instance').log_url)
    failed_alert = SlackWebhookOperator(
        task_id='slack_alert',
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return failed_alert.execute(context=context)


 ############################################################################################
# INITIALIZE DAG CONFIGURATION
###########################################################################################

default_args = dict(
    owner='anton.weiss',
    start_date=datetime(2019, 7, 30),
    retries=1,
    retry_delay=timedelta(seconds=5),
    email_on_failure=True,
    email_on_success=True,
    email_on_retry=False,
    on_failure_callback=task_fail_slack_alert,
    email=['anton.weiss@naturalint.com']
)

dag = DAG(
    description='DAG that test email sending',
    dag_id=os.path.basename(__file__).replace(".pyc", "").replace(".py", ""),
    schedule_interval='10 10 * * *',
    concurrency=48,
    max_active_runs=1,
    catchup=False,
    default_args=default_args
)

my_task = KubernetesPodOperator(
    namespace='default',
    image='alpine',
    cmds=[
        "sleep 30 && exit 1",
    ],
    name="email-test",
    image_pull_secrets="regcred",
    image_pull_policy="Always",
    task_id=f"mytask",
    get_logs=True,
    in_cluster=True,
    is_delete_operator_pod=True,
    dag=dag)

my_other_task = KubernetesPodOperator(
    namespace='default',
    image='alpine',
    cmds=[
        "sleep 5 && exit 0",
    ],
    name="succ_email-test",
    image_pull_secrets="regcred",
    image_pull_policy="Always",
    task_id=f"mytask_good",
    get_logs=True,
    in_cluster=True,
    is_delete_operator_pod=True,
    dag=dag)

my_task >> my_other_task