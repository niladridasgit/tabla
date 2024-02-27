from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from utils.common.slack_notification_helper import SlackNotification
from hs_pod_operator.notification_hook import NotificationHook
from hs_operators.hs_hive_operator import HSHiveOperator
import pendulum

QUERIES_S3_BUCKET = 's3://bucket-path/'
VAULT_PATH = 'prod/data/airflow/acquisition'
ALERT_CHANNEL = '#acquisition_db'
SLACK_NOTIFY = "@abc"

tz = pendulum.timezone('Asia/Calcutta')

default_args = {
    'owner': 'abc.def@ghi.com',
    'description': "User Acquisition Meta",
    'start_date': datetime(2020, 12, 22, tzinfo=tz),
    'end_date': datetime.strptime('2025-12-31', "%Y-%m-%d"),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'params': {
        'slack_channel_name': ALERT_CHANNEL,
        'dag_failure_message': 'User_Acq_meta failed',
        'dag_success_message': 'User_Acq_meta success'
    },
    'on_failure_callback': NotificationHook(callback=SlackNotification.dag_status_slack_alert).execute
}

dag = DAG(
    dag_id='User_Acquisition_Meta',
    default_args=default_args,
    schedule_interval='00 13 * * *',
    max_active_runs=1,
    catchup=False
)

start_node = DummyOperator(
    dag=dag,
    task_id='Avod_Etl_Start_Node',
    on_success_callback=NotificationHook(
        callback="startNodeSuccess",
        send_slack=True,
        slack_channel=ALERT_CHANNEL,
        slack_notify=SLACK_NOTIFY
    ).execute
)

watched_video_temp_node = HSHiveOperator(
    task_id="Sqlcode1_HiveQueryNode",
    name="SqlCode1_HiveQueryNode",
    vault_path=VAULT_PATH,
    query_file='/'.join([QUERIES_S3_BUCKET, "avod/abc/user_acquisition_meta/watched_video_temp.sql"]),
    env_vars={'run_date': '{{dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}'},
    slack_on_success=False,
    slack_on_failure=True,
    slack_channel=ALERT_CHANNEL,
    slack_notify=SLACK_NOTIFY,
    dag=dag
)

user_acq_meta_node = HSHiveOperator(
    task_id="Sqlcode2_HiveQueryNode",
    name="SqlCode2_HiveQueryNode",
    vault_path=VAULT_PATH,
    query_file='/'.join([QUERIES_S3_BUCKET, "avod/abc/user_acquisition_meta/acquisition_meta.sql"]),
    slack_on_success=True,
    slack_on_failure=True,
    slack_channel=ALERT_CHANNEL,
    slack_notify=SLACK_NOTIFY,
    dag=dag
)

start_node.set_downstream(watched_video_temp_node)
watched_video_temp_node.set_downstream(user_acq_meta_node)
