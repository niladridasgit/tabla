import airflow
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from internal_operators.internal_operator import InternalOperator
import pendulum

queries_bucket = 's3://bucket-path/'

tz = pendulum.timezone('Asia/Calcutta')

default_args = {
    'owner': 'n.d@gmail.com',
    'description': "DAG",
    'start_date': datetime(2020, 12, 22, tzinfo=tz),
    'end_date': datetime(2024, 12, 22, tzinfo=tz),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='DAG',
    default_args=default_args,
    schedule_interval='00 13 * * *'
)

start_node = DummyOperator(
    dag=dag,
    task_id='Task1',
    name="start_node"
)

next_node = InternalOperator(
    task_id="Task2",
    name="next_node",
    query_file='/'.join([queries_bucket, "a/b/c/query1.sql"]),
    dag=dag
)

end_node = InternalOperator(
    task_id="Task3",
    name="end_node",
    query_file=queries_bucket+"a/b/c/query2.sql",
    parameters={'current_date': '{{dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}'},
    dag=dag
)

start_node.set_downstream(next_node)
next_node.set_downstream(end_node)
