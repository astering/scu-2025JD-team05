from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
}

ENTITIES_BASE_PATH = "hdfs://node-master:9000/mir/ThirtyMusic/entities"
RELATIONS_BASE_PATH = "hdfs://node-master:9000/mir/ThirtyMusic/relations"

SPARK_SCRIPT = "airflow/dags/node2/scripts/parse_love_and_sessions.py"

with DAG(
    dag_id='love_sessions_etl_to_hdfs',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["spark", "love_sessions", "dw"]
) as dag:

    spark_submit_love_sessions = SparkSubmitOperator(
        task_id='parse_love_and_sessions',
        application=SPARK_SCRIPT,
        name='parse_love_sessions',
        conn_id='spark_default',
        application_args=[
            ENTITIES_BASE_PATH,
            RELATIONS_BASE_PATH
        ],
        conf={'spark.executor.memory': '2g'},
        executor_cores=2,
        executor_memory='2g',
        num_executors=2,
        verbose=True
    )

