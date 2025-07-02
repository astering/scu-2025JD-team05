from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
}

SPARK_SCRIPT_PATH = "airflow/dags/node2/scripts/users_cleaned_to_dw.py"

with DAG(
    dag_id='clean_ods_users_to_hive',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["spark", "ods_users", "clean"]
) as dag:

    start = EmptyOperator(task_id='start')

    clean_users_task = SparkSubmitOperator(
        task_id='clean_ods_users_to_dw',
        application=SPARK_SCRIPT_PATH,
        conn_id='spark_default',
        name='clean_ods_users',
        application_args=[],
        conf={'spark.executor.memory': '2g'},
        executor_cores=2,
        executor_memory='2g',
        num_executors=2,
        verbose=True
    )

    end = EmptyOperator(task_id='end')

    start >> clean_users_task >> end
