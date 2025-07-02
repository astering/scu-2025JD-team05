from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
}

SPARK_SCRIPT_PATH = "airflow/dags/node2/scripts/users_feature_kmeans_auto.py"

with DAG(
    dag_id='user_feature_kmeans_auto_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["spark", "dw_users", "kmeans"]
) as dag:

    start = EmptyOperator(task_id='start')

    kmeans_cluster_task = SparkSubmitOperator(
        task_id='run_user_kmeans_auto',
        application=SPARK_SCRIPT_PATH,
        conn_id='spark_default',
        name='user_feature_kmeans_auto',
        application_args=[],
        conf={'spark.executor.memory': '2g'},
        executor_cores=2,
        executor_memory='2g',
        num_executors=2,
        verbose=True
    )

    end = EmptyOperator(task_id='end')

    start >> kmeans_cluster_task >> end
