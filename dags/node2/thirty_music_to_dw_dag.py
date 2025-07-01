from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
import pendulum

default_args = {
    'start_date': pendulum.datetime(2025, 1, 1, tz="UTC"),
    'catchup': False,
}

ENTITIES_BASE_PATH = "hdfs://node-master:9000/mir/ThirtyMusic/entities"
RELATIONS_BASE_PATH = "hdfs://node-master:9000/mir/ThirtyMusic/relations"

SPARK_SCRIPT = "airflow/dags/node2/scripts/thirtymusic_to_dw.py"

with DAG(
    "thirtymusic_to_dw",
    default_args=default_args,
    schedule=None,
    tags=["spark", "thirtymusic", "dw"],
) as dag:

    start = EmptyOperator(task_id="start")

    etl_task = SparkSubmitOperator(
        task_id="spark_thirtymusic_to_dw",
        application=SPARK_SCRIPT,
        conn_id="spark_default",
        application_args=[
            ENTITIES_BASE_PATH,
            RELATIONS_BASE_PATH,
        ],
        conf={"spark.driver.memory": "4g"},
        executor_cores=2,
        executor_memory="4g",
        num_executors=3,
        name="thirtymusic_to_dw",
        verbose=True,
    )

    end = EmptyOperator(task_id="end")

    start >> etl_task >> end

