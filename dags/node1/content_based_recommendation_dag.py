from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
import pendulum

default_args = {
    'start_date': pendulum.datetime(2025, 1, 1, tz="Asia/Shanghai"),
    'catchup': False,
}

SPARK_SCRIPT = "airflow/dags/node1/scripts/content_based_recommendation_to_mysql.py"
LOVE_FILE = "hdfs://node-master:9000/mir/ThirtyMusic/relations/love.idomaar"
TRACK_FILE = "hdfs://node-master:9000/mir/ThirtyMusic/entities/tracks.idomaar"

conn = BaseHook.get_connection("mysql_ads_db2")
mysql_url = f"jdbc:mysql://{conn.host}:{conn.port}/{conn.schema}"

with DAG("content_based_recommendation_to_mysql",
         default_args=default_args,
         schedule=None,
         tags=["spark", "recommendation", "content_based"]) as dag:

    start = EmptyOperator(task_id="start")

    content_rec_task = SparkSubmitOperator(
        task_id="spark_content_based_recommendation",
        application=SPARK_SCRIPT,
        conn_id="spark_default",
        application_args=[
            LOVE_FILE,
            TRACK_FILE,
            mysql_url,
            conn.login,
            conn.password
        ],
        conf={"spark.driver.memory": "2g"},
        executor_cores=1,
        executor_memory="2g",
        num_executors=2,
        name="content_based_recs",
        verbose=True
    )

    end = EmptyOperator(task_id="end")

    start >> content_rec_task >> end
