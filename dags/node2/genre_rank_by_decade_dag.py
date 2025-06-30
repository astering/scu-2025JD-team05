from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
import pendulum

MYSQL_CONN_ID = "mysql_ads_db2"
SPARK_SCRIPT_PATH = "airflow/dags/node2/scripts/genre_rank_by_decade_to_mysql.py"

mysql_conn = BaseHook.get_connection(MYSQL_CONN_ID)
mysql_jdbc_url = f"jdbc:mysql://{mysql_conn.host}:{mysql_conn.port}/{mysql_conn.schema}?useSSL=false&serverTimezone=UTC"
mysql_user = mysql_conn.login
mysql_password = mysql_conn.password
mysql_driver = "com.mysql.cj.jdbc.Driver"
target_table = "top_genres_by_decade"

with DAG(
    dag_id="genre_rank_by_decade_etl",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Shanghai"),
    catchup=False,
    tags=["genre", "decade", "spark"]
) as dag:
    start = EmptyOperator(task_id="start")

    genre_etl = SparkSubmitOperator(
        task_id="run_genre_rank_by_decade",
        application=SPARK_SCRIPT_PATH,
        conn_id="spark_default",
        application_args=[
            "hdfs://node-master:9000/mir/ThirtyMusic/relations/events.idomaar",
            "hdfs://node-master:9000/mir/ThirtyMusic/entities/users.idomaar",
            "hdfs://node-master:9000/mir/ThirtyMusic/entities/tracks.idomaar",
            "hdfs://node-master:9000/mir/ThirtyMusic/entities/tags.idomaar",
            mysql_jdbc_url,
            mysql_user,
            mysql_password,
            mysql_driver,
            target_table
        ],
        name="genre_rank_by_decade_{{ ds_nodash }}",
        conf={"spark.driver.memory": "2g"},
        executor_cores=2,
        executor_memory="2g",
        num_executors=2,
        verbose=True,
    )

    end = EmptyOperator(task_id="end")

    start >> genre_etl >> end
