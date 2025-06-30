from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base import BaseHook

SPARK_SCRIPTS_PATH = "airflow/dags/node2/scripts"
TRACK_PATH = "hdfs://node-master:9000/mir/ThirtyMusic/entities/tracks.idomaar"
PERSON_PATH = "hdfs://node-master:9000/mir/ThirtyMusic/entities/persons.idomaar"
ALBUM_PATH = "hdfs://node-master:9000/mir/ThirtyMusic/entities/albums.idomaar"

MYSQL_CONN_ID = "mysql_ads_db2"
MYSQL_TARGET_TABLE = "top_track"
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

# 读取 mysql 连接信息
mysql_conn = BaseHook.get_connection(MYSQL_CONN_ID)
mysql_jdbc_url = f"jdbc:mysql://{mysql_conn.host}:{mysql_conn.port}/{mysql_conn.schema}"
mysql_user = mysql_conn.login
mysql_password = mysql_conn.password

with DAG(
    dag_id="top_tracks_etl_to_mysql",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Shanghai"),
    catchup=False,
    schedule=None,
    tags=["spark", "mysql", "top_tracks"]
) as dag:
    start = EmptyOperator(task_id="start")

    load_top_tracks = SparkSubmitOperator(
        task_id="spark_top_tracks_to_mysql",
        application=f"{SPARK_SCRIPTS_PATH}/top_tracks_to_mysql_node2.py",
        conn_id="spark_default",
        application_args=[
            TRACK_PATH,
            PERSON_PATH,
            ALBUM_PATH,
            mysql_jdbc_url,
            mysql_user,
            mysql_password,
            MYSQL_DRIVER,
            MYSQL_TARGET_TABLE
        ],
        name="top_tracks_etl_{{ ds_nodash }}",
        verbose=True,
        conf={"spark.driver.memory": "2g"},
        executor_cores=1,
        executor_memory="2g",
        num_executors=2
    )

    end = EmptyOperator(task_id="end")

    start >> load_top_tracks >> end
