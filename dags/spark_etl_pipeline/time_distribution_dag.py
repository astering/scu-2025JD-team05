from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base import BaseHook

# 通用路径配置
SPARK_SCRIPTS_PATH = "airflow/dags/spark_etl_pipeline/scripts"

# 原始任务配置：Top Track
TRACK_PATH = "hdfs://node-master:9000/mir/ThirtyMusic/entities/tracks.idomaar"
PERSON_PATH = "hdfs://node-master:9000/mir/ThirtyMusic/entities/persons.idomaar"
ALBUM_PATH = "hdfs://node-master:9000/mir/ThirtyMusic/entities/albums.idomaar"

# 新任务配置：Playlist（time analysis）
PLAYLIST_PATH = "hdfs://node-master:9000/mir/ThirtyMusic/entities/playlist.idomaar"

# MySQL连接
MYSQL_CONN_ID = "mysql_ads_db2"
MYSQL_DRIVER = "com.mysql.jdbc.Driver"

# 建立连接
mysql_conn = BaseHook.get_connection(MYSQL_CONN_ID)
mysql_jdbc_url = f"jdbc:mysql://{mysql_conn.host}:{mysql_conn.port}/{mysql_conn.schema}"
mysql_user = mysql_conn.login
mysql_password = mysql_conn.password

# 定义 DAG
with DAG(
        dag_id="top_tracks_and_time_distribution_etl",
        start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Shanghai"),
        catchup=False,
        schedule=None,
        tags=["spark", "mysql", "etl"]
) as dag:
    start = EmptyOperator(task_id="start")

    # 任务 1：Top Tracks
    load_top_tracks = SparkSubmitOperator(
        task_id="spark_top_tracks_to_mysql",
        application=f"{SPARK_SCRIPTS_PATH}/top_tracks_to_mysql.py",
        conn_id="spark_default",
        application_args=[
            TRACK_PATH,
            PERSON_PATH,
            ALBUM_PATH,
            mysql_jdbc_url,
            mysql_user,
            mysql_password,
            MYSQL_DRIVER,
            "top_track"
        ],
        name="top_tracks_etl_{{ ds_nodash }}",
        verbose=True,
        conf={"spark.driver.memory": "2g"},
        executor_cores=1,
        executor_memory="2g",
        num_executors=2
    )

    # 任务 2：Time Playlist 分布分析
    load_time_distribution = SparkSubmitOperator(
        task_id="spark_time_playlist_to_mysql",
        application=f"{SPARK_SCRIPTS_PATH}/time_distribution_to_mysql.py",
        conn_id="spark_default",
        application_args=[
            PLAYLIST_PATH,
            mysql_jdbc_url,
            mysql_user,
            mysql_password,
            MYSQL_DRIVER,
            "time_playlist"
        ],
        name="time_distribution_{{ ds_nodash }}",
        verbose=True,
        conf={"spark.driver.memory": "2g"},
        executor_cores=1,
        executor_memory="2g",
        num_executors=2
    )

    end = EmptyOperator(task_id="end")

    # 调度顺序：两条线可以并行运行
    start >> [load_top_tracks, load_time_distribution] >> end
