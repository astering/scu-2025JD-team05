# play_event_dag.py
from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base import BaseHook

# 路径与参数配置
SPARK_SCRIPTS_PATH = "airflow/dags/node2/scripts"
EVENTS_PATH = "hdfs://node-master:9000/mir/ThirtyMusic/relations/events.idomaar"
USERS_PATH = "hdfs://node-master:9000/mir/ThirtyMusic/entities/users.idomaar"
TRACKS_PATH = "hdfs://node-master:9000/mir/ThirtyMusic/entities/tracks.idomaar"
TAGS_PATH = "hdfs://node-master:9000/mir/ThirtyMusic/entities/tags.idomaar"

MYSQL_CONN_ID = "mysql_ads_db2"
MYSQL_TABLE = "play_event"
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

# 读取 MySQL 连接信息
mysql_conn = BaseHook.get_connection(MYSQL_CONN_ID)
mysql_url = f"jdbc:mysql://{mysql_conn.host}:{mysql_conn.port}/{mysql_conn.schema}"
mysql_user = mysql_conn.login
mysql_password = mysql_conn.password

with DAG(
    dag_id="play_event_etl_to_mysql",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Shanghai"),
    catchup=False,
    schedule=None,
    tags=["spark", "mysql", "play_event"]
) as dag:
    start = EmptyOperator(task_id="start")

    load_play_event = SparkSubmitOperator(
        task_id="spark_play_event_to_mysql",
        application=f"{SPARK_SCRIPTS_PATH}/play_event_to_mysql.py",
        conn_id="spark_default",
        application_args=[
            EVENTS_PATH,
            USERS_PATH,
            TRACKS_PATH,
            TAGS_PATH,  # 从 tags.idomaar 提取标签
            mysql_url,
            mysql_user,
            mysql_password,
            MYSQL_DRIVER,
            MYSQL_TABLE
        ],
        name="play_event_etl_{{ ds_nodash }}",
        verbose=True,
        conf={"spark.driver.memory": "4g"},
        executor_cores=2,
        executor_memory="4g",
        num_executors=2
    )

    end = EmptyOperator(task_id="end")

    start >> load_play_event >> end
