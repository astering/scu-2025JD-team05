from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook

# ==== 常量配置 ====
AIRFLOW_HOME = "airflow"
SPARK_SCRIPTS_PATH = AIRFLOW_HOME + "/dags/node1/scripts"
SPARK_SCRIPT_NAME = "user_image_to_mysql.py"

# HDFS 路径
USER_FILE = "hdfs://node-master:9000/mir/ThirtyMusic/entities/users.idomaar"
SESSION_FILE = "hdfs://node-master:9000/mir/ThirtyMusic/relations/sessions.idomaar"
TRACK_FILE = "hdfs://node-master:9000/mir/ThirtyMusic/entities/tracks.idomaar"

# MySQL 配置
MYSQL_CONN_ID = "mysql_ads_db2"
MYSQL_TARGET_TABLE = "user_image"
MYSQL_DRIVER = "com.mysql.jdbc.Driver"

mysql_conn = BaseHook.get_connection(MYSQL_CONN_ID)
mysql_jdbc_url = f"jdbc:mysql://{mysql_conn.host}:{mysql_conn.port}/{mysql_conn.schema}"
mysql_user = mysql_conn.login
mysql_password = mysql_conn.password

# ==== DAG 定义 ====
with DAG(
    dag_id="user_image_to_mysql",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["spark", "user", "portrait"],
    doc_md="""
    ### 用户画像 DAG

    - 从 users.idomaar 获取基础信息  
    - 从 sessions.idomaar 获取播放总时长  
    - 从 tracks.idomaar 提取歌曲标签喜好  
    画像结果写入 MySQL 表 `user_image`。
    """
) as dag:
    start = EmptyOperator(task_id="start")

    user_image_task = SparkSubmitOperator(
        task_id="spark_load_user_image_to_mysql",
        application=f"{SPARK_SCRIPTS_PATH}/{SPARK_SCRIPT_NAME}",
        conn_id="spark_default",
        application_args=[
            USER_FILE,
            SESSION_FILE,
            TRACK_FILE,
            mysql_jdbc_url,
            mysql_user,
            mysql_password,
            MYSQL_DRIVER,
            MYSQL_TARGET_TABLE
        ],
        conf={"spark.driver.memory": "2g"},
        executor_cores=1,
        executor_memory="2g",
        num_executors=2,
        name="user_image_to_mysql_{{ ds_nodash }}",
        verbose=True,
    )

    end = EmptyOperator(task_id="end")

    start >> user_image_task >> end
