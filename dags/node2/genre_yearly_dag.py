from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base import BaseHook

# 脚本路径
SPARK_SCRIPTS_PATH = "airflow/dags/node2/scripts"

# 数据路径
EVENT_PATH = "hdfs://node-master:9000/mir/ThirtyMusic/relations/events.idomaar"
TRACK_PATH = "hdfs://node-master:9000/mir/ThirtyMusic/entities/tracks.idomaar"
TAGS_PATH = "hdfs://node-master:9000/mir/ThirtyMusic/entities/tags.idomaar"

# MySQL 配置
MYSQL_CONN_ID = "mysql_ads_db2"
MYSQL_TARGET_TABLE = "genre_yearly_stat"
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

# 从 Airflow connection 中获取 MySQL 配置
mysql_conn = BaseHook.get_connection(MYSQL_CONN_ID)
mysql_jdbc_url = f"jdbc:mysql://{mysql_conn.host}:{mysql_conn.port}/{mysql_conn.schema}?useSSL=false&serverTimezone=UTC"
mysql_user = mysql_conn.login
mysql_password = mysql_conn.password

# 定义 DAG
with DAG(
        dag_id="genre_yearly_etl_to_mysql",
        start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Shanghai"),
        catchup=False,
        schedule=None,
        tags=["spark", "mysql", "genre"]
) as dag:
    start = EmptyOperator(task_id="start")

    genre_stat_task = SparkSubmitOperator(
        task_id="spark_genre_yearly_to_mysql",
        application=f"{SPARK_SCRIPTS_PATH}/genre_yearly_to_mysql.py",
        conn_id="spark_default",
        application_args=[
            EVENT_PATH,
            TRACK_PATH,
            TAGS_PATH,
            mysql_jdbc_url,
            mysql_user,
            mysql_password,
            MYSQL_DRIVER,
            MYSQL_TARGET_TABLE
        ],
        name="genre_yearly_etl_{{ ds_nodash }}",
        verbose=True,
        conf={"spark.driver.memory": "2g"},
        executor_cores=1,
        executor_memory="2g",
        num_executors=2
    )

    end = EmptyOperator(task_id="end")

    start >> genre_stat_task >> end
