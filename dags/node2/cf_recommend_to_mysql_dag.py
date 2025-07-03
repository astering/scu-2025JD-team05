from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
import pendulum

default_args = {
    'start_date': pendulum.datetime(2025, 1, 1, tz="Asia/Shanghai"),
    'catchup': False,
}

# Spark 脚本路径
SPARK_SCRIPT = "airflow/dags/node2/scripts/cf_recommend_to_mysql.py"

# 数据路径（HDFS）
EVENTS_PATH = "hdfs://node-master:9000/user/hive/warehouse/dw_events"
LOVE_PATH = "hdfs://node-master:9000/user/hive/warehouse/ods_love"
TRACKS_PATH = "hdfs://node-master:9000/user/hive/warehouse/ods_tracks"
USERS_PATH = "hdfs://node-master:9000/user/hive/warehouse/dw_users"

# 获取 MySQL 连接信息
conn = BaseHook.get_connection("mysql_ads_db2")
MYSQL_URL = f"jdbc:mysql://{conn.host}:{conn.port}/{conn.schema}?useSSL=false&serverTimezone=UTC"

with DAG(
    dag_id="cf_recommend_to_mysql_dag",
    description="协同过滤推荐结果写入 MySQL（含用户名与歌曲名）",
    default_args=default_args,
    schedule_interval="@daily",
    tags=["spark", "recommend", "mysql"],
) as dag:

    start = EmptyOperator(task_id="start")

    spark_submit = SparkSubmitOperator(
        task_id="run_cf_recommend",
        application=SPARK_SCRIPT,
        conn_id="spark_default",
        application_args=[
            EVENTS_PATH,
            LOVE_PATH,
            MYSQL_URL,
            conn.login,
            conn.password,
            TRACKS_PATH,
            USERS_PATH,
        ],
        conf={"spark.driver.memory": "4g"},
        executor_cores=2,
        executor_memory="4g",
        num_executors=3,
        name="cf_recommend_to_mysql_job",
        verbose=True,
    )

    end = EmptyOperator(task_id="end")

    start >> spark_submit >> end
