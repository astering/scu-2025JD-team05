from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
import pendulum

#已修改
default_args = {
    'start_date': pendulum.datetime(2025, 1, 1, tz="Asia/Shanghai"),
    'catchup': False,
}

# Spark 脚本路径（根据实际部署路径修改）
SPARK_SCRIPT = "airflow/dags/node2/scripts/cf_recommend_to_mysql.py"

# 数据路径（HDFS）
EVENTS_PATH = "hdfs://node-master:9000/user/hive/warehouse/dw_events"
LOVE_PATH = "hdfs://node-master:9000/user/hive/warehouse/ods_love"

# 读取 Airflow 中配置的 MySQL 连接
conn = BaseHook.get_connection("mysql_ads_db2")
MYSQL_URL = f"jdbc:mysql://{conn.host}:{conn.port}/{conn.schema}?useSSL=false&serverTimezone=UTC"

with DAG(
    dag_id="cf_recommend_to_mysql_dag",
    description="协同过滤推荐结果写入 MySQL",
    default_args=default_args,
    schedule_interval="@daily",  # 每天运行一次
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
