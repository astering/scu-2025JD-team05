from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
import pendulum

default_args = {
    'start_date': pendulum.datetime(2025, 1, 1, tz="UTC"),
    'catchup': False,
}

# 修改为你自己的 PySpark 分析脚本路径
SPARK_SCRIPT = "airflow/dags/node2/scripts/event_time_to_mysql.py"

# 事件数据文件路径，支持HDFS或本地文件系统路径
EVENT_FILE = "hdfs://node-master:9000/mir/ThirtyMusic/relations/events.idomaar"

# 从 Airflow 连接配置读取 MySQL 信息
conn = BaseHook.get_connection("mysql_ads_db2")
mysql_url = f"jdbc:mysql://{conn.host}:{conn.port}/{conn.schema}"

with DAG(
    "event_time_analysis_dag",
    default_args=default_args,
    schedule="@daily",  # 每天运行一次
    tags=["spark", "event", "time_analysis"],
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    spark_event_time_task = SparkSubmitOperator(
        task_id="spark_event_time_to_mysql",
        application=SPARK_SCRIPT,
        conn_id="spark_default",
        application_args=[
            EVENT_FILE,
            mysql_url,
            conn.login,
            conn.password,
            "event_time_active_hours",  # MySQL 目标表名
        ],
        conf={"spark.driver.memory": "2g"},
        executor_cores=1,
        executor_memory="2g",
        num_executors=2,
        name="event_time_analysis",
        verbose=True,
    )

    end = EmptyOperator(task_id="end")

    start >> spark_event_time_task >> end
