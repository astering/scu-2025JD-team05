from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
import pendulum

default_args = {
    'start_date': pendulum.datetime(2025, 1, 1, tz="UTC"),
    'catchup': False,
}

# 修改为 Node1 上的脚本路径
SPARK_SCRIPT = "airflow/dags/node1/scripts/user_image_to_mysql.py"  # 确保脚本路径指向正确位置

# HDFS 文件路径
USER_FILE = "hdfs://node-master:9000/mir/ThirtyMusic/entities/users.idomaar"  # HDFS 路径
SESSION_FILE = "hdfs://node-master:9000/mir/ThirtyMusic/relations/sessions.idomaar"  # HDFS 路径

conn = BaseHook.get_connection("mysql_ads_db2")
mysql_url = f"jdbc:mysql://{conn.host}:{conn.port}/{conn.schema}"

with DAG("user_image_to_mysql",
         default_args=default_args,
         schedule=None,
         tags=["spark", "user", "portrait"],
         ) as dag:

    start = EmptyOperator(task_id="start")

    user_image_task = SparkSubmitOperator(
        task_id="spark_user_image_to_mysql",
        application=SPARK_SCRIPT,
        conn_id="spark_default",
        application_args=[
            USER_FILE,
            SESSION_FILE,
            mysql_url,
            conn.login,
            conn.password
        ],
        conf={"spark.driver.memory": "2g"},
        executor_cores=1,
        executor_memory="2g",
        num_executors=2,
        name="user_image_to_mysql",
        verbose=True
    )

    end = EmptyOperator(task_id="end")

    start >> user_image_task >> end
