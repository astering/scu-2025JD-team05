from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
import pendulum

default_args = {
    'start_date': pendulum.datetime(2025, 1, 1, tz="UTC"),
    'catchup': False,
}

# 脚本路径：读取 Hive 表 dw_users，去除 register_time，写入 MySQL
SPARK_SCRIPT = "airflow/dags/node2/scripts/user_to_mysql.py"

# 获取 MySQL 连接信息
conn = BaseHook.get_connection("mysql_ads_db2")
mysql_url = f"jdbc:mysql://{conn.host}:{conn.port}/{conn.schema}"

with DAG("dw_users_to_mysql",
         default_args=default_args,
         schedule=None,
         tags=["spark", "dw_users", "mysql"],
         ) as dag:

    start = EmptyOperator(task_id="start")

    dw_users_task = SparkSubmitOperator(
        task_id="spark_dw_users_to_mysql",
        application=SPARK_SCRIPT,
        conn_id="spark_default",
        application_args=[
            mysql_url,
            conn.login,
            conn.password,
            "dw_users_cleaned"
        ],
        conf={"spark.driver.memory": "2g"},
        executor_cores=1,
        executor_memory="2g",
        num_executors=2,
        name="dw_users_to_mysql",
        verbose=True
    )

    end = EmptyOperator(task_id="end")

    start >> dw_users_task >> end
