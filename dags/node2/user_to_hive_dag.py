from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# 默认参数
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
}

# 指定 Spark 脚本路径（容器/主机中）
SPARK_SCRIPT_PATH = "airflow/dags/node2/scripts/user_to_hive.py"

# 创建 DAG
with DAG(
    dag_id='user_to_hive_dag',
    default_args=default_args,
    schedule_interval=None,  # 可改为 '0 3 * * *' 表示每天 3 点运行
    catchup=False,
    tags=["spark", "user", "hive", "ods", "dw"]
) as dag:

    # 起始任务
    start = EmptyOperator(task_id='start')

    # SparkSubmitOperator 执行用户处理脚本
    user_to_hive_task = SparkSubmitOperator(
        task_id='spark_user_to_hive',
        application=SPARK_SCRIPT_PATH,
        conn_id='spark_default',
        name='user_to_hive_job',
        application_args=["/mir/ThirtyMusic/entities/users.idomaar"],
        conf={'spark.executor.memory': '2g'},
        executor_cores=2,
        executor_memory='2g',
        num_executors=2,
        verbose=True
    )

    # 结束任务
    end = EmptyOperator(task_id='end')

    # 定义任务依赖
    start >> user_to_hive_task >> end
