from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base import BaseHook

# --- 1. 常量与路径设定 ---
SPARK_SCRIPTS_PATH = "airflow/dags/node1/scripts"

HDFS_RAW_DATA_PATH = "hdfs://node-master:9000/mir/millionsongsubset"
LOCAL_FILE_DATA_PATH = "~/mir/millionsongsubset"
FILE_NAME = "msd_summary_file.json"

ODS_DB = "ods"
DW_DB = "dw"
ODS_TABLE = "ods_music_msd"
DW_TABLE = "dw_music_msd"
ODS_TABLE_FQN = f"{ODS_DB}.{ODS_TABLE}"
DW_TABLE_FQN = f"{DW_DB}.{DW_TABLE}"

MYSQL_CONN_ID = "mysql_ads_db2"
MYSQL_DRIVER = "com.mysql.jdbc.Driver"
MYSQL_TARGET_TABLE = "something"

mysql_conn = BaseHook.get_connection(MYSQL_CONN_ID)
mysql_jdbc_url = f"jdbc:mysql://{mysql_conn.host}:{mysql_conn.port}/{mysql_conn.schema}"
mysql_user = mysql_conn.login
mysql_password = mysql_conn.password

with DAG(
    dag_id="user_image_midchart",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["spark", "user", "portrait"],
) as dag:

    start = EmptyOperator(task_id="start")

    upload_to_hdfs = BashOperator(
        task_id="upload_source_data_to_hdfs",
        bash_command=f"""
            hdfs dfs -mkdir -p {HDFS_RAW_DATA_PATH}
            hdfs dfs -rm -f {HDFS_RAW_DATA_PATH}/{FILE_NAME}
            hdfs dfs -put {LOCAL_FILE_DATA_PATH}/{FILE_NAME} {HDFS_RAW_DATA_PATH}/{FILE_NAME}
        """,
    )

    ods_load_spark_job = SparkSubmitOperator(
        task_id="spark_load_to_ods_hive",
        conn_id="spark_default",
        application=f"{SPARK_SCRIPTS_PATH}/ods_loader.py",
        application_args=[f"{HDFS_RAW_DATA_PATH}/{FILE_NAME}", ODS_TABLE_FQN],
        conf={"spark.driver.memory": "2g"},
        executor_cores=1,
        executor_memory="2g",
        num_executors=2,
        name="ods_load_{{ ds_nodash }}",
        verbose=True,
    )

    dw_transform_spark_job = SparkSubmitOperator(
        task_id="spark_transform_to_dw_hive",
        conn_id="spark_default",
        application=f"{SPARK_SCRIPTS_PATH}/dw_transformer.py",
        application_args=[ODS_TABLE_FQN, DW_TABLE_FQN],
        conf={"spark.driver.memory": "2g"},
        executor_cores=1,
        executor_memory="2g",
        num_executors=2,
        name="dw_transform_{{ ds_nodash }}",
        verbose=True,
    )

    # 修改：读取新的 session.idomaar 路径，提取 user_id 与 track 信息
    extract_user_image_job = SparkSubmitOperator(
        task_id="extract_user_image_to_txt",
        conn_id="spark_default",
        application=f"{SPARK_SCRIPTS_PATH}/extract_user_image.py",
        application_args=[
            "hdfs://node-master:9000/mir/ThirtyMusic/relations/session.idomaar",
            "hdfs://node-master:9000/user_image.txt"
        ],
        conf={"spark.driver.memory": "2g"},
        executor_cores=1,
        executor_memory="2g",
        num_executors=1,
        name="extract_user_image_{{ ds_nodash }}",
        verbose=True,
    )

    end = EmptyOperator(task_id="end")

    # 任务依赖
    start >> extract_user_image_job >> end
