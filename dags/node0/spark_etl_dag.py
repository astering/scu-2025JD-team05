# from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base import BaseHook

# --- 1. 定义常量和变量 ---
# 确保这些路径对于 Airflow Worker 是可访问的
# 如果脚本在 HDFS 上，使用 hdfs:///... 路径

# 启动airflow时终端的路径即为根路径，一般为家目录
# AIRFLOW_HOME = "airflow" # 以启动终端时的目录为基，前后不能有斜杠
# SPARK_SCRIPTS_PATH = AIRFLOW_HOME + "/dags/node0/scripts"
# 或者干脆写成：
SPARK_SCRIPTS_PATH = "airflow/dags/node0/scripts"

HDFS_RAW_DATA_PATH = "hdfs://node-master:9000/mir/millionsongsubset" # 末尾不能有斜杠
LOCAL_FILE_DATA_PATH = "~/mir/millionsongsubset" # 末尾不能有斜杠
FILE_NAME = "msd_summary_file.h5" # 前面不能有斜杠

# Hive 数据库和表名
ODS_DB = "ods"
DW_DB = "dw"

ODS_TABLE = "ods_music"
DW_TABLE = "dw_music"

ODS_TABLE_FQN = f"{ODS_DB}.{ODS_TABLE}"  # FQN: Fully Qualified Name
DW_TABLE_FQN = f"{DW_DB}.{DW_TABLE}"

MYSQL_CONN_ID = "mysql_ads_db2"
MYSQL_DRIVER = "com.mysql.jdbc.Driver"

# 分析结果存入mysql的表名，不能重复，否则覆盖旧版本
# MYSQL_TARGET_TABLE = "top_20_businesses"
# MYSQL_TARGET_TABLE = "sum_music"
MYSQL_TARGET_TABLE = "something"

# 通过hook获取mysql连接信息
mysql_conn = BaseHook.get_connection(MYSQL_CONN_ID)
# 拼接参数
mysql_jdbc_url = f"jdbc:mysql://{mysql_conn.host}:{mysql_conn.port}/{mysql_conn.schema}"
mysql_user = mysql_conn.login
mysql_password = mysql_conn.password

with DAG(
        # dag_id="spark_etl_hdfs_to_hive_dw",
        dag_id="spark_etl_mir_data_load",
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        catchup=False,
        schedule=None,  # 或者 "0 2 * * *" 表示每天凌晨2点运行
        tags=["spark", "etl", "hive"],
        doc_md="""
    ### Spark ETL Pipeline

    This DAG demonstrates a complete ETL process using Spark on YARN.
    1. **Upload to HDFS**: Simulates uploading raw data to HDFS.
    2. **Load to ODS**: Runs a Spark job to load data from HDFS into an ODS Hive table.
    3. **Transform to DW**: Runs another Spark job to transform ODS data and load it into a DW Hive table.
    """,
) as dag:
    start = EmptyOperator(task_id="start")

    # --- 2. 准备数据并上传到 HDFS ---
    # 这个任务演示了数据提取和上传的过程
    # 它会从本地文件目录上传文件到 HDFS，并确保每次运行时都是一样的

    # upload_to_hdfs = BashOperator(
    #     task_id="upload_source_data_to_hdfs",
    #     bash_command=f"""
    #         # 确保 HDFS 目标目录存在，并清理旧文件
    #         hdfs dfs -mkdir -p {HDFS_RAW_DATA_PATH}
    #         hdfs dfs -rm -f {HDFS_RAW_DATA_PATH}/{FILE_NAME}

    #         # 上传新文件到 HDFS
    #         hdfs dfs -put {LOCAL_FILE_DATA_PATH}/{FILE_NAME} {HDFS_RAW_DATA_PATH}/{FILE_NAME}
    #     """,
    # )

    # --- 3. 运行 Spark 作业加载数据到 ODS 层 ---
    ods_load_spark_job = SparkSubmitOperator(
        task_id="spark_load_to_ods_hive",
        conn_id="spark_default",  # 引用在 Airflow UI 中配置的连接
        application=f"{SPARK_SCRIPTS_PATH}/ods_loader_h5.py",
        application_args=[f"{HDFS_RAW_DATA_PATH}/{FILE_NAME}", ODS_TABLE_FQN],
        # Spark 应用的配置
        conf={"spark.driver.memory": "2g"},
        executor_cores=1,
        executor_memory="2g",
        num_executors=2,
        name="ods_load_{{ ds_nodash }}",  # Spark 应用的名称
        verbose=True,  # 在 Airflow 日志中打印 Spark Driver 的日志
    )

    # --- 4. 运行 Spark 作业转换数据到 DW 层 ---
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

    # 5. 处理分析dw层数据，存入mysql
    # mission_name = 'analyse_music'
    # load_result_to_mysql = SparkSubmitOperator(
    #     task_id=f"spark_{mission_name}_to_mysql",
    #     conn_id="spark_default",
    #     application=f"{SPARK_SCRIPTS_PATH}/{mission_name}.py",
    #     #传入新脚本需要的参数
    #     application_args=[
    #         DW_TABLE_FQN,
    #         mysql_jdbc_url,
    #         mysql_user,
    #         mysql_password,
    #         MYSQL_DRIVER,
    #         MYSQL_TARGET_TABLE
    #     ],
    #     name=f"{mission_name}_to_mysql_{{ ds_nodash }}",
    #     verbose=True,
    # )

    end = EmptyOperator(task_id="end")

    # --- 5. 定义任务依赖关系 ---
    start >> ods_load_spark_job >> dw_transform_spark_job >> end
    # start >> upload_to_hdfs >> ods_load_spark_job >> dw_transform_spark_job >> end
    # start >> upload_to_hdfs >> ods_load_spark_job >> dw_transform_spark_job >> load_result_to_mysql >> end
    # start >> load_sum_music_to_mysql >> end
