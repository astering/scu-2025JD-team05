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

# Hive 数据库和表名
DW_DB = "dw"
DW_TABLE = "dw_music_msd"
DW_TABLE2 = "dw_music_fm"
DW_TABLE_FQN = f"{DW_DB}.{DW_TABLE}"
DW_TABLE_FQN2 = f"{DW_DB}.{DW_TABLE2}"
MYSQL_CONN_ID = "mysql_ads_db2"
MYSQL_DRIVER = "com.mysql.jdbc.Driver"

# 分析结果存入mysql的表名，不能重复，否则覆盖旧版本
MYSQL_TARGET_TABLE = "song_similarity2"

# 通过hook获取mysql连接信息
mysql_conn = BaseHook.get_connection(MYSQL_CONN_ID)
# 拼接参数
mysql_jdbc_url = f"jdbc:mysql://{mysql_conn.host}:{mysql_conn.port}/{mysql_conn.schema}"
mysql_user = mysql_conn.login
mysql_password = mysql_conn.password

with DAG(
        # dag_id="spark_etl_hdfs_to_hive_dw",
        dag_id="spark_etl_dw_to_mysql"+"3",
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        catchup=False,
        schedule=None,  # 或者 "0 2 * * *" 表示每天凌晨2点运行
        tags=["spark", "etl", "hive", "mysql"],
        doc_md="spark_etl_dw_to_mysql, analyse_music, load_result_to_mysql",
) as dag:
    start = EmptyOperator(task_id="start")

    # 5. 处理分析dw层数据，存入mysql
    mission_name = MYSQL_TARGET_TABLE

    load_result_to_mysql = SparkSubmitOperator(
        task_id=f"spark_{mission_name}_to_mysql",
        conn_id="spark_default",
        application=f"{SPARK_SCRIPTS_PATH}/{mission_name}.py",
        #传入新脚本需要的参数
        application_args=[
            DW_TABLE_FQN,
            DW_TABLE_FQN2,
            mysql_jdbc_url,
            mysql_user,
            mysql_password,
            MYSQL_DRIVER,
            MYSQL_TARGET_TABLE
        ],
        name=f"{mission_name}_to_mysql_{{ ds_nodash }}",
        verbose=True,
    )

    end = EmptyOperator(task_id="end")

    # --- 5. 定义任务依赖关系 ---
    start >> load_result_to_mysql >> end
