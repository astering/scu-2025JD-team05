import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: dw_users_to_mysql.py <mysql_url> <mysql_user> <mysql_password> <mysql_table>")
        sys.exit(1)

    mysql_url = sys.argv[1]
    mysql_user = sys.argv[2]
    mysql_password = sys.argv[3]
    mysql_table = sys.argv[4]
    mysql_driver = "com.mysql.cj.jdbc.Driver"

    spark = SparkSession.builder \
        .appName("DW Users to MySQL") \
        .enableHiveSupport() \
        .getOrCreate()

    # 读取 Hive 表数据（会读取 /user/hive/warehouse/dw_users 下的所有 parquet 文件）
    df = spark.sql("SELECT * FROM dw.dw_users")

    # 去除 register_time 列
    df = df.drop("register_time")

    # 打印前10行
    print("===== 前10行数据如下 =====")
    df.show(10, truncate=False)

    # 写入到 MySQL
    df.write.format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", mysql_table) \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("driver", mysql_driver) \
        .mode("overwrite") \
        .save()

    print("数据成功写入 MySQL 表：", mysql_table)
    spark.stop()
