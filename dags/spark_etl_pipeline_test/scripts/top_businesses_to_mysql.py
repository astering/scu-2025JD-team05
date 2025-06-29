import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    # 需要更多参数来连接 MySQL
    if len(sys.argv) != 7:
        print("""
        Usage: top_businesses_to_mysql.py <dw_table_name> <mysql_url> <mysql_user> <mysql_password> <mysql_driver> <mysql_target_table>
        Example: top_businesses_to_mysql.py dw.business_summary jdbc:mysql://host:port/db_name user password com.mysql.cj.jdbc.Driver top_20_businesses
        """, file=sys.stderr)
        sys.exit(-1)

    dw_table_name = sys.argv[1]
    mysql_url = sys.argv[2]
    mysql_user = sys.argv[3]
    mysql_password = sys.argv[4]
    mysql_driver = sys.argv[5]
    mysql_target_table = sys.argv[6]

    # 1. 创建支持 Hive 的 SparkSession
    spark = SparkSession.builder \
        .appName(f"Top 20 Businesses to MySQL") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        # 2. 从 DW Hive 表读取数据
        dw_df = spark.table(dw_table_name)
        print(f"Successfully read data from DW table: {dw_table_name}")

        # 3. 计算评论数最多的前 20 个商家
        # 选择需要的列，按 review_count 降序排列，取前 20
        top_businesses_df = dw_df.select(
            "business_id",
            "name",
            "city",
            "state",
            "stars",
            "review_count"
        ).orderBy(col("review_count").desc()).limit(20)

        print("Top 20 businesses calculated:")
        top_businesses_df.show()

        # 4. 将结果写入 MySQL
        # 使用 .write.jdbc() 方法
        # mode("overwrite") 会在写入前 TRUNCATE TABLE，这对于结果表来说很常用
        top_businesses_df.write \
            .format("jdbc") \
            .option("url", mysql_url) \
            .option("driver", mysql_driver) \
            .option("dbtable", mysql_target_table) \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .mode("overwrite") \
            .save()

        print(f"Successfully wrote top 20 businesses to MySQL table: {mysql_target_table}")

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    # 5. 停止 SparkSession
    spark.stop()
    print("Process finished successfully.")