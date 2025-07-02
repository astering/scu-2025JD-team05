import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, explode, when, isnan

if __name__ == "__main__":
    # 需要更多参数来连接 MySQL
    if len(sys.argv) != 7:
        print("!!!wrong sys.argv!!!", file=sys.stderr)
        sys.exit(-1)

    dw_table_name = sys.argv[1]
    mysql_url = sys.argv[2]
    mysql_user = sys.argv[3]
    mysql_password = sys.argv[4]
    mysql_driver = sys.argv[5]
    mysql_target_table = sys.argv[6]

    # 1. 创建支持 Hive 的 SparkSession
    # 这里的名字会显示在http://node-master:8088/cluster
    spark = SparkSession.builder \
        .appName(f"{mysql_target_table} to MySQL") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        # 2. 从 DW Hive 表读取数据
        dw_df_msd = spark.table(dw_table_name)
        print(f"Successfully read data from DW table: {dw_table_name}")

        # 3. 分析
        # 先统计每个 artist_id 出现的次数，命名为 artist_count
        artist_count_df = dw_df_msd.groupBy("artist_id").count().withColumnRenamed("count", "artist_count")

        # 选择需要的字段并去重
        result_df = dw_df_msd.select("artist_familiarity", "artist_hotttnesss", "artist_id") \
            .filter(col("artist_id").isNotNull()) \
            .dropDuplicates(["artist_id"])
            # .distinct()

        # 将 artist_count 加入结果
        result_df = result_df.join(artist_count_df, on="artist_id", how="left")

        result_df = result_df.withColumn(
            "artist_familiarity",
            when(isnan(col("artist_familiarity")), 0).otherwise(col("artist_familiarity"))
        ).withColumn(
            "artist_hotttnesss",
            when(isnan(col("artist_hotttnesss")), 0).otherwise(col("artist_hotttnesss"))
        )

        result_df = result_df.orderBy("artist_familiarity", ascending=False)

        print("result:")
        result_df.show()

        # 4. 将结果写入 MySQL
        # 使用 .write.jdbc() 方法
        # mode("overwrite") 会在写入前 TRUNCATE TABLE，这对于结果表来说很常用
        result_df.write \
            .format("jdbc") \
            .option("url", mysql_url) \
            .option("driver", mysql_driver) \
            .option("dbtable", mysql_target_table) \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .mode("overwrite") \
            .save()

        print(f"Successfully wrote result to MySQL table: {mysql_target_table}")

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    # 5. 停止 SparkSession
    spark.stop()
    print("Process finished successfully.")