import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, explode, when, isnan

if __name__ == "__main__":
    # 需要更多参数来连接 MySQL
    if len(sys.argv) != 8:
        print("!!!wrong sys.argv!!!", file=sys.stderr)
        sys.exit(-1)

    dw_table_name = sys.argv[1]
    dw_table_name2 = sys.argv[2]
    mysql_url = sys.argv[3]
    mysql_user = sys.argv[4]
    mysql_password = sys.argv[5]
    mysql_driver = sys.argv[6]
    mysql_target_table = sys.argv[7]

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
        dw_df_fm = spark.table(dw_table_name2)
        print(f"Successfully read data from DW table: {dw_table_name2}")

        # 3. 分析
        # 连接msd和fm两张表，保留track_id相同的部分
        joined_df = dw_df_msd.join(dw_df_fm, on="track_id", how="inner") \
            .select("track_id",
                    dw_df_fm["title"].alias("title"),
                    "artist",
                    "release",
                    "song_hotttnesss",
                    "year",
                    "similars")

        # 过滤空行
        result_df = joined_df.filter(col("similars").isNotNull() & (size(col("similars")) > 0))

        # 将 song_hotttnesss 为 NaN 的值替换为 0
        result_df = result_df.withColumn(
            "song_hotttnesss",
            when(isnan(col("song_hotttnesss")), 0).otherwise(col("song_hotttnesss"))
        )

        # 将similars列展开为多行，并分离为两列
        result_df = result_df.withColumn("similar", explode(col("similars"))) \
            .withColumn("similar_track_id", col("similar.track_id")) \
            .withColumn("similar_score", col("similar.score")) \
            .drop("similars", "similar")

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