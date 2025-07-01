import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, explode, avg, max, min, sum
from pyspark.sql import Window
from pyspark.sql.functions import row_number, lit, when

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
        .appName(f"Analyse Music to MySQL") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        # 2. 从 DW Hive 表读取数据
        dw_df_msd = spark.table(dw_table_name)
        dw_df_fm = spark.table(dw_table_name2)
        print(f"Successfully read data from DW table: {dw_table_name}")

        # 3. 分析
        # 连接msd和fm两张表，保留track_id相同的部分
        joined_df = dw_df_msd.join(dw_df_fm, on="track_id", how="inner") \
            .select("track_id", "year", "tags")

        # 展开tags数组，每个tag变成一行
        exploded_df = joined_df.withColumn("tag_struct", explode("tags")) \
            .select("track_id", "year", col("tag_struct.tag").alias("tag"), col("tag_struct.value").alias("value"))

        # 归一化
        exploded_df = exploded_df.withColumn("value", col("value") / 100)

        # 按year和tag聚合，统计每个tag在每年中的总和
        # 先按year, tag聚合
        tag_sum_df = exploded_df.groupBy("year", "tag").agg(
            sum("value").alias("tag_value_sum")
        )

        # 为每年内的tag按tag_value_sum降序排名
        window_spec = Window.partitionBy("year").orderBy(col("tag_value_sum").desc())
        ranked_df = tag_sum_df.withColumn("rank", row_number().over(window_spec))

        # 标记前9名，其他为others
        tagged_df = ranked_df.withColumn(
            "final_tag",
            when(col("rank") <= 9, col("tag")).otherwise(lit("others"))
        )

        # 合并others
        analyse_music_df = tagged_df.groupBy("year", "final_tag").agg(
            sum("tag_value_sum").alias("tag_value_sum")
        ).withColumnRenamed("final_tag", "tag")

        print("result:")
        analyse_music_df.show()

        # 4. 将结果写入 MySQL
        # 使用 .write.jdbc() 方法
        # mode("overwrite") 会在写入前 TRUNCATE TABLE，这对于结果表来说很常用
        analyse_music_df.write \
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