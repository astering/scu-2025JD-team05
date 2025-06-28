from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, hour, when, col, count
import sys

if __name__ == "__main__":
    # 参数读取
    playlist_path = sys.argv[1]
    jdbc_url = sys.argv[2]
    jdbc_user = sys.argv[3]
    jdbc_password = sys.argv[4]
    jdbc_driver = sys.argv[5]
    target_table = sys.argv[6]

    spark = SparkSession.builder \
        .appName("time_distribution_playlist") \
        .getOrCreate()

    # 读取 idomaar 数据
    raw_df = spark.read.text(playlist_path)

    # 解析 JSON，每行是一条事件
    json_df = raw_df.selectExpr("CAST(value AS STRING) AS json_str") \
        .selectExpr("from_json(json_str, 'struct[timestamp:long]') AS data") \
        .select("data.timestamp")

    # 转换为小时
    time_df = json_df.withColumn("hour", hour(from_unixtime(col("timestamp") / 1000)))

    # 分类统计四个时段
    distribution_df = time_df.withColumn(
        "time_range",
        when((col("hour") >= 0) & (col("hour") < 6), "00-06")
        .when((col("hour") >= 6) & (col("hour") < 12), "06-12")
        .when((col("hour") >= 12) & (col("hour") < 18), "12-18")
        .otherwise("18-24")
    ).groupBy("time_range").agg(count("*").alias("count"))

    # 写入 MySQL
    distribution_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", target_table) \
        .option("user", jdbc_user) \
        .option("password", jdbc_password) \
        .option("driver", jdbc_driver) \
        .mode("overwrite") \
        .save()

    spark.stop()
