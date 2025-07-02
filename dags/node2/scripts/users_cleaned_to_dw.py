from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, trim, lit

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Clean ODS Users") \
        .enableHiveSupport() \
        .getOrCreate()

    input_path = "/user/hive/warehouse/ods_users"
    df = spark.read.parquet(input_path)

    print("=== 原始数据预览 ===")
    df.show(5, truncate=False)

    cleaned_df = df.withColumn(
        "country",
        when(col("country").isNull() | (trim(col("country")) == ""), lit("unknown"))
        .otherwise(col("country"))
    )

    print("=== 清洗后数据预览 ===")
    cleaned_df.show(5, truncate=False)

    # 修改为写入 dw_users 表
    cleaned_df.write.mode("overwrite").saveAsTable("dw_users")

    print("dw_users 表写入完成")
    spark.stop()
