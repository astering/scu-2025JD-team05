import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType
from pyspark.sql.types import MapType, ArrayType

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: dw_transformer.py <ods_table_name> <dw_table_name>", file=sys.stderr)
        sys.exit(-1)

    ods_table_name = sys.argv[1]  # e.g., 'ods.sales_raw'
    dw_table_name = sys.argv[2]  # e.g., 'dw.sales_summary_daily'

    # 1. 创建支持 Hive 的 SparkSession
    spark = SparkSession.builder \
        .appName(f"DW Transform for {dw_table_name}") \
        .enableHiveSupport() \
        .getOrCreate()

    print(f"Spark Session created for DW transformation.")

    try:
        # 2. 从 ODS Hive 表读取数据
        ods_df = spark.table(ods_table_name)
        print(f"Successfully read data from ODS table: {ods_table_name}")

        music_schema = StructType([
            StructField("artist", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("similars", ArrayType(ArrayType(StringType())), True),  # 或 DoubleType
            StructField("tags", ArrayType(ArrayType(StringType())), True),
            StructField("track_id", StringType(), True),
            StructField("title", StringType(), True)
        ])

        # 3. 应用业务转换逻辑
        # 使用 from_json 将 json_body 列解析成一个名为 'parsed_json' 的 struct 列
        print("Applying transformation logic...")

        # 先解析
        parsed_df = ods_df.withColumn("parsed_json", from_json(col("json_body"), music_schema))

        parsed_df = parsed_df.select("parsed_json.*")

        # 4. 数据清洗与处理
        # 将similars和tags内部数组转为struct数组
        dw_df = parsed_df \
            .withColumn(
                "similars_struct",
                expr("transform(similars, x -> named_struct('track_id', x[0], 'score', cast(x[1] as double)))")
            ) \
            .withColumn(
                "tags_struct",
                expr("transform(tags, x -> named_struct('tag', x[0], 'value', cast(x[1] as integer)))")
            )

        # 你可以选择只保留struct列或保留原始列
        dw_df = dw_df.drop("similars", "tags").withColumnRenamed("similars_struct", "similars").withColumnRenamed("tags_struct", "tags")

        dw_df.show(10)

        # 5. 创建 Hive 数据库（如果不存在）
        db_name = dw_table_name.split('.')[0]
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"Database '{db_name}' ensured to exist.")

        # 6. 创建 Hive 表（如果不存在）
        # 下面sql表内容不重要，会被覆写
        create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {dw_table_name} (
                    latitude     float,
                    review_count int,
                    is_open      tinyint,
                    hours        string
                )
                """
        spark.sql(create_table_sql)
        print(f"Table {dw_table_name} with partitioning ensured to exist.")

        # 7. 将转换后的 DataFrame 写入 DW 表
        dw_df.write.mode("overwrite").saveAsTable(dw_table_name)
        print(f"Successfully wrote transformed data to DW table: {dw_table_name}")

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    # 6. 停止 SparkSession
    spark.stop()
    print("DW transformation process finished successfully.")
