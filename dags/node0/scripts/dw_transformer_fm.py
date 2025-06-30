import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
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
            StructField("similars", ArrayType(ArrayType(
                # Each element is [track_id, score]
                # track_id: string, score: double
                StructType([
                    StructField("0", StringType(), True),
                    StructField("1", DoubleType(), True)
                ])
            )), True),
            StructField("tags", ArrayType(ArrayType(
                # Each element is [tag, value]
                # tag: string, value: string (could be int, but sample is string)
                StructType([
                    StructField("0", StringType(), True),
                    StructField("1", IntegerType(), True)
                ])
            )), True),
            StructField("track_id", StringType(), True),
            StructField("title", StringType(), True)
        ])

        # 3. 应用业务转换逻辑
        # 使用 from_json 将 json_body 列解析成一个名为 'parsed_json' 的 struct 列
        print("Applying transformation logic...")

        # 先解析
        parsed_df = ods_df.withColumn("parsed_json", from_json(col("json_body"), music_schema))

        # 获取所有字段名，将斜杠替换为下划线
        old_fields = parsed_df.select("parsed_json.*").schema.names
        new_fields = [f.replace("/", "_") for f in old_fields]

        # 构造 select 语句，重命名所有字段
        select_exprs = [
            f"parsed_json.`{old}` as `{new}`" if old != new else f"parsed_json.`{old}`"
            for old, new in zip(old_fields, new_fields)
        ]

        dw_df = parsed_df.selectExpr(*select_exprs)

        # 4. 数据清洗与处理
        # 自己完成

        dw_df.show(10)

        # 5. 创建 Hive 数据库（如果不存在）
        db_name = dw_table_name.split('.')[0]
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"Database '{db_name}' ensured to exist.")

        # 6. 创建 Hive 表（如果不存在）
        # 下面sql表内容不重要，会被覆写
        create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {dw_table_name} (
                    business_id      string,
                    name             string,
                    address          string,
                    city             string,
                    state            string,
                    postal_code  string,
                    latitude     float,
                    longitude    float,
                    stars        float,
                    review_count int,
                    is_open      tinyint,
                    attributes   string,
                    categories   string,
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
