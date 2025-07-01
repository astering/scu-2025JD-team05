import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_replace
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
            StructField("analysis_sample_rate", StringType(), True),
            StructField("artist_7digitalid", StringType(), True),
            StructField("artist_familiarity", StringType(), True),
            StructField("artist_hotttnesss", StringType(), True),
            StructField("artist_id", StringType(), True),
            StructField("artist_latitude", StringType(), True),
            StructField("artist_location", StringType(), True),
            StructField("artist_longitude", StringType(), True),
            StructField("artist_mbid", StringType(), True),
            StructField("artist_name", StringType(), True),
            StructField("artist_playmeid", StringType(), True),
            StructField("audio_md5", StringType(), True),
            StructField("danceability", StringType(), True),
            StructField("duration", StringType(), True),
            StructField("end_of_fade_in", StringType(), True),
            StructField("energy", StringType(), True),
            StructField("key", StringType(), True),
            StructField("key_confidence", StringType(), True),
            StructField("loudness", StringType(), True),
            StructField("mode", StringType(), True),
            StructField("mode_confidence", StringType(), True),
            StructField("release", StringType(), True),
            StructField("release_7digitalid", StringType(), True),
            StructField("song_hotttnesss", StringType(), True),
            StructField("song_id", StringType(), True),
            StructField("start_of_fade_out", StringType(), True),
            StructField("tempo", StringType(), True),
            StructField("time_signature", StringType(), True),
            StructField("time_signature_confidence", StringType(), True),
            StructField("title", StringType(), True),
            StructField("track_7digitalid", StringType(), True),
            StructField("track_id", StringType(), True),
            StructField("year", StringType(), True)
        ])

        # 3. 应用业务转换逻辑
        print("Applying transformation logic...")

        # 使用 from_json 将 json_body 列解析成一个名为 'parsed_json' 的 struct 列
        parsed_df = ods_df.withColumn("parsed_json", from_json(col("json_body"), music_schema)) \
            .select("parsed_json.*")

        # 4. 数据清洗与处理
        # 手动维护字段类型列表
        int_fields = [
            "analysis_sample_rate", "artist_7digitalid", "artist_playmeid", "key",
            "mode", "release_7digitalid", "time_signature", "track_7digitalid", "year"
        ]
        double_fields = [
            "artist_familiarity", "artist_hotttnesss", "artist_latitude", "artist_longitude",
            "danceability", "duration", "end_of_fade_in", "energy", "key_confidence",
            "loudness", "mode_confidence", "song_hotttnesss", "start_of_fade_out",
            "tempo", "time_signature_confidence"
        ]
        string_fields = [
            "artist_id", "artist_location", "artist_mbid", "artist_name",
            "audio_md5", "release", "song_id", "title", "track_id",
        ]
        # # 其余为 string_fields
        # all_fields = [f.name for f in music_schema.fields]
        # string_fields = [f for f in all_fields if f not in int_fields + double_fields]

        cleaned_df = parsed_df

        # 1. StringType 字段：去除 b'' 包裹
        for field in string_fields:
            cleaned_df = cleaned_df.withColumn(
                field,
                regexp_replace(col(field), r"^b'|'$", r"")
                # regexp_replace(col(field), r"^b'(.*)'$", r"\1")
                # regexp_replace(col(field), r'^b\'(.*)\'$', r"\1")
                # regexp_replace(col(field), r"^\"b'(.*)'\"$", r"\1")
                # regexp_replace(col(field), r'^"b\'(.*)\'"$', r"\1")
            )

        # 2. IntegerType 字段：去除双引号并转为 int
        for field in int_fields:
            cleaned_df = cleaned_df.withColumn(
                field,
                regexp_replace(col(field), r'^"(.*)"$', r"\1").cast("int")
            )

        # 3. DoubleType 字段：去除双引号并转为 double
        for field in double_fields:
            cleaned_df = cleaned_df.withColumn(
                field,
                regexp_replace(col(field), r'^"(.*)"$', r"\1").cast("double")
            )

        dw_df = cleaned_df

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
