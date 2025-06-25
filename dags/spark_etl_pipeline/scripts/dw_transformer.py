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
            StructField("analysis/songs", ArrayType(
                StructType([
                    StructField("analysis_sample_rate", IntegerType(), True),
                    StructField("audio_md5", StringType(), True),
                    StructField("danceability", DoubleType(), True),
                    StructField("duration", DoubleType(), True),
                    StructField("end_of_fade_in", DoubleType(), True),
                    StructField("energy", DoubleType(), True),
                    StructField("idx_bars_confidence", IntegerType(), True),
                    StructField("idx_bars_start", IntegerType(), True),
                    StructField("idx_beats_confidence", IntegerType(), True),
                    StructField("idx_beats_start", IntegerType(), True),
                    StructField("idx_sections_confidence", IntegerType(), True),
                    StructField("idx_sections_start", IntegerType(), True),
                    StructField("idx_segments_confidence", IntegerType(), True),
                    StructField("idx_segments_loudness_max", IntegerType(), True),
                    StructField("idx_segments_loudness_max_time", IntegerType(), True),
                    StructField("idx_segments_loudness_start", IntegerType(), True),
                    StructField("idx_segments_pitches", IntegerType(), True),
                    StructField("idx_segments_start", IntegerType(), True),
                    StructField("idx_segments_timbre", IntegerType(), True),
                    StructField("idx_tatums_confidence", IntegerType(), True),
                    StructField("idx_tatums_start", IntegerType(), True),
                    StructField("key", IntegerType(), True),
                    StructField("key_confidence", DoubleType(), True),
                    StructField("loudness", DoubleType(), True),
                    StructField("mode", IntegerType(), True),
                    StructField("mode_confidence", DoubleType(), True),
                    StructField("start_of_fade_out", DoubleType(), True),
                    StructField("tempo", DoubleType(), True),
                    StructField("time_signature", IntegerType(), True),
                    StructField("time_signature_confidence", DoubleType(), True),
                    StructField("track_id", StringType(), True)
                ])
            ), True),
            StructField("metadata/artist_terms", ArrayType(StringType()), True),
            StructField("metadata/artist_terms_freq", ArrayType(DoubleType()), True),
            StructField("metadata/artist_terms_weight", ArrayType(DoubleType()), True),
            StructField("metadata/similar_artists", ArrayType(StringType()), True),
            StructField("metadata/songs", ArrayType(
                StructType([
                    StructField("analyzer_version", StringType(), True),
                    StructField("artist_7digitalid", IntegerType(), True),
                    StructField("artist_familiarity", DoubleType(), True),
                    StructField("artist_hotttnesss", DoubleType(), True),
                    StructField("artist_id", StringType(), True),
                    StructField("artist_latitude", DoubleType(), True),
                    StructField("artist_location", StringType(), True),
                    StructField("artist_longitude", DoubleType(), True),
                    StructField("artist_mbid", StringType(), True),
                    StructField("artist_name", StringType(), True),
                    StructField("artist_playmeid", IntegerType(), True),
                    StructField("genre", StringType(), True),
                    StructField("idx_artist_terms", IntegerType(), True),
                    StructField("idx_similar_artists", IntegerType(), True),
                    StructField("release", StringType(), True),
                    StructField("release_7digitalid", IntegerType(), True),
                    StructField("song_hotttnesss", DoubleType(), True),
                    StructField("song_id", StringType(), True),
                    StructField("title", StringType(), True),
                    StructField("track_7digitalid", IntegerType(), True)
                ])
            ), True),
            StructField("musicbrainz/artist_mbtags", ArrayType(StringType()), True),
            StructField("musicbrainz/artist_mbtags_count", ArrayType(IntegerType()), True),
            StructField("musicbrainz/songs", ArrayType(
                StructType([
                    StructField("idx_artist_mbtags", IntegerType(), True),
                    StructField("year", IntegerType(), True)
                ])
            ), True)
        ])

        # 3. 应用业务转换逻辑
        # 使用 from_json 将 json_body 列解析成一个名为 'parsed_json' 的 struct 列
        # 使用 select 和 ".*" 语法将 struct 中的所有字段直接展开为顶级列
        print("Applying transformation logic...")

        dw_df = (ods_df.withColumn("parsed_json", from_json(col("json_body"), music_schema))
                 .select("parsed_json.*"))

        # 4. 数据清洗与处理
        # 自己完成

        dw_df.show(10)

        # 5. 创建 Hive 数据库（如果不存在）
        db_name = dw_table_name.split('.')[0]
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"Database '{db_name}' ensured to exist.")

        # 6. 创建 Hive 表（如果不存在）
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
