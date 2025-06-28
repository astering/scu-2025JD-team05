import sys
import urllib.parse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, LongType
from pyspark.sql.window import Window

if __name__ == "__main__":
    if len(sys.argv) != 9:
        print("""
        Usage: top_tracks_to_mysql.py <track_path> <person_path> <album_path> <mysql_url> <mysql_user> <mysql_password> <mysql_driver> <target_table>
        """, file=sys.stderr)
        sys.exit(-1)

    track_path = sys.argv[1]
    person_path = sys.argv[2]
    album_path = sys.argv[3]
    mysql_url = sys.argv[4]
    mysql_user = sys.argv[5]
    mysql_password = sys.argv[6]
    mysql_driver = sys.argv[7]
    target_table = sys.argv[8]

    spark = SparkSession.builder.appName("TopTracksETL").getOrCreate()

    decode_udf = udf(lambda s: urllib.parse.unquote(s) if s else s, StringType())

    # === 1. schema ===
    track_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("track_id", LongType(), True),
        StructField("ignore", IntegerType(), True),
        StructField("payload", StringType(), True),
        StructField("meta", StringType(), True)
    ])

    json_payload_schema = StructType([
        StructField("duration", LongType(), True),
        StructField("playcount", LongType(), True),
        StructField("MBID", StringType(), True),
        StructField("name", StringType(), True)
    ])

    json_meta_schema = StructType([
        StructField("artists", ArrayType(StructType([
            StructField("id", LongType(), True),
            StructField("type", StringType(), True)
        ]))),
        StructField("albums", ArrayType(StructType([
            StructField("id", LongType(), True),
            StructField("type", StringType(), True)
        ]))),
        StructField("tags", ArrayType(StringType()))
    ])

    person_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("id", LongType(), True),
        StructField("ignore", IntegerType(), True),
        StructField("payload", StringType(), True),
        StructField("meta", StringType(), True)
    ])

    album_schema = person_schema

    # === 2. load and parse track.idomaar ===
    track_raw = spark.read.option("delimiter", "\t").schema(track_schema).csv(track_path)
    track_df = track_raw \
        .withColumn("payload_json", from_json(col("payload"), json_payload_schema)) \
        .withColumn("meta_json", from_json(col("meta"), json_meta_schema)) \
        .select(
            "track_id",
            col("payload_json.duration").alias("duration"),
            col("payload_json.playcount").alias("playcount"),
            col("payload_json.MBID").alias("track_mbid"),
            col("payload_json.name").alias("title"),
            col("meta_json.artists")[0]["id"].alias("artist_id"),
            col("meta_json.albums")[0]["id"].alias("album_id")
        )

    # === 3. 去重，track_id 相同时保留 playcount 最大的 ===
    window_dedup = Window.partitionBy("track_id").orderBy(col("playcount").desc())
    track_df = track_df.withColumn("row_num", row_number().over(window_dedup)).filter("row_num = 1").drop("row_num")

    # === 4. 取前 100 ===
    window_spec = Window.orderBy(col("playcount").desc())
    top_tracks = track_df.withColumn("rank", row_number().over(window_spec)).filter("rank <= 100").drop("rank")

    # === 5. 读取 person 和 album 映射 ===
    def extract_name(df, id_col="id"):
        return df.withColumn("payload_json", from_json(col("payload"), StructType([
            StructField("MBID", StringType(), True),
            StructField("name", StringType(), True)
        ]))).select(
            col(id_col),
            col("payload_json.name").alias("name")
        )

    person_raw = spark.read.option("delimiter", "\t").schema(person_schema).csv(person_path)
    album_raw = spark.read.option("delimiter", "\t").schema(album_schema).csv(album_path)

    person_df = extract_name(person_raw, "id").withColumnRenamed("name", "artist_name")
    album_df = extract_name(album_raw, "id").withColumnRenamed("name", "album_name")

    # === 6. URL 解码字段 ===
    person_df = person_df.withColumn("artist_name", decode_udf(col("artist_name")))
    top_tracks = top_tracks.withColumn("title", decode_udf(col("title")))

    # === 7. Join 三表 ===
    final_df = top_tracks \
        .join(person_df, top_tracks.artist_id == person_df.id, how="left").drop(person_df.id) \
        .join(album_df, top_tracks.album_id == album_df.id, how="left").drop(album_df.id)

    # 明确字段类型
    final_df = final_df.selectExpr(
        "cast(track_id as BIGINT)",
        "cast(duration as BIGINT)",
        "cast(playcount as BIGINT)",
        "cast(track_mbid as STRING)",
        "cast(title as STRING)",
        "cast(artist_id as BIGINT)",
        "cast(artist_name as STRING)",
        "cast(album_id as BIGINT)",
        "cast(album_name as STRING)"
    )

    # === 8. 写入 MySQL ===
    final_df.write \
        .format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", target_table) \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("driver", mysql_driver) \
        .mode("overwrite") \
        .save()

    print("Top tracks successfully written to MySQL.")
    spark.stop()
    print("Process finished successfully.")
