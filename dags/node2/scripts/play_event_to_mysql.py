# play_event_to_mysql.py
import sys
import os
import json
import urllib.parse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, MapType, ArrayType, TimestampType
from pyspark.sql.functions import lit

# 安全地进行 URL 解码
def decode_url(s):
    try:
        return urllib.parse.unquote(s) if s else "NULL"
    except:
        return "NULL"

if __name__ == "__main__":
    if len(sys.argv) != 10:
        print("""
        Usage: play_event_to_mysql.py <events_path> <users_path> <tracks_path> <lastfm_json_dir> <mysql_url> <mysql_user> <mysql_password> <mysql_driver> <mysql_table>
        """, file=sys.stderr)
        sys.exit(-1)

    events_path = sys.argv[1]
    users_path = sys.argv[2]
    tracks_path = sys.argv[3]
    lastfm_json_dir = sys.argv[4]
    mysql_url = sys.argv[5]
    mysql_user = sys.argv[6]
    mysql_password = sys.argv[7]
    mysql_driver = sys.argv[8]
    mysql_table = sys.argv[9]

    spark = SparkSession.builder.appName("PlayEventETL").getOrCreate()

    decode_udf = udf(decode_url, StringType())

    # schema for events.idomaar
    event_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("event_id", LongType(), True),
        StructField("timestamp", LongType(), True),
        StructField("payload", StringType(), True),
        StructField("meta", StringType(), True)
    ])

    payload_schema = StructType([StructField("playtime", LongType(), True)])
    meta_schema = StructType([
        StructField("subjects", ArrayType(MapType(StringType(), StringType()))),
        StructField("objects", ArrayType(MapType(StringType(), StringType())))
    ])

    # 1. 读取 events
    events_raw = spark.read.option("delimiter", "\t").schema(event_schema).csv(events_path)
    events_df = events_raw.filter(col("event_type") == "event.play") \
        .withColumn("payload_json", from_json(col("payload"), payload_schema)) \
        .withColumn("meta_json", from_json(col("meta"), meta_schema)) \
        .select(
            col("event_id"),
            (col("timestamp") * 1000).cast(TimestampType()).alias("event_time"),
            col("payload_json.playtime").alias("play_time"),
            col("meta_json.subjects")[0]["id"].cast(LongType()).alias("user_id"),
            col("meta_json.objects")[0]["id"].cast(LongType()).alias("track_id")
        )

    # 2. 读取 users
    user_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("user_id", LongType(), True),
        StructField("ignore", IntegerType(), True),
        StructField("payload", StringType(), True)
    ])

    user_payload_schema = StructType([
        StructField("lastfm_username", StringType(), True)
    ])

    users_raw = spark.read.option("delimiter", "\t").schema(user_schema).csv(users_path)
    users_df = users_raw.withColumn("payload_json", from_json(col("payload"), user_payload_schema)) \
        .select(col("user_id"), decode_udf(col("payload_json.lastfm_username")).alias("user_name"))

    # 3. 读取 tracks
    track_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("track_id", LongType(), True),
        StructField("ignore", IntegerType(), True),
        StructField("payload", StringType(), True)
    ])

    track_payload_schema = StructType([
        StructField("name", StringType(), True)
    ])

    tracks_raw = spark.read.option("delimiter", "\t").schema(track_schema).csv(tracks_path)
    tracks_df = tracks_raw.withColumn("payload_json", from_json(col("payload"), track_payload_schema)) \
        .select(col("track_id"), decode_udf(col("payload_json.name")).alias("track_name"))

    # 4. 遍历 lastfm JSON 文件，构造 tag 数据
    import glob
    tag_data = []
    for json_file in glob.glob(os.path.join(lastfm_json_dir, "**/*.json"), recursive=True):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                j = json.load(f)
                title = decode_url(j.get("title", "NULL"))
                tags = [t[0] for t in j.get("tags", []) if t and isinstance(t, (list, tuple)) and len(t) > 0]
                tag_data.append((title, ",".join(tags)))
        except Exception as e:
            print(f"Failed to parse {json_file}: {e}", file=sys.stderr)
            continue

    tag_df = spark.createDataFrame(tag_data, ["track_name", "track_tag"])

    # 5. Join 合并
    final_df = events_df \
        .join(users_df, on="user_id", how="left") \
        .join(tracks_df, on="track_id", how="left") \
        .join(tag_df, on="track_name", how="left") \
        .fillna({"user_name": "NULL", "track_name": "NULL", "track_tag": "NULL"})

    # 6. 写入 MySQL
    final_df.select(
        col("event_id"),
        col("event_time"),
        col("play_time"),
        col("user_id"),
        col("user_name"),
        col("track_id"),
        col("track_name"),
        col("track_tag")
    ).write \
        .format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", mysql_table) \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("driver", mysql_driver) \
        .mode("overwrite") \
        .save()

    print("Play event ETL written to MySQL successfully.")
    spark.stop()
