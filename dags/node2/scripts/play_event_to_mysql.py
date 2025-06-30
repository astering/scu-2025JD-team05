# play_event_to_mysql.py：从 tracks.idomaar 中解析 tag_id，再从 tags.idomaar 获取 tag 名称

import sys
import urllib.parse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, explode, from_unixtime
from pyspark.sql.types import *

# 安全 URL 解码函数
def decode_url(s):
    try:
        return urllib.parse.unquote(s) if s else "NULL"
    except:
        return "NULL"

if __name__ == "__main__":
    if len(sys.argv) != 10:
        print("""
        Usage: play_event_to_mysql.py <events_path> <users_path> <tracks_path> <tags_path> <mysql_url> <mysql_user> <mysql_password> <mysql_driver> <mysql_table>
        """, file=sys.stderr)
        sys.exit(-1)

    events_path, users_path, tracks_path, tags_path, mysql_url, mysql_user, mysql_password, mysql_driver, mysql_table = sys.argv[1:]

    spark = SparkSession.builder.appName("PlayEventETL").getOrCreate()

    decode_udf = udf(decode_url, StringType())

    # 1. 读取事件
    event_schema = StructType([
        StructField("event_type", StringType()),
        StructField("event_id", LongType()),
        StructField("timestamp", LongType()),
        StructField("payload", StringType()),
        StructField("meta", StringType())
    ])
    payload_schema = StructType([StructField("playtime", LongType())])
    meta_schema = StructType([
        StructField("subjects", ArrayType(MapType(StringType(), StringType()))),
        StructField("objects", ArrayType(MapType(StringType(), StringType())))
    ])

    events_raw = spark.read.option("delimiter", "\t").schema(event_schema).csv(events_path)
    events_df = events_raw.filter(col("event_type") == "event.play") \
        .withColumn("payload_json", from_json(col("payload"), payload_schema)) \
        .withColumn("meta_json", from_json(col("meta"), meta_schema)) \
        .withColumn("event_time", from_unixtime(col("timestamp")).cast(TimestampType())) \
        .select(
            col("event_id"),
            col("event_time"),
            col("payload_json.playtime").alias("play_time"),
            col("meta_json.subjects")[0]["id"].cast(LongType()).alias("user_id"),
            col("meta_json.objects")[0]["id"].cast(LongType()).alias("track_id")
        )

    # 2. 读取用户
    user_schema = StructType([
        StructField("event_type", StringType()),
        StructField("user_id", LongType()),
        StructField("ignore", IntegerType()),
        StructField("payload", StringType())
    ])
    user_payload_schema = StructType([StructField("lastfm_username", StringType())])

    users_raw = spark.read.option("delimiter", "\t").schema(user_schema).csv(users_path)
    users_df = users_raw.withColumn("payload_json", from_json(col("payload"), user_payload_schema)) \
        .select("user_id", decode_udf(col("payload_json.lastfm_username")).alias("user_name"))

    # 3. 读取 tracks + tag_id
    track_schema = StructType([
        StructField("event_type", StringType()),
        StructField("track_id", LongType()),
        StructField("ignore", IntegerType()),
        StructField("payload", StringType()),
        StructField("meta", StringType())
    ])
    payload_schema = StructType([StructField("name", StringType())])
    meta_schema = StructType([
        StructField("tags", ArrayType(StructType([
            StructField("type", StringType()),
            StructField("id", LongType())
        ])))
    ])

    tracks_raw = spark.read.option("delimiter", "\t").schema(track_schema).csv(tracks_path)
    tracks_df = tracks_raw \
        .withColumn("payload_json", from_json(col("payload"), payload_schema)) \
        .withColumn("meta_json", from_json(col("meta"), meta_schema)) \
        .select(
            col("track_id"),
            decode_udf(col("payload_json.name")).alias("track_name"),
            col("meta_json.tags").alias("tag_array")
        )

    tag_df = tracks_df.withColumn("tag", explode("tag_array")).select(
        col("track_id"),
        col("track_name"),
        col("tag.id").alias("tag_id")
    ).fillna({"tag_id": -1})

    # 4. 读取 tags.idomaar
    tags_schema = StructType([
        StructField("event_type", StringType()),
        StructField("tag_id", LongType()),
        StructField("ignore", IntegerType()),
        StructField("payload", StringType()),
        StructField("meta", StringType())
    ])
    tag_payload_schema = StructType([
        StructField("value", StringType()),
        StructField("url", StringType())
    ])
    tags_raw = spark.read.option("delimiter", "\t").schema(tags_schema).csv(tags_path)
    tags_df = tags_raw.withColumn("payload_json", from_json(col("payload"), tag_payload_schema)) \
        .select("tag_id", decode_udf(col("payload_json.value")).alias("tag_value"))

    # 5. 组合所有信息
    joined_df = events_df \
        .join(users_df, on="user_id", how="left") \
        .join(tag_df, on="track_id", how="left") \
        .join(tags_df, on="tag_id", how="left") \
        .fillna({"user_name": "NULL", "track_name": "NULL", "tag_value": "NULL"})

    # 6. 写入 MySQL
    joined_df.select(
        "event_id", "event_time", "play_time", "user_id", "user_name", "track_id", "track_name", col("tag_value").alias("track_tag")
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
