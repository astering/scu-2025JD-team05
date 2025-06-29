import sys
import os
import json
import urllib.parse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, year, explode, lit, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

def extract_tags_map(base_dir):
    tags_map = {}
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".json"):
                try:
                    with open(os.path.join(root, file), 'r') as f:
                        data = json.load(f)
                        title = urllib.parse.unquote(data.get("title", "NULL"))
                        tags = [t[0] for t in data.get("tags", []) if len(t) > 0]
                        tags_map[title] = tags
                except Exception:
                    continue
    return tags_map

if __name__ == "__main__":
    if len(sys.argv) != 8:
        print("""
        Usage: genre_yearly_to_mysql.py <event_path> <track_path> <lastfm_path> <mysql_url> <mysql_user> <mysql_password> <mysql_driver> <target_table>
        """, file=sys.stderr)
        sys.exit(-1)

    event_path, track_path, lastfm_path, mysql_url, mysql_user, mysql_password, mysql_driver, target_table = sys.argv[1:]

    spark = SparkSession.builder.appName("GenreYearlyStat").getOrCreate()

    decode_udf = udf(lambda s: urllib.parse.unquote(s) if s else "NULL", StringType())

    # === 1. Load events ===
    event_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("event_id", LongType(), True),
        StructField("timestamp", LongType(), True),
        StructField("payload", StringType(), True),
        StructField("meta", StringType(), True)
    ])

    events_df = spark.read.option("delimiter", "\t").schema(event_schema).csv(event_path)
    events_df = events_df.filter(col("event_type") == "event.play")

    # 解析字段
    from pyspark.sql.functions import from_json
    payload_schema = StructType([StructField("playtime", LongType(), True)])
    meta_schema = StructType([
        StructField("subjects",
            StructType([StructField("type", StringType(), True), StructField("id", LongType(), True)])),
        StructField("objects",
            StructType([StructField("type", StringType(), True), StructField("id", LongType(), True)])
    ])


    events_df = events_df \
        .withColumn("event_time", from_unixtime(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("year", year(from_unixtime(col("timestamp")))) \
        .withColumn("payload_json", from_json(col("payload"), payload_schema)) \
        .withColumn("meta_json", from_json(col("meta"), StructType([
            StructField("subjects", ArrayType(StructType([
                StructField("type", StringType(), True),
                StructField("id", LongType(), True)
            ]))),
            StructField("objects", ArrayType(StructType([
                StructField("type", StringType(), True),
                StructField("id", LongType(), True)
            ])))
        ])))

    events_df = events_df.select(
        col("event_id"),
        col("event_time"),
        col("year"),
        col("payload_json.playtime").alias("play_time"),
        col("meta_json.subjects")[0]["id"].alias("user_id"),
        col("meta_json.objects")[0]["id"].alias("track_id")
    )

    # === 2. Load track names ===
    track_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("track_id", LongType(), True),
        StructField("ignore", IntegerType(), True),
        StructField("payload", StringType(), True),
        StructField("meta", StringType(), True)
    ])

    track_df = spark.read.option("delimiter", "\t").schema(track_schema).csv(track_path)
    track_df = track_df.withColumn("payload_json", from_json(col("payload"), StructType([
        StructField("name", StringType(), True)
    ])))
    track_df = track_df.select("track_id", decode_udf(col("payload_json.name")).alias("track_name"))

    # === 3. Load LastFM tags ===
    tags_map = extract_tags_map(lastfm_path)
    tags_rdd = spark.sparkContext.parallelize(tags_map.items())
    tags_df = tags_rdd.toDF(["track_name", "tags"])

    # === 4. Join events with track and tags ===
    events_with_names = events_df.join(track_df, on="track_id", how="left")
    events_with_tags = events_with_names.join(tags_df, on="track_name", how="left")

    exploded_df = events_with_tags.withColumn("tag", explode(col("tags")))

    result_df = exploded_df.groupBy("year", "tag").count().withColumnRenamed("count", "play_count")

    # === 5. 写入 MySQL ===
    result_df.write \
        .format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", target_table) \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("driver", mysql_driver) \
        .mode("overwrite") \
        .save()

    print("Genre by year statistics successfully written to MySQL.")
    spark.stop()
