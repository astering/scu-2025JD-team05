import sys
import json
import urllib.parse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: genre_rank_by_decade.py <events_path> <users_path> <tracks_path> <json_dir>", file=sys.stderr)
        sys.exit(-1)

    events_path, users_path, tracks_path, json_dir = sys.argv[1:5]

    spark = SparkSession.builder.appName("GenreByDecade").getOrCreate()

    # Step 1: 读取 events 数据
    event_schema = StructType([
        StructField("event_type", StringType()),
        StructField("event_id", LongType()),
        StructField("timestamp", LongType()),
        StructField("payload", StringType()),
        StructField("meta", StringType())
    ])
    events_df = spark.read.option("delimiter", "\t").schema(event_schema).csv(events_path)
    events_df = events_df.filter(col("event_type") == "event.play")

    events_parsed = events_df.withColumn("payload_json", from_json(col("payload"), StructType([
        StructField("playtime", LongType())
    ]))).withColumn("meta_json", from_json(col("meta"), StructType([
        StructField("subjects", ArrayType(StructType([StructField("type", StringType()), StructField("id", LongType())]))),
        StructField("objects", ArrayType(StructType([StructField("type", StringType()), StructField("id", LongType())])))
    ])))

    play_events = events_parsed.select(
        col("event_id"),
        (col("timestamp") * 1000).cast("timestamp").alias("event_time"),
        col("payload_json.playtime").alias("playtime"),
        col("meta_json.subjects")[0]["id"].alias("user_id"),
        col("meta_json.objects")[0]["id"].alias("track_id")
    )

    # Step 2: 用户信息
    user_schema = StructType([
        StructField("event_type", StringType()),
        StructField("user_id", LongType()),
        StructField("timestamp", LongType()),
        StructField("payload", StringType()),
        StructField("meta", StringType())
    ])
    users_df = spark.read.option("delimiter", "\t").schema(user_schema).csv(users_path)
    users_df = users_df.withColumn("payload_json", from_json(col("payload"), StructType([
        StructField("age", IntegerType(), True)
    ])))

    users = users_df.select("user_id", col("payload_json.age").alias("age")).filter(col("age").isNotNull())
    users = users.withColumn("birth_decade", (lit(2025) - col("age")) / 10)
    users = users.withColumn("birth_decade", (floor(col("birth_decade")) * 10).cast("int"))

    # Step 3: track_id -> track_name
    track_schema = StructType([
        StructField("event_type", StringType()),
        StructField("track_id", LongType()),
        StructField("timestamp", LongType()),
        StructField("payload", StringType()),
        StructField("meta", StringType())
    ])
    tracks_df = spark.read.option("delimiter", "\t").schema(track_schema).csv(tracks_path)
    tracks_df = tracks_df.withColumn("payload_json", from_json(col("payload"), StructType([
        StructField("name", StringType(), True)
    ])))

    tracks = tracks_df.select("track_id", col("payload_json.name").alias("track_name")).fillna("NULL")
    tracks = tracks.withColumn("track_name", expr("decode(unbase64(translate(track_name, '+', ' ')), 'UTF-8')"))

    # Step 4: 读取所有 JSON 文件（track title -> tags）
    import glob
    all_json_files = glob.glob(os.path.join(json_dir, "**", "*.json"), recursive=True)
    tag_data = []
    for path in all_json_files:
        with open(path, "r", encoding="utf-8") as f:
            try:
                content = json.load(f)
                track_name = urllib.parse.unquote_plus(content.get("title", "NULL"))
                tags = [t[0] for t in content.get("tags", [])]
                for tag in tags:
                    tag_data.append((track_name, tag))
            except:
                pass

    tag_df = spark.createDataFrame(tag_data, ["track_name", "tag"])

    # Step 5: 联合所有数据
    joined = play_events.join(users, "user_id", "inner") \
                        .join(tracks, "track_id", "inner") \
                        .join(tag_df, "track_name", "left")

    genre_count = joined.groupBy("birth_decade", "tag") \
                        .agg(count("*").alias("play_count"))

    window_spec = Window.partitionBy("birth_decade").orderBy(col("play_count").desc())
    top_genres = genre_count.withColumn("rank", row_number().over(window_spec)) \
                            .filter(col("rank") <= 10) \
                            .select("birth_decade", "tag", "play_count")

    top_genres.write.format("jdbc") \
        .option("url", "jdbc:mysql://your_host:3306/your_db") \
        .option("dbtable", "top_genres_by_decade") \
        .option("user", "your_user") \
        .option("password", "your_password") \
        .option("driver", "com.mysql.jdbc.Driver") \
        .mode("overwrite").save()

    spark.stop()
