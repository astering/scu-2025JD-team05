import sys
import json
import urllib.parse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, explode, lit, floor, when, row_number, udf
)
from pyspark.sql.types import *
from pyspark.sql.window import Window

# 解码URL的简单UDF
decode_udf = lambda s: urllib.parse.unquote(s) if s else "NULL"

if __name__ == "__main__":
    # 修改为接收9个参数（包含mysql用户名、密码、驱动、目标表）
    if len(sys.argv) != 10:
        print("Usage: genre_rank_by_decade_to_mysql.py <events_path> <users_path> <tracks_path> <tags_path> <mysql_url> <mysql_user> <mysql_password> <mysql_driver> <target_table>", file=sys.stderr)
        sys.exit(-1)

    events_path, users_path, tracks_path, tags_path, mysql_url, mysql_user, mysql_password, mysql_driver, target_table = sys.argv[1:]

    spark = SparkSession.builder.appName("GenreRankByDecade").getOrCreate()

    # 1. Load play events
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
        StructField("subjects", ArrayType(StructType([
            StructField("type", StringType()),
            StructField("id", LongType())
        ]))),
        StructField("objects", ArrayType(StructType([
            StructField("type", StringType()),
            StructField("id", LongType())
        ])))
    ])))
    play_events = events_parsed.select(
        col("event_id"),
        (col("timestamp") * 1000).cast("timestamp").alias("event_time"),
        col("payload_json.playtime").alias("playtime"),
        col("meta_json.subjects")[0]["id"].alias("user_id"),
        col("meta_json.objects")[0]["id"].alias("track_id")
    )

    # 2. Load user data and compute birth decade
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

    # 3. Load tracks, extract tag IDs from meta, explode tags, handle empty tags as -1
    track_schema = StructType([
        StructField("event_type", StringType()),
        StructField("track_id", LongType()),
        StructField("timestamp", LongType()),
        StructField("payload", StringType()),
        StructField("meta", StringType())
    ])
    track_meta_schema = StructType([
        StructField("tags", ArrayType(MapType(StringType(), StringType())), True)
    ])
    tracks_df = spark.read.option("delimiter", "\t").schema(track_schema).csv(tracks_path)
    tracks_df = tracks_df.withColumn("meta_json", from_json(col("meta"), track_meta_schema))
    tracks_df = tracks_df.withColumn("tag_structs", when(
        (col("meta_json.tags").isNotNull()) & (col("meta_json.tags") != []),
        col("meta_json.tags")
    ).otherwise(lit([{"type": "tag", "id": "-1"}])))
    tracks_exploded = tracks_df.select(
        "track_id", explode(col("tag_structs")).alias("tag_struct")
    ).select(
        "track_id", col("tag_struct.id").cast(LongType()).alias("tag_id")
    )

    # 4. Load tag id -> tag name mapping from tags.idomaar
    tag_schema = StructType([
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
    tags_df = spark.read.option("delimiter", "\t").schema(tag_schema).csv(tags_path)
    tags_df = tags_df.withColumn("payload_json", from_json(col("payload"), tag_payload_schema))

    # 注册解码UDF
    decode_udf = udf(lambda s: urllib.parse.unquote(s) if s else "-1", StringType())
    tags_df = tags_df.withColumn("tag_name", when(col("tag_id") == -1, lit("-1")).otherwise(decode_udf(col("payload_json.value"))))
    tags_df = tags_df.select(col("tag_id"), col("tag_name").alias("tag"))

    # 5. Join all dataframes
    joined_df = play_events.join(users, "user_id", "inner") \
        .join(tracks_exploded, "track_id", "inner") \
        .join(tags_df, "tag_id", "left") \
        .fillna({"tag": "-1"})

    # 6. Aggregate top tags by birth_decade
    genre_count = joined_df.groupBy("birth_decade", "tag") \
        .count() \
        .withColumnRenamed("count", "play_count")

    window_spec = Window.partitionBy("birth_decade").orderBy(col("play_count").desc())
    top_genres = genre_count.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= 10) \
        .select("birth_decade", "tag", "play_count")

    # 7. Write results to MySQL
    top_genres.write.format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", target_table) \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("driver", mysql_driver) \
        .mode("overwrite") \
        .save()

    spark.stop()
