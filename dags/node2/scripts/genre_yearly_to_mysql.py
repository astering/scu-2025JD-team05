import sys
import urllib.parse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_unixtime, size, when, array, create_map, lit, explode, floor,
    row_number, udf, from_json
)
from pyspark.sql.types import *
from pyspark.sql.window import Window

def decode_url(s):
    try:
        return urllib.parse.unquote(s) if s else "-1"
    except:
        return "-1"

decode_udf = udf(decode_url, StringType())

if __name__ == "__main__":
    if len(sys.argv) != 10:
        print("""
        Usage: genre_yearly_to_mysql.py <events_path> <users_path> <tracks_path> <tags_path> <mysql_url> <mysql_user> <mysql_password> <mysql_driver> <target_table>
        """, file=sys.stderr)
        sys.exit(-1)

    (events_path, users_path, tracks_path, tags_path,
     mysql_url, mysql_user, mysql_password, mysql_driver, target_table) = sys.argv[1:]

    spark = SparkSession.builder.appName("GenreRankByDecade").getOrCreate()

    # === 1. Load event data ===
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

    events_df = spark.read.option("delimiter", "\t").schema(event_schema).csv(events_path) \
        .filter(col("event_type") == "event.play") \
        .withColumn("event_time", from_unixtime(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("payload_json", from_json(col("payload"), payload_schema)) \
        .withColumn("meta_json", from_json(col("meta"), meta_schema)) \
        .select(
            "event_id", "event_time",
            col("payload_json.playtime").alias("play_time"),
            col("meta_json.subjects")[0]["id"].cast(LongType()).alias("user_id"),
            col("meta_json.objects")[0]["id"].cast(LongType()).alias("track_id")
        )

    # === 2. Load user data and compute birth decade ===
    user_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("user_id", LongType(), True),
        StructField("ignore", IntegerType(), True),
        StructField("payload", StringType(), True),
        StructField("meta", StringType(), True)
    ])
    user_payload_schema = StructType([
        StructField("age", IntegerType(), True)
    ])

    users_df = spark.read.option("delimiter", "\t").schema(user_schema).csv(users_path) \
        .withColumn("payload_json", from_json(col("payload"), user_payload_schema)) \
        .select("user_id", col("payload_json.age").alias("age")) \
        .filter(col("age").isNotNull())

    users_df = users_df.withColumn("birth_decade", floor((lit(2025) - col("age")) / 10) * 10)

    # === 3. Load track data and extract tag IDs ===
    track_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("track_id", LongType(), True),
        StructField("ignore", IntegerType(), True),
        StructField("payload", StringType(), True),
        StructField("meta", StringType(), True)
    ])

    # 保持tags为 ARRAY<MAP<STRING, STRING>>
    track_meta_schema = StructType([
        StructField("tags", ArrayType(MapType(StringType(), StringType())), True)
    ])

    track_df = spark.read.option("delimiter", "\t").schema(track_schema).csv(tracks_path) \
        .withColumn("meta_json", from_json(col("meta"), track_meta_schema)) \
        .withColumn("tag_ids", when(
            col("meta_json.tags").isNotNull() & (size(col("meta_json.tags")) > 0),
            col("meta_json.tags")
        ).otherwise(
            array(create_map(lit("type"), lit("tag"), lit("id"), lit("-1")))
        )) \
        .withColumn("tag_id", explode(col("tag_ids"))) \
        .select(
            "track_id",
            col("tag_id")["id"].cast(LongType()).alias("tag_id")
        )

    # === 4. Load tag ID → tag name mapping ===
    tag_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("tag_id", LongType(), True),
        StructField("ignore", IntegerType(), True),
        StructField("payload", StringType(), True),
        StructField("meta", StringType(), True)
    ])

    tag_payload_schema = StructType([
        StructField("value", StringType(), True),
        StructField("url", StringType(), True)
    ])

    tag_df = spark.read.option("delimiter", "\t").schema(tag_schema).csv(tags_path) \
        .withColumn("payload_json", from_json(col("payload"), tag_payload_schema)) \
        .select(
            col("tag_id"),
            decode_udf(col("payload_json.value")).alias("tag")
        )

    # === 5. Join events + tracks + tags + users ===
    joined_df = events_df \
        .join(track_df, on="track_id", how="left") \
        .join(tag_df, on="tag_id", how="left") \
        .join(users_df, on="user_id", how="inner") \
        .fillna({"tag": "-1"})

    # === 6. Group by birth_decade + tag and rank top 10 ===
    from pyspark.sql.functions import count
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    genre_count = joined_df.groupBy("birth_decade", "tag") \
        .agg(count("*").alias("play_count"))

    window_spec = Window.partitionBy("birth_decade").orderBy(col("play_count").desc())
    top_genres = genre_count.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= 10) \
        .select("birth_decade", "tag", "play_count")

    # === 7. Write to MySQL ===
    top_genres.write \
        .format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", target_table) \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("driver", mysql_driver) \
        .mode("overwrite") \
        .save()

    print("Genre rank by decade statistics successfully written to MySQL.")
    spark.stop()
