import sys
import urllib.parse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_unixtime, year, size, when, array, create_map, lit, explode, from_json, count, row_number, udf
)
from pyspark.sql.types import *
from pyspark.sql.window import Window

# 用于解码标签
def decode_url(s):
    try:
        return urllib.parse.unquote(s) if s else "-1"
    except:
        return "-1"

decode_udf = udf(decode_url, StringType())

if __name__ == "__main__":
    if len(sys.argv) != 9:
        print("""
        Usage: genre_yearly_to_mysql.py <events_path> <tracks_path> <tags_path> <mysql_url> <mysql_user> <mysql_password> <mysql_driver> <target_table>
        """, file=sys.stderr)
        sys.exit(-1)

    (events_path, tracks_path, tags_path,
     mysql_url, mysql_user, mysql_password, mysql_driver, target_table) = sys.argv[1:]

    spark = SparkSession.builder.appName("GenreRankByYear").getOrCreate()

    # === 1. Load events data and extract event year ===
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
        .withColumn("event_time", from_unixtime(col("timestamp"))) \
        .withColumn("event_year", year(from_unixtime(col("timestamp")))) \
        .withColumn("payload_json", from_json(col("payload"), payload_schema)) \
        .withColumn("meta_json", from_json(col("meta"), meta_schema)) \
        .select(
            "event_id", "event_year",
            col("payload_json.playtime").alias("play_time"),
            col("meta_json.subjects")[0]["id"].cast(LongType()).alias("user_id"),
            col("meta_json.objects")[0]["id"].cast(LongType()).alias("track_id")
        )

    # === 2. Load track data and extract tag_ids ===
    track_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("track_id", LongType(), True),
        StructField("ignore", IntegerType(), True),
        StructField("payload", StringType(), True),
        StructField("meta", StringType(), True)
    ])

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

    # === 3. Load tags and decode tag names ===
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

    # === 4. Join all ===
    joined_df = events_df \
        .join(track_df, on="track_id", how="left") \
        .join(tag_df, on="tag_id", how="left") \
        .fillna({"tag": "-1"})

    # === 5. Group by year + tag and count ===
    genre_count = joined_df.groupBy("event_year", "tag") \
        .agg(count("*").alias("play_count"))

    # === 6. Rank top 3 per year ===
    window_spec = Window.partitionBy("event_year").orderBy(col("play_count").desc())

    top_genres = genre_count.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= 3) \
        .select("event_year", "tag", "play_count")

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

    print("Genre top 3 per year successfully written to MySQL.")
    spark.stop()
