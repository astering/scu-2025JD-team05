import sys
import json
import urllib.parse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, sum as _sum
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("""
        Usage: top_artists_to_mysql.py <track_path> <person_path> <jdbc_url> <user> <password>
        """, file=sys.stderr)
        sys.exit(-1)

    track_path, person_path, jdbc_url, user, password = sys.argv[1:]

    spark = SparkSession.builder \
        .appName("Top 100 Artists by Playcount") \
        .getOrCreate()

    try:
        # 读取并解析 track.idomaar
        def parse_track_line(line):
            try:
                parts = line.strip().split("\t")
                if len(parts) != 5 or parts[0] != "track":
                    return None
                track_id = int(parts[1])
                payload = json.loads(parts[3])
                meta = json.loads(parts[4])
                playcount = int(payload.get("playcount", 0))
                artists = meta.get("artists", [])
                artist_id = int(artists[0]["id"]) if artists and "id" in artists[0] else -1
                return (track_id, playcount, artist_id)
            except Exception:
                return None

        track_rdd = spark.sparkContext.textFile(track_path).map(parse_track_line).filter(lambda x: x is not None)

        track_df = spark.createDataFrame(track_rdd, schema=StructType([
            StructField("track_id", LongType(), True),
            StructField("playcount", LongType(), True),
            StructField("artist_id", LongType(), True)
        ]))

        # 读取并解析 persons.idomaar
        def parse_person_line(line):
            try:
                parts = line.strip().split("\t")
                if len(parts) != 5 or parts[0] != "person":
                    return None
                artist_id = int(parts[1])
                payload = json.loads(parts[3])
                name_raw = payload.get("name", "")
                name_decoded = urllib.parse.unquote(name_raw)
                return (artist_id, name_decoded)
            except Exception:
                return None

        person_rdd = spark.sparkContext.textFile(person_path).map(parse_person_line).filter(lambda x: x is not None)

        person_df = spark.createDataFrame(person_rdd, schema=StructType([
            StructField("artist_id", LongType(), True),
            StructField("artist_name", StringType(), True)
        ]))

        # 聚合播放量
        joined_df = track_df.join(person_df, on="artist_id", how="left")

        top_artist_df = joined_df.groupBy("artist_id", "artist_name") \
            .agg(_sum("playcount").alias("total_playcount")) \
            .orderBy(desc("total_playcount")) \
            .limit(100)

        # 写入 MySQL
        top_artist_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "top_artist") \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "com.mysql.jdbc.Driver") \
            .mode("overwrite") \
            .save()

        print("? Successfully wrote top 100 artists to MySQL.")

    except Exception as e:
        print(f"? Error: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    spark.stop()
