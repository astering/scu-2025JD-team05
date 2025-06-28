import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, desc, explode, coalesce, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, LongType

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("""
        Usage: top_tracks_to_mysql.py <track_file> <persons_file> <albums_file>
                                       <mysql_url> <mysql_user> <mysql_password>
                                       <mysql_driver> <mysql_target_table>
        """, file=sys.stderr)
        sys.exit(-1)

    track_path = sys.argv[1]
    persons_path = sys.argv[2]
    albums_path = sys.argv[3]
    mysql_url = sys.argv[4]
    mysql_user = sys.argv[5]
    mysql_password = sys.argv[6]
    mysql_driver = sys.argv[7]
    mysql_target_table = sys.argv[8]

    spark = SparkSession.builder \
        .appName("Top Tracks ETL to MySQL") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        # ============ Step 1: 读取 Track 数据 =============
        track_schema = StructType([
            StructField("duration", LongType(), True),
            StructField("playcount", IntegerType(), True),
            StructField("MBID", StringType(), True),
            StructField("name", StringType(), True)
        ])

        track_df = (
            spark.read.option("delimiter", "\t").text(track_path)
            .withColumnRenamed("value", "raw")
            .filter(col("raw").startswith("track"))
        )

        # 拆分列：格式为 track \t track_id \t -1 \t json \t meta_json
        track_df = track_df.selectExpr("split(raw, '\t') as parts") \
            .select(
                col("parts[1]").cast("long").alias("track_id"),
                from_json(col("parts[3]"), track_schema).alias("payload_json"),
                from_json(col("parts[4]"), StructType([
                    StructField("artists", ArrayType(StructType([
                        StructField("type", StringType(), True),
                        StructField("id", LongType(), True)
                    ]))),
                    StructField("albums", ArrayType(StructType([
                        StructField("type", StringType(), True),
                        StructField("id", LongType(), True)
                    ])))
                ])).alias("meta_json")
            )

        track_df = track_df.select(
            "track_id",
            col("payload_json.duration").alias("duration"),
            col("payload_json.playcount").alias("playcount"),
            col("payload_json.MBID").alias("track_mbid"),
            col("payload_json.name").alias("title"),
            col("meta_json.artists")[0]["id"].alias("artist_id"),
            col("meta_json.albums")[0]["id"].alias("album_id")
        )

        # ============ Step 2: 读取 persons.idomaar（艺人信息） ============
        persons_df = (
            spark.read.option("delimiter", "\t").text(persons_path)
            .withColumnRenamed("value", "raw")
            .filter(col("raw").startswith("person"))
        )

        persons_df = persons_df.selectExpr("split(raw, '\t') as parts") \
            .select(
                col("parts[1]").cast("long").alias("artist_id"),
                from_json(col("parts[3]"), StructType([
                    StructField("name", StringType(), True)
                ])).alias("payload_json")
            )

        persons_df = persons_df.select(
            "artist_id",
            col("payload_json.name").alias("artist_name")
        )

        # ============ Step 3: 读取 albums.idomaar（专辑信息） ============
        albums_df = (
            spark.read.option("delimiter", "\t").text(albums_path)
            .withColumnRenamed("value", "raw")
            .filter(col("raw").startswith("album"))
        )

        albums_df = albums_df.selectExpr("split(raw, '\t') as parts") \
            .select(
                col("parts[1]").cast("long").alias("album_id"),
                from_json(col("parts[3]"), StructType([
                    StructField("name", StringType(), True)
                ])).alias("payload_json")
            )

        albums_df = albums_df.select(
            "album_id",
            col("payload_json.name").alias("album_name")
        )

        # ============ Step 4: 合并信息并处理 null =============
        final_df = track_df \
            .join(persons_df, on="artist_id", how="left") \
            .join(albums_df, on="album_id", how="left") \
            .na.fill({
                "track_mbid": "UNKNOWN",
                "title": "UNKNOWN",
                "artist_name": "UNKNOWN",
                "album_name": "UNKNOWN"
            })

        final_df = final_df.selectExpr(
            "cast(track_id as BIGINT)",
            "cast(duration as BIGINT)",
            "cast(playcount as BIGINT)",
            "coalesce(track_mbid, 'UNKNOWN') as track_mbid",
            "coalesce(title, 'UNKNOWN') as title",
            "cast(artist_id as BIGINT)",
            "coalesce(artist_name, 'UNKNOWN') as artist_name",
            "cast(album_id as BIGINT)",
            "coalesce(album_name, 'UNKNOWN') as album_name"
        )

        # ============ Step 5: 获取播放量前100条 ============
        top100_df = final_df.orderBy(desc("playcount")).limit(100)
        # ============ Step 6: 写入 MySQL ============
        top100_df.write \
            .format("jdbc") \
            .option("url", mysql_url) \
            .option("dbtable", mysql_target_table) \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .option("driver", mysql_driver) \
            .mode("overwrite") \
            .save()
        print(" Successfully wrote top 100 tracks to MySQL.")

    except Exception as e:
        print(f" Error occurred: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    spark.stop()
    print(" Top Tracks ETL finished successfully.")