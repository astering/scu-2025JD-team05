import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: content_based_recommendation_to_mysql.py <love_file> <track_file> <mysql_url> <mysql_user> <mysql_password>", file=sys.stderr)
        sys.exit(1)

    love_file = sys.argv[1]
    track_file = sys.argv[2]
    mysql_url = sys.argv[3]
    mysql_user = sys.argv[4]
    mysql_password = sys.argv[5]
    mysql_table = "content_based_recs"
    mysql_driver = "com.mysql.jdbc.Driver"

    spark = SparkSession.builder \
        .appName("Content Based Recommendation ETL") \
        .getOrCreate()

    def parse_love(line):
        try:
            parts = line.split("\t")
            user_id = json.loads(parts[4])["subjects"][0]["id"]
            track_id = json.loads(parts[4])["objects"][0]["id"]
            return (user_id, track_id)
        except:
            return None

    def parse_track(line):
        try:
            parts = line.split("\t")
            track_id = int(parts[1])
            props = json.loads(parts[3])
            tags = props.get("tags", [])  # Assume list of strings or objects
            return (track_id, tags)
        except:
            return None

    love_rdd = spark.sparkContext.textFile(love_file).map(parse_love).filter(lambda x: x is not None)
    track_rdd = spark.sparkContext.textFile(track_file).map(parse_track).filter(lambda x: x is not None)

    love_schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("track_id", IntegerType(), True),
    ])

    track_schema = StructType([
        StructField("track_id", IntegerType(), True),
        StructField("tags", ArrayType(StringType()), True),
    ])

    love_df = spark.createDataFrame(love_rdd, love_schema)
    track_df = spark.createDataFrame(track_rdd, track_schema)

    # 用户历史喜好：关联track，展开标签
    user_tags_df = love_df.join(track_df, "track_id").select("user_id", explode("tags").alias("tag"))

    # 为每个用户推荐拥有相同标签的其他track
    tag_to_track_df = track_df.select("track_id", explode("tags").alias("tag"))

    recs_df = user_tags_df.join(tag_to_track_df, "tag") \
        .filter(col("track_id") != col("track_id")) \
        .groupBy("user_id", "track_id") \
        .agg(count("*").alias("score")) \
        .orderBy("user_id", col("score").desc())

    # 写入 MySQL
    recs_df.write.format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", mysql_table) \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("driver", mysql_driver) \
        .mode("overwrite") \
        .save()

    print("Content-based recommendation written to MySQL.")
    spark.stop()
