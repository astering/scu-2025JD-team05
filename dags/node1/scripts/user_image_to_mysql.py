import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, explode, collect_list
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType

if __name__ == "__main__":
    if len(sys.argv) != 9:
        print("""
        Usage: user_image_to_mysql.py <user_file> <session_file> <track_file> <mysql_url> <mysql_user> <mysql_password> <mysql_driver> <mysql_target_table>
        """, file=sys.stderr)
        sys.exit(-1)

    user_file = sys.argv[1]
    session_file = sys.argv[2]
    track_file = sys.argv[3]
    mysql_url = sys.argv[4]
    mysql_user = sys.argv[5]
    mysql_password = sys.argv[6]
    mysql_driver = sys.argv[7]
    mysql_target_table = sys.argv[8]

    spark = SparkSession.builder \
        .appName("User Image to MySQL") \
        .getOrCreate()

    try:
        # 1. 用户信息
        def parse_user(line):
            try:
                parts = line.split("\t")
                if len(parts) < 4:
                    return None
                user_id = int(parts[1])
                props = json.loads(parts[3])
                age = int(props.get("age")) if props.get("age") else None
                gender = props.get("gender", "")
                return (user_id, age, gender)
            except:
                return None

        user_rdd = spark.sparkContext.textFile(user_file).map(parse_user).filter(lambda x: x)
        user_df = spark.createDataFrame(user_rdd, schema=StructType([
            StructField("user_id", IntegerType(), True),
            StructField("age", IntegerType(), True),
            StructField("gender", StringType(), True),
        ]))
        user_df.show(5, truncate=False)

        # 2. Session 汇总
        def parse_session(line):
            try:
                parts = line.split("\t")
                if len(parts) < 5:
                    return None
                user_json = json.loads(parts[4])
                play_json = json.loads(parts[3])

                user_id = int(user_json["subjects"][0]["id"])
                playtime = int(play_json.get("playtime", 0))
                return (user_id, playtime) if playtime > 0 else None
            except:
                return None

        session_rdd = spark.sparkContext.textFile(session_file).map(parse_session).filter(lambda x: x)
        session_df = spark.createDataFrame(session_rdd, schema=StructType([
            StructField("user_id", IntegerType(), True),
            StructField("session_time", IntegerType(), True),
        ]))

        session_agg_df = session_df.groupBy("user_id").agg(
            count("*").alias("session_count"),
            _sum("session_time").alias("total_play_time"),
            avg("session_time").alias("avg_session_time")
        ).withColumn(
            "user_type",
            (col("total_play_time") > 7200).cast("string")
        ).replace({"true": "重度", "false": "轻度"}, subset=["user_type"])

        session_agg_df.show(5, truncate=False)

        # 3. track标签
        def parse_track(line):
            try:
                parts = line.split("\t")
                if len(parts) < 5:
                    return None
                track_id = int(parts[1])
                tags = json.loads(parts[4]).get("tags", [])
                tag_names = [t["value"] for t in tags if "value" in t]
                return (track_id, tag_names) if tag_names else None
            except:
                return None

        track_rdd = spark.sparkContext.textFile(track_file).map(parse_track).filter(lambda x: x)
        track_df = spark.createDataFrame(track_rdd, schema=StructType([
            StructField("track_id", IntegerType(), True),
            StructField("tags", ArrayType(StringType()), True),
        ]))
        track_df.show(5, truncate=False)

        # 4. user-track对应
        def extract_user_track(line):
            try:
                parts = line.split("\t")
                user_id = int(json.loads(parts[4])["subjects"][0]["id"])
                objects = json.loads(parts[4])["objects"]
                return [(user_id, int(obj["id"])) for obj in objects if obj["type"] == "track"]
            except:
                return []

        user_track_rdd = spark.sparkContext.textFile(session_file).flatMap(extract_user_track).filter(lambda x: x)
        user_track_df = spark.createDataFrame(user_track_rdd, schema=StructType([
            StructField("user_id", IntegerType(), True),
            StructField("track_id", IntegerType(), True),
        ]))
        user_track_df.show(5, truncate=False)

        # 5. 用户标签画像
        user_tag_df = user_track_df.join(track_df, on="track_id", how="left") \
            .select("user_id", explode("tags").alias("tag")) \
            .groupBy("user_id", "tag").count() \
            .orderBy("user_id", col("count").desc())

        top_tags_df = user_tag_df.groupBy("user_id").agg(
            collect_list("tag").alias("tag_list")
        ).withColumn("top_tags", col("tag_list").cast("string"))

        top_tags_df.show(5, truncate=False)

        # 6. 汇总最终画像
        final_df = user_df.join(session_agg_df, on="user_id", how="left") \
                          .join(top_tags_df.select("user_id", "top_tags"), on="user_id", how="left")

        final_df.show(10, truncate=False)

        # 7. 写入MySQL
        final_df.write.format("jdbc") \
            .option("url", mysql_url) \
            .option("dbtable", mysql_target_table) \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .option("driver", mysql_driver) \
            .mode("overwrite") \
            .save()

        print("写入成功！")

    except Exception as e:
        print(f"出错了：{e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    spark.stop()
