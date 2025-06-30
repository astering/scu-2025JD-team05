import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, explode, collect_list
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType

if __name__ == "__main__":
    if len(sys.argv) != 8:
        print("""
        Usage: user_image_to_mysql.py <user_file> <session_file> <track_file> <mysql_url> <mysql_user> <mysql_password> <mysql_driver> <mysql_target_table>
        Example: user_image_to_mysql.py users.idomaar sessions.idomaar tracks.idomaar jdbc:mysql://host:port/db_name user password com.mysql.jdbc.Driver user_image
        """, file=sys.stderr)
        sys.exit(-1)

    user_file = sys.argv[1]
    session_file = sys.argv[2]
    track_file = sys.argv[3]
    mysql_url = sys.argv[4]
    mysql_user = sys.argv[5]
    mysql_password = sys.argv[6]
    mysql_driver = sys.argv[7]
    mysql_target_table = "user_image"

    # 1. 创建 SparkSession
    spark = SparkSession.builder \
        .appName("User Image to MySQL") \
        .getOrCreate()

    try:
        # 2. 读取并解析用户信息
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

        user_rdd = spark.sparkContext.textFile(user_file).map(parse_user).filter(lambda x: x is not None)
        user_schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("age", IntegerType(), True),
            StructField("gender", StringType(), True),
        ])
        user_df = spark.createDataFrame(user_rdd, user_schema)

        # 3. 读取并解析 sessions
        def parse_session(line):
            try:
                parts = line.split("\t")
                if len(parts) < 5:
                    return None
                user_id = int(json.loads(parts[4])['subjects'][0]['id'])
                session_duration = int(json.loads(parts[3])['playtime'])
                return (user_id, session_duration)
            except:
                return None

        session_rdd = spark.sparkContext.textFile(session_file).map(parse_session).filter(lambda x: x is not None)
        session_schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("session_time", IntegerType(), True),
        ])
        session_df = spark.createDataFrame(session_rdd, session_schema)

        session_agg_df = session_df.groupBy("user_id").agg(
            count("*").alias("session_count"),
            _sum("session_time").alias("total_play_time"),
            avg("session_time").alias("avg_session_time")
        )

        session_agg_df = session_agg_df.withColumn(
            "user_type",
            (col("total_play_time") > 7200).cast("string")
        ).replace({"true": "重度", "false": "轻度"}, subset=["user_type"])

        # 4. 读取并解析 tracks，提取标签
        def parse_track(line):
            try:
                parts = line.split("\t")
                if len(parts) < 5:
                    return None
                track_id = int(parts[1])
                tag_list = json.loads(parts[4]).get("tags", [])
                tag_names = [t.get("value", "") for t in tag_list if t.get("value")]
                return (track_id, tag_names)
            except:
                return None

        track_rdd = spark.sparkContext.textFile(track_file).map(parse_track).filter(lambda x: x is not None)
        track_schema = StructType([
            StructField("track_id", IntegerType(), True),
            StructField("tags", ArrayType(StringType()), True),
        ])
        track_df = spark.createDataFrame(track_rdd, track_schema)

        # 5. 构建 user-track-tag 关系
        def extract_user_track(line):
            try:
                parts = line.split("\t")
                if len(parts) < 5:
                    return []
                user_id = int(json.loads(parts[4])['subjects'][0]['id'])
                objects = json.loads(parts[4])['objects']
                return [(user_id, int(obj['id'])) for obj in objects if obj['type'] == 'track']
            except:
                return []

        user_track_rdd = spark.sparkContext.textFile(session_file).flatMap(extract_user_track).filter(lambda x: x is not None)
        user_track_schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("track_id", IntegerType(), True),
        ])
        user_track_df = spark.createDataFrame(user_track_rdd, user_track_schema)

        user_tag_df = user_track_df.join(track_df, on="track_id", how="left") \
            .select("user_id", explode("tags").alias("tag")) \
            .groupBy("user_id", "tag").count() \
            .orderBy("user_id", col("count").desc())

        top_tags_df = user_tag_df.groupBy("user_id").agg(
            collect_list("tag").alias("tag_list")
        ).withColumn("top_tags", col("tag_list").cast("string"))

        # 6. 合并所有数据
        final_df = user_df.join(session_agg_df, on="user_id", how="left") \
                          .join(top_tags_df.select("user_id", "top_tags"), on="user_id", how="left")

        print("Final Data Preview:")
        final_df.show(10, truncate=False)

        # 7. 写入 MySQL
        final_df.write \
            .format("jdbc") \
            .option("url", mysql_url) \
            .option("driver", mysql_driver) \
            .option("dbtable", mysql_target_table) \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .mode("overwrite") \
            .save()

        print(f"Successfully wrote user image to MySQL table: {mysql_target_table}")

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    spark.stop()
    print("Process finished successfully.")
