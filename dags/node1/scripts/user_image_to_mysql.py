import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: user_image_to_mysql.py <user_file> <session_file> <mysql_url> <mysql_user> <mysql_password>", file=sys.stderr)
        sys.exit(1)

    user_file = sys.argv[1]
    session_file = sys.argv[2]
    mysql_url = sys.argv[3]
    mysql_user = sys.argv[4]
    mysql_password = sys.argv[5]
    mysql_table = "user_image"
    mysql_driver = "com.mysql.jdbc.Driver"

    spark = SparkSession.builder \
        .appName("User Image ETL") \
        .getOrCreate()

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

    # 读取并清洗用户数据
    user_rdd = spark.sparkContext.textFile(user_file).map(parse_user).filter(lambda x: x is not None)
    user_schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
    ])
    user_df = spark.createDataFrame(user_rdd, user_schema)

    # 读取并清洗 session 数据
    session_rdd = spark.sparkContext.textFile(session_file).map(parse_session).filter(lambda x: x is not None)
    session_schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("session_time", IntegerType(), True),
    ])
    session_df = spark.createDataFrame(session_rdd, session_schema)

    # 用户画像聚合：统计播放次数与总时长
    user_image_df = session_df.groupBy("user_id").agg(
        count("*").alias("session_count"),
        _sum("session_time").alias("total_play_time"),
        avg("session_time").alias("avg_session_time")
    ).join(user_df, on="user_id", how="left")

    # 写入 MySQL
    user_image_df.write.format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", mysql_table) \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("driver", mysql_driver) \
        .mode("overwrite") \
        .save()

    print("User image data written to MySQL.")
    spark.stop()
