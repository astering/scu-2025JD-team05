import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType

def parse_love(line):
    try:
        parts = line.strip().split("\t")
        rel = json.loads(parts[4])
        user_id = rel["subjects"][0]["id"]
        track_id = rel["objects"][0]["id"]
        return (user_id, track_id)
    except:
        return None

def parse_sessions(line):
    try:
        parts = line.strip().split("\t")
        session_id = int(parts[1])
        timestamp = int(parts[2])
        rel = json.loads(parts[4])
        user_id = rel["subjects"][0]["id"]
        track_records = []
        for obj in rel["objects"]:
            track_records.append((
                session_id,
                timestamp,
                user_id,
                obj.get("id"),
                obj.get("playstart"),
                obj.get("playtime"),
                obj.get("playratio"),
                obj.get("action")
            ))
        return track_records
    except:
        return []

if __name__ == "__main__":
    # 获取命令行参数
    entities_base_path = sys.argv[1]
    relations_base_path = sys.argv[2]

    # 初始化 SparkSession 并启用 Hive 支持
    spark = SparkSession.builder \
        .appName("ThirtyMusicToDW") \
        .enableHiveSupport() \
        .getOrCreate()

    # ========== LOVE ==========
    love_path = f"{relations_base_path}/love.idomaar"
    love_rdd = spark.sparkContext.textFile(love_path).map(parse_love).filter(lambda x: x is not None)

    love_df = spark.createDataFrame(love_rdd, schema=StructType([
        StructField("user_id", LongType()),
        StructField("track_id", LongType())
    ]))

    print("=== LOVE 数据预览 ===")
    love_df.show(10, truncate=False)

    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS ods_love (
            user_id BIGINT,
            track_id BIGINT
        )
        STORED AS PARQUET
        LOCATION '/user/hive/warehouse/ods_love'
    """)

    love_df.write.mode("overwrite").parquet("/user/hive/warehouse/ods_love")

    # ========== SESSIONS ==========
    sessions_path = f"{relations_base_path}/sessions.idomaar"
    sessions_rdd = spark.sparkContext.textFile(sessions_path) \
        .flatMap(parse_sessions) \
        .filter(lambda x: x is not None)

    sessions_df = spark.createDataFrame(sessions_rdd, schema=StructType([
        StructField("session_id", LongType()),
        StructField("timestamp", LongType()),
        StructField("user_id", LongType()),
        StructField("track_id", LongType()),
        StructField("playstart", LongType()),
        StructField("playtime", LongType()),
        StructField("playratio", DoubleType()),
        StructField("action", StringType())
    ]))

    print("=== SESSIONS 数据预览 ===")
    sessions_df.show(10, truncate=False)

    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS ods_sessions (
            session_id BIGINT,
            timestamp BIGINT,
            user_id BIGINT,
            track_id BIGINT,
            playstart BIGINT,
            playtime BIGINT,
            playratio DOUBLE,
            action STRING
        )
        STORED AS PARQUET
        LOCATION '/user/hive/warehouse/ods_sessions'
    """)

    sessions_df.write.mode("overwrite").parquet("/user/hive/warehouse/ods_sessions")

    spark.stop()
