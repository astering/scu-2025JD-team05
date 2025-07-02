import sys
import re
import json
from collections import Counter
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType

def parse_line_generic(line, expected_type):
    """
    通用解析函数，支持 love 和 sessions 类型。
    love 返回 Row(user_id, track_id)
    sessions 返回多个 Row(session_id, timestamp, user_id, track_id, playstart, playtime, playratio, action)
    传入 expected_type 来做类型校验
    """
    try:
        # 正则匹配5列，支持制表符或空格分隔
        match = re.match(r'^(\S+)[\t ]+(\S+)[\t ]+(\S+)[\t ]+({.*?})[\t ]+({.*})$', line.strip())
        if not match:
            print(f"[结构错误] 分割字段不足5: {line.strip()}")
            return None

        type_, id1, id2, json1_str, json2_str = match.groups()

        if type_ != expected_type:
            print(f"[类型错误] 期望: {expected_type}, 实际: {type_}")
            return None

        relation = json.loads(json2_str)
        user_id = relation["subjects"][0]["id"]

        if expected_type == "preference":
            # love 文件格式，返回 user_id, track_id
            track_id = relation["objects"][0]["id"]
            return Row(user_id=int(user_id), track_id=int(track_id))

        elif expected_type == "event.session":
            # sessions 文件格式，返回多条播放记录
            session_id = int(id1)
            timestamp = int(id2)
            rows = []
            for obj in relation["objects"]:
                rows.append(Row(
                    session_id=session_id,
                    timestamp=timestamp,
                    user_id=int(user_id),
                    track_id=int(obj.get("id")),
                    playstart=int(obj.get("playstart", -1)),
                    playtime=int(obj.get("playtime", -1)),
                    playratio=float(obj.get("playratio", 0.0)),
                    action=obj.get("action", "")
                ))
            return rows

        else:
            print(f"[未知类型] {expected_type}")
            return None

    except Exception as e:
        print(f"[解析失败] {e} | 行内容: {line.strip()}")
        return None


def process_love(spark, path):
    rdd_raw = spark.sparkContext.textFile(path)
    print(f"LOVE 原始行数: {rdd_raw.count()}")
    rdd = rdd_raw.map(lambda line: parse_line_generic(line, "preference")).filter(lambda x: x is not None)
    print(f"LOVE 解析后行数: {rdd.count()}")

    df = spark.createDataFrame(rdd, schema=StructType([
        StructField("user_id", LongType()),
        StructField("track_id", LongType())
    ]))

    df.show(10, truncate=False)

    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS ods_love (
            user_id BIGINT,
            track_id BIGINT
        )
        STORED AS PARQUET
        LOCATION '/user/hive/warehouse/ods_love'
    """)

    df.write.mode("overwrite").parquet("/user/hive/warehouse/ods_love")


def process_sessions(spark, path):
    rdd_raw = spark.sparkContext.textFile(path)
    print(f"SESSIONS 原始行数: {rdd_raw.count()}")

    # sessions 解析后每行返回列表，使用 flatMap 展开
    rdd = rdd_raw.flatMap(lambda line: parse_line_generic(line, "event.session") or []).filter(lambda x: x is not None)
    print(f"SESSIONS 解析后记录数: {rdd.count()}")

    df = spark.createDataFrame(rdd, schema=StructType([
        StructField("session_id", LongType()),
        StructField("timestamp", LongType()),
        StructField("user_id", LongType()),
        StructField("track_id", LongType()),
        StructField("playstart", LongType()),
        StructField("playtime", LongType()),
        StructField("playratio", DoubleType()),
        StructField("action", StringType())
    ]))

    df.show(10, truncate=False)

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

    df.write.mode("overwrite").parquet("/user/hive/warehouse/ods_sessions")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit parse_love_and_sessions.py <entities_base_path> <relations_base_path>")
        sys.exit(1)

    entities_base_path = sys.argv[1]
    relations_base_path = sys.argv[2]

    spark = SparkSession.builder \
        .appName("ThirtyMusicIdomaarParser") \
        .enableHiveSupport() \
        .getOrCreate()

    love_path = f"{relations_base_path}/love.idomaar"
    sessions_path = f"{relations_base_path}/sessions.idomaar"

    process_love(spark, love_path)
    process_sessions(spark, sessions_path)

    spark.stop()
