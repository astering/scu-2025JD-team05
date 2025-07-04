import sys
import json
import urllib.parse
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_unixtime, col, when, lit


def parse_user_line(line):
    parts = line.split("\t")
    if len(parts) < 4 or parts[0] != "user":
        return None
    try:
        user_id = int(parts[1])
        create_time = int(parts[2])
        data = json.loads(parts[3])
        data = {k: urllib.parse.unquote(v) if isinstance(v, str) else v for k, v in data.items()}

        # 清洗字段
        age = data.get("age")
        if age is None or not isinstance(age, int):
            age = -1

        country = data.get("country", "")
        if not country or country.strip() == "":
            country = "unknown"

        return {
            "user_id": user_id,
            "create_time": create_time,
            "lastfm_username": data.get("lastfm_username"),
            "gender": data.get("gender"),
            "age": age,
            "country": country,
            "playcount": data.get("playcount", 0),
            "playlists": data.get("playlists", 0),
            "subscribertype": data.get("subscribertype")
        }
    except Exception as e:
        return None


def create_and_insert_table(spark, df, table_name):
    df.show(10, truncate=False)
    df.write.mode("overwrite").saveAsTable(table_name)


def main(users_file_path):
    spark = SparkSession.builder.appName("Users ETL to ODS & DW").enableHiveSupport().getOrCreate()

    # 读取并解析 users.idomaar
    rdd = spark.sparkContext.textFile(users_file_path)
    parsed_rdd = rdd.map(parse_user_line).filter(lambda x: x is not None)

    schema = StructType([
        StructField("user_id", LongType()),
        StructField("create_time", LongType()),
        StructField("lastfm_username", StringType()),
        StructField("gender", StringType()),
        StructField("age", IntegerType()),
        StructField("country", StringType()),
        StructField("playcount", LongType()),
        StructField("playlists", IntegerType()),
        StructField("subscribertype", StringType())
    ])

    df = spark.createDataFrame(parsed_rdd, schema)

    # 写入 ODS 表
    create_and_insert_table(spark, df, "ods_users")

    # 写入 DW 表（增加注册时间字段）
    dw_df = df.withColumn("register_time", from_unixtime(col("create_time")))
    create_and_insert_table(spark, dw_df, "dw_users")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: users_to_dw.py <users_file_path>", file=sys.stderr)
        sys.exit(1)

    main(sys.argv[1])
