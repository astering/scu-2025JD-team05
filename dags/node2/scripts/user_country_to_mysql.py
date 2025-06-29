# 文件路径：dags/spark_etl_pipeline/scripts/user_country_to_mysql.py

import sys
import json
import urllib.parse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import StructType, StructField, StringType

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: user_country_to_mysql.py <user_file> <mysql_url> <mysql_user> <mysql_password>", file=sys.stderr)
        sys.exit(1)

    user_file = sys.argv[1]
    mysql_url = sys.argv[2]
    mysql_user = sys.argv[3]
    mysql_password = sys.argv[4]
    mysql_table = "user_country_count"
    mysql_driver = "com.mysql.jdbc.Driver"  # 或 com.mysql.cj.jdbc.Driver（推荐）

    spark = SparkSession.builder \
        .appName("User Country Count ETL") \
        .getOrCreate()

    def parse_user_line(line):
        try:
            parts = line.split("\t")
            if len(parts) < 4:
                return None
            if parts[0] != "user":
                return None
            user_props = json.loads(parts[3])
            country = user_props.get("country", "").strip()
            if not country:
                return None
            username = user_props.get("lastfm_username", "").strip()
            username_decoded = urllib.parse.unquote(username)
            return (country, username_decoded)
        except Exception as e:
            print(f"Failed to parse line: {line}\nError: {e}")
            return None

    # 读取并解析数据
    raw_rdd = spark.sparkContext.textFile(user_file)
    print(f"Total lines in file: {raw_rdd.count()}")

    user_rdd = raw_rdd.map(parse_user_line).filter(lambda x: x is not None)
    print(f"Valid parsed user lines: {user_rdd.count()}")

    # 构建 DataFrame
    schema = StructType([
        StructField("country", StringType(), True),
        StructField("username", StringType(), True),
    ])

    user_df = spark.createDataFrame(user_rdd, schema)
    user_df.cache()

    country_count_df = user_df.groupBy("country").agg(count("*").alias("user_count"))
    country_count_df.cache()

    print("===== Top 10 Country Counts =====")
    country_count_df.show(10)

    # 写入 MySQL（确保非空）
    if country_count_df.count() > 0:
        country_count_df.write.format("jdbc") \
            .option("url", mysql_url) \
            .option("dbtable", mysql_table) \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .option("driver", mysql_driver) \
            .mode("overwrite") \
            .save()
        print("✅ Data written to MySQL successfully.")
    else:
        print("⚠️ No data to write to MySQL.")

    spark.stop()
