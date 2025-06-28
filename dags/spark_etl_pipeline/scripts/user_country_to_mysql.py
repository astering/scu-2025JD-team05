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
    mysql_driver = "com.mysql.jdbc.Driver"

    spark = SparkSession.builder \
        .appName("User Country Count ETL") \
        .getOrCreate()

    def parse_user_line(line):
        try:
            parts = line.split("\t")
            if len(parts) < 4:
                return None
            user_props = json.loads(parts[3])
            country = user_props.get("country", "").strip()
            if not country:
                return None
            username = user_props.get("lastfm_username", "").strip()
            username_decoded = urllib.parse.unquote(username)  # ✅ 仅解码用户名
            return (country, username_decoded)
        except Exception:
            return None

    user_rdd = spark.sparkContext.textFile(user_file).map(parse_user_line).filter(lambda x: x is not None)

    schema = StructType([
        StructField("country", StringType(), True),
        StructField("username", StringType(), True),
    ])

    user_df = spark.createDataFrame(user_rdd, schema)

    country_count_df = user_df.groupBy("country").agg(count("*").alias("user_count"))

    # 写入 MySQL
    country_count_df.write.format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", mysql_table) \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("driver", mysql_driver) \
        .mode("overwrite") \
        .save()

    print("User country count written to MySQL.")
    spark.stop()
