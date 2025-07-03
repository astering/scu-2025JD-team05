import sys
import json
import urllib.parse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when
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
    mysql_driver = "com.mysql.cj.jdbc.Driver"

    spark = SparkSession.builder.appName("User Country Gender Count ETL").getOrCreate()

    def parse_user_line(line):
        try:
            parts = line.split("\t")
            if len(parts) < 4 or parts[0] != "user":
                return None
            user_props = json.loads(parts[3])
            country = user_props.get("country", "").strip()
            gender = user_props.get("gender", "").strip().lower()
            if not country or gender not in ["m", "f", "n"]:
                return None
            return (country, gender)
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
        StructField("gender", StringType(), True),
    ])

    user_df = spark.createDataFrame(user_rdd, schema)
    user_df.cache()

    # 按国家统计性别数量：m、f、n
    country_gender_df = user_df.groupBy("country").agg(
        sum(when(col("gender") == "m", 1).otherwise(0)).alias("male_count"),
        sum(when(col("gender") == "f", 1).otherwise(0)).alias("female_count"),
        sum(when(col("gender") == "n", 1).otherwise(0)).alias("neutral_count")
    )

    # 总人数列
    country_gender_df = country_gender_df.withColumn(
        "user_count",
        col("male_count") + col("female_count") + col("neutral_count")
    )

    print("===== Top 10 Country Gender Counts =====")
    country_gender_df.orderBy(col("user_count").desc()).show(10)

    # 写入 MySQL（确保非空）
    if country_gender_df.count() > 0:
        country_gender_df.write.format("jdbc") \
            .option("url", mysql_url) \
            .option("dbtable", mysql_table) \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .option("driver", mysql_driver) \
            .mode("overwrite") \
            .save()
        print("Data written to MySQL successfully.")
    else:
        print("No data to write to MySQL.")

    spark.stop()
