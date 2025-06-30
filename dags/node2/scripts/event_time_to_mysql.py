import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, hour, to_date
from pyspark.sql.types import StructType, StructField, StringType, LongType

def main():
    if len(sys.argv) != 6:
        print("""
Usage: event_time_to_mysql.py <events_path> <mysql_url> <mysql_user> <mysql_password> <mysql_table>
        """, file=sys.stderr)
        sys.exit(-1)

    events_path, mysql_url, mysql_user, mysql_password, mysql_table = sys.argv[1:]

    spark = SparkSession.builder.appName("EventTimeAnalysis").getOrCreate()

    # �����¼��ļ���schema
    schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("event_id", LongType(), True),
        StructField("timestamp", LongType(), True),
        StructField("payload", StringType(), True),
        StructField("meta", StringType(), True),
    ])

    # ��ȡ�¼��ļ���tab�ָ�
    events_raw = spark.read.option("delimiter", "\t").schema(schema).csv(events_path)

    # ���˲����¼�
    play_events = events_raw.filter(col("event_type") == "event.play") \
        .withColumn("event_time", from_unixtime(col("timestamp")).cast("timestamp")) \
        .withColumn("event_date", to_date(col("event_time"))) \
        .withColumn("event_hour", hour(col("event_time"))) \
        .select("event_date", "event_hour")

    # �ۺ�ͳ��ÿ��ÿСʱ���Ŵ���
    active_hours = play_events.groupBy("event_date", "event_hour") \
        .count() \
        .withColumnRenamed("count", "play_count") \
        .orderBy("event_date", "event_hour")

    # д�� MySQL
    active_hours.write.format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", mysql_table) \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("driver", "com.mysql.jdbc.Driver") \
        .mode("overwrite") \
        .save()

    print("Event time analysis results written to MySQL successfully.")

    spark.stop()

if __name__ == "__main__":
    main()
