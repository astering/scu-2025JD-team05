import sys
import json
from urllib.parse import unquote

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf, max as spark_max
from pyspark.sql.types import StringType, IntegerType, LongType

def safe_unquote(s):
    if s and isinstance(s, str):
        return unquote(s)
    else:
        return "Unknown"

def safe_int(x):
    try:
        if x is None:
            return 0
        val = int(x)
        return val if val >= 0 else 0
    except Exception:
        return 0

# ���� UDF
decode_udf = udf(safe_unquote, StringType())
safe_int_udf = udf(safe_int, LongType())

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("""
        Usage: top_tracks_etl.py <track_path> <person_path> <album_path> <mysql_url> <mysql_user> <mysql_password>
        """, file=sys.stderr)
        sys.exit(-1)

    track_path = sys.argv[1]
    person_path = sys.argv[2]
    album_path = sys.argv[3]
    mysql_url = sys.argv[4]
    mysql_user = sys.argv[5]
    mysql_password = sys.argv[6]

    spark = SparkSession.builder \
        .appName("TopTracksETL") \
        .enableHiveSupport() \
        .getOrCreate()

    # ��ȡtrack.idomaar������ֻ����type=="track"
    track_rdd = spark.sparkContext.textFile(track_path) \
        .map(lambda line: line.split('\t')) \
        .filter(lambda x: len(x) == 5 and x[0] == "track")

    track_df = track_rdd.map(lambda x: (
        int(x[1]),
        json.loads(x[3]).get("duration", -1),
        safe_int(json.loads(x[3]).get("playcount", 0)),
        json.loads(x[3]).get("MBID") or "",
        unquote(json.loads(x[3]).get("name", "")),
        # artists ��һ���б�ȡ��һ��artist��id������-1
        int(json.loads(x[4]).get("artists", [{}])[0].get("id", -1)) if json.loads(x[4]).get("artists") else -1,
        json.loads(x[4]).get("albums", [{}])[0].get("id") if json.loads(x[4]).get("albums") else None,
    )).toDF(["track_id", "duration", "playcount", "track_mbid", "title", "artist_id", "album_id"])

    # ���˵� artist_id == -1 �� playcount == 0 ����Ч��¼
    track_df = track_df.filter((col("artist_id") != -1) & (col("playcount") > 0))

    # ��ȡpersons.idomaar��ֻȡtype=="person"
    person_rdd = spark.sparkContext.textFile(person_path) \
        .map(lambda line: line.split('\t')) \
        .filter(lambda x: len(x) == 5 and x[0] == "person")

    person_df = person_rdd.map(lambda x: (
        int(x[1]),
        unquote(json.loads(x[3]).get("name", "")) or "Unknown"
    )).toDF(["artist_id", "artist_name"])

    # ��ȡalbums.idomaar����������
    album_rdd = spark.sparkContext.textFile(album_path) \
        .map(lambda line: line.split('\t')) \
        .filter(lambda x: len(x) == 5 and x[0] == "album")

    album_df = album_rdd.map(lambda x: (
        int(x[1]),
        unquote(json.loads(x[3]).get("name", "")) or "Unknown"
    )).toDF(["album_id", "album_name"])

    # �������ݣ�track join person join album
    joined_df = track_df.join(person_df, "artist_id", "left") \
        .join(album_df, "album_id", "left")

    # ����ֵ
    joined_df = joined_df.na.fill({"artist_name": "Unknown", "album_name": "Unknown"})

    # ͳ�Ʋ�����ǰ100�ĸ���
    top_tracks_df = joined_df.orderBy(col("playcount").desc()).limit(100)

    # ���� top_tracks �� MySQL
    top_tracks_df.select(
        "track_id", "duration", "playcount", "track_mbid", "title", "artist_id", "artist_name", "album_id", "album_name"
    ).write.format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", "top_track") \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("overwrite").save()

    # ͳ��ÿ���������и����Ĳ������ܺͣ�ȡǰ100
    artist_playcount_df = joined_df.groupBy("artist_id", "artist_name") \
        .sum("playcount") \
        .withColumnRenamed("sum(playcount)", "total_playcount") \
        .orderBy(col("total_playcount").desc()) \
        .limit(100)

    # ���� top_artist �� MySQL
    artist_playcount_df.write.format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", "top_artist") \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("overwrite").save()

    spark.stop()
