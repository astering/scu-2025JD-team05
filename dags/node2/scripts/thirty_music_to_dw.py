import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def parse_idomaar_line(line, expected_type):
    parts = line.split("\t")
    if len(parts) < 4:
        return None
    if parts[0] != expected_type:
        return None
    try:
        json_str = parts[3]
        data = json.loads(json_str)
        return data
    except Exception as e:
        return None

def parse_relation_line(line, expected_type):
    parts = line.split("\t")
    if len(parts) < 4:
        return None
    if parts[0] != expected_type:
        return None
    try:
        data = json.loads(parts[3])
        return data
    except Exception as e:
        return None

def main(entities_base_path, relations_base_path):
    spark = SparkSession.builder.appName("ThirtyMusic ETL to DW").enableHiveSupport().getOrCreate()

    # ----------------- entities -----------------

    # 1. albums.idomaar
    albums_path = f"{entities_base_path}/albums.idomaar"
    albums_rdd = spark.sparkContext.textFile(albums_path)
    albums_parsed = albums_rdd.map(lambda x: parse_idomaar_line(x, "album")).filter(lambda x: x is not None)
    albums_schema = StructType([
        StructField("MBID", StringType()),
        StructField("title", StringType())
    ])
    albums_df = spark.createDataFrame(albums_parsed, albums_schema)
    albums_df.createOrReplaceTempView("stg_albums")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dw_albums (
            MBID STRING,
            title STRING
        )
        STORED AS PARQUET
    """)
    albums_df.write.mode("overwrite").insertInto("dw_albums")

    # 2. persons.idomaar
    persons_path = f"{entities_base_path}/persons.idomaar"
    persons_rdd = spark.sparkContext.textFile(persons_path)
    persons_parsed = persons_rdd.map(lambda x: parse_idomaar_line(x, "person")).filter(lambda x: x is not None)
    persons_schema = StructType([
        StructField("MBID", StringType()),
        StructField("name", StringType())
    ])
    persons_df = spark.createDataFrame(persons_parsed, persons_schema)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dw_persons (
            MBID STRING,
            name STRING
        )
        STORED AS PARQUET
    """)
    persons_df.write.mode("overwrite").insertInto("dw_persons")

    # 3. playlists.idomaar
    playlists_path = f"{entities_base_path}/playlist.idomaar"
    playlists_rdd = spark.sparkContext.textFile(playlists_path)
    playlists_parsed = playlists_rdd.map(lambda x: parse_idomaar_line(x, "playlist")).filter(lambda x: x is not None)
    playlists_schema = StructType([
        StructField("ID", LongType()),
        StructField("Title", StringType()),
        StructField("numtracks", IntegerType()),
        StructField("duration", IntegerType())
    ])
    playlists_df = spark.createDataFrame(playlists_parsed, playlists_schema)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dw_playlists (
            ID BIGINT,
            Title STRING,
            numtracks INT,
            duration INT
        )
        STORED AS PARQUET
    """)
    playlists_df.write.mode("overwrite").insertInto("dw_playlists")

    # 4. tags.idomaar
    tags_path = f"{entities_base_path}/tags.idomaar"
    tags_rdd = spark.sparkContext.textFile(tags_path)
    tags_parsed = tags_rdd.map(lambda x: parse_idomaar_line(x, "tag")).filter(lambda x: x is not None)
    tags_schema = StructType([
        StructField("value", StringType()),
        StructField("url", StringType())
    ])
    tags_df = spark.createDataFrame(tags_parsed, tags_schema)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dw_tags (
            value STRING,
            url STRING
        )
        STORED AS PARQUET
    """)
    tags_df.write.mode("overwrite").insertInto("dw_tags")

    # 5. tracks.idomaar
    tracks_path = f"{entities_base_path}/tracks.idomaar"
    tracks_rdd = spark.sparkContext.textFile(tracks_path)
    tracks_parsed = tracks_rdd.map(lambda x: parse_idomaar_line(x, "track")).filter(lambda x: x is not None)
    tracks_schema = StructType([
        StructField("duration", IntegerType()),
        StructField("playcount", IntegerType()),
        StructField("MBID", StringType()),
        StructField("name", StringType())
    ])
    tracks_df = spark.createDataFrame(tracks_parsed, tracks_schema)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dw_tracks (
            duration INT,
            playcount INT,
            MBID STRING,
            name STRING
        )
        STORED AS PARQUET
    """)
    tracks_df.write.mode("overwrite").insertInto("dw_tracks")

    # 6. users.idomaar
    users_path = f"{entities_base_path}/users.idomaar"
    users_rdd = spark.sparkContext.textFile(users_path)
    users_parsed = users_rdd.map(lambda x: parse_idomaar_line(x, "user")).filter(lambda x: x is not None)
    users_schema = StructType([
        StructField("duration", IntegerType()),
        StructField("playcount", IntegerType()),
        StructField("MBID", StringType()),
        StructField("name", StringType())
    ])
    users_df = spark.createDataFrame(users_parsed, users_schema)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dw_users (
            duration INT,
            playcount INT,
            MBID STRING,
            name STRING
        )
        STORED AS PARQUET
    """)
    users_df.write.mode("overwrite").insertInto("dw_users")

    # ----------------- relations -----------------

    # ============ events.idomaar =============
    events_path = f"{relations_base_path}/events.idomaar"
    events_rdd = spark.sparkContext.textFile(events_path)

    def parse_event_line(line):
        parts = line.split("\t")
        if len(parts) < 5 or parts[0] != "event.play":
            return None
        try:
            timestamp = int(parts[2])
            attr = json.loads(parts[3])
            relation = json.loads(parts[4])
            playtime = attr.get("playtime", None)

            # 获取 user_id 和 track_id（简化为只取第一个）
            subjects = relation.get("subjects", [])
            objects = relation.get("objects", [])

            user_id = subjects[0]["id"] if subjects and subjects[0]["type"] == "user" else None
            track_id = objects[0]["id"] if objects and objects[0]["type"] == "track" else None

            return (timestamp, user_id, track_id, playtime)
        except:
            return None

    events_parsed = events_rdd.map(parse_event_line).filter(lambda x: x is not None)

    events_schema = StructType([
        StructField("timestamp", LongType()),
        StructField("user_id", LongType()),
        StructField("track_id", LongType()),
        StructField("playtime", IntegerType())
    ])

    events_df = spark.createDataFrame(events_parsed, events_schema)
    spark.sql("""
              CREATE TABLE IF NOT EXISTS dw_events
              (
                  timestamp
                  BIGINT,
                  user_id
                  BIGINT,
                  track_id
                  BIGINT,
                  playtime
                  INT
              )
                  STORED AS PARQUET
              """)
    events_df.write.mode("overwrite").insertInto("dw_events")

    # 8. love.idomaar (preference)
    love_path = f"{relations_base_path}/love.idomaar"
    love_rdd = spark.sparkContext.textFile(love_path)
    def parse_love(line):
        parts = line.split("\t")
        if len(parts) < 4 or parts[0] != "preference":
            return None
        try:
            data = json.loads(parts[3])
            value = data.get("value", None)
            return (value,)
        except:
            return None
    love_parsed = love_rdd.map(parse_love).filter(lambda x: x is not None)
    love_schema = StructType([
        StructField("value", StringType())
    ])
    love_df = spark.createDataFrame(love_parsed, love_schema)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dw_love (
            value STRING
        )
        STORED AS PARQUET
    """)
    love_df.write.mode("overwrite").insertInto("dw_love")

    # 9. sessions.idomaar (event.session)
    sessions_path = f"{relations_base_path}/sessions.idomaar"
    sessions_rdd = spark.sparkContext.textFile(sessions_path)
    def parse_session(line):
        parts = line.split("\t")
        if len(parts) < 4 or parts[0] != "event.session":
            return None
        try:
            data = json.loads(parts[3])
            numtracks = data.get("numtracks", None)
            playtime = data.get("playtime", None)
            return (numtracks, playtime)
        except:
            return None
    sessions_parsed = sessions_rdd.map(parse_session).filter(lambda x: x is not None)
    sessions_schema = StructType([
        StructField("numtracks", IntegerType()),
        StructField("playtime", IntegerType())
    ])
    sessions_df = spark.createDataFrame(sessions_parsed, sessions_schema)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dw_sessions (
            numtracks INT,
            playtime INT
        )
        STORED AS PARQUET
    """)
    sessions_df.write.mode("overwrite").insertInto("dw_sessions")

    print("全部 ThirtyMusic 数据成功写入 Hive DW 层")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: thirtymusic_to_dw.py <entities_base_path> <relations_base_path>", file=sys.stderr)
        sys.exit(1)

    entities_base_path = sys.argv[1]
    relations_base_path = sys.argv[2]
    main(entities_base_path, relations_base_path)
