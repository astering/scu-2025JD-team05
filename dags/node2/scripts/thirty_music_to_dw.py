import sys
import json
import urllib.parse
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_unixtime, col

def parse_idomaar_entity_line(line, expected_type):
    parts = line.split("\t")
    if len(parts) < 4 or parts[0] != expected_type:
        return None
    try:
        entity_id = int(parts[1])
        data = json.loads(parts[3])
        data = {k: urllib.parse.unquote(v) if isinstance(v, str) else v for k, v in data.items()}
        data[f"{expected_type}_id"] = entity_id
        return data
    except:
        return None

def parse_relation_events_line(line):
    parts = line.split("\t")
    if len(parts) < 5 or parts[0] != "event.play":
        return None
    try:
        timestamp = int(parts[2])
        attr = json.loads(parts[3])
        rel = json.loads(parts[4])
        playtime = attr.get("playtime")

        user_id = rel.get("subjects", [{}])[0].get("id") if rel.get("subjects") else None
        track_id = rel.get("objects", [{}])[0].get("id") if rel.get("objects") else None

        return (timestamp, user_id, track_id, playtime)
    except:
        return None

def parse_relation_love_line(line):
    parts = line.split("\t")
    if len(parts) < 5 or parts[0] != "preference":
        return None
    try:
        attr = json.loads(parts[3])
        rel = json.loads(parts[4])
        value = urllib.parse.unquote(attr.get("value", ""))
        user_id = rel.get("subjects", [{}])[0].get("id") if rel.get("subjects") else None
        track_id = rel.get("objects", [{}])[0].get("id") if rel.get("objects") else None
        return (user_id, track_id, value)
    except:
        return None

def parse_relation_session_line(line):
    parts = line.split("\t")
    if len(parts) < 5 or parts[0] != "event.session":
        return None
    try:
        timestamp = int(parts[2])
        attr = json.loads(parts[3])
        rel = json.loads(parts[4])
        numtracks = attr.get("numtracks")
        playtime = attr.get("playtime")
        user_id = rel.get("subjects", [{}])[0].get("id") if rel.get("subjects") else None
        return (timestamp, user_id, numtracks, playtime)
    except:
        return None

def create_and_insert_table(spark, df, table_name):
    df.show(10, truncate=False)
    df.write.mode("overwrite").saveAsTable(table_name)

def main(entities_base_path, relations_base_path):
    spark = SparkSession.builder.appName("ThirtyMusic ETL to ODS & DW").enableHiveSupport().getOrCreate()

    # ========== ODS: Entities ==========
    entity_configs = [
        ("albums", "album", StructType([
            StructField("MBID", StringType()),
            StructField("title", StringType()),
            StructField("album_id", LongType())
        ])),
        ("persons", "person", StructType([
            StructField("MBID", StringType()),
            StructField("name", StringType()),
            StructField("person_id", LongType())
        ])),
        ("playlist", "playlist", StructType([
            StructField("ID", LongType()),
            StructField("Title", StringType()),
            StructField("numtracks", IntegerType()),
            StructField("duration", IntegerType()),
            StructField("playlist_id", LongType())
        ])),
        ("tags", "tag", StructType([
            StructField("value", StringType()),
            StructField("url", StringType()),
            StructField("tag_id", LongType())
        ])),
        ("tracks", "track", StructType([
            StructField("duration", IntegerType()),
            StructField("playcount", IntegerType()),
            StructField("MBID", StringType()),
            StructField("name", StringType()),
            StructField("track_id", LongType())
        ])),
        ("users", "user", StructType([
            StructField("duration", IntegerType()),
            StructField("playcount", IntegerType()),
            StructField("MBID", StringType()),
            StructField("name", StringType()),
            StructField("user_id", LongType())
        ]))
    ]

    for file, expected_type, schema in entity_configs:
        path = f"{entities_base_path}/{file}.idomaar"
        rdd = spark.sparkContext.textFile(path)
        parsed = rdd.map(lambda x: parse_idomaar_entity_line(x, expected_type)).filter(lambda x: x is not None)
        df = spark.createDataFrame(parsed, schema)
        create_and_insert_table(spark, df, f"ods_{file}")

    # ========== ODS: Relations ==========

    # events.idomaar
    events_path = f"{relations_base_path}/events.idomaar"
    events_rdd = spark.sparkContext.textFile(events_path)
    events_df = spark.createDataFrame(
        events_rdd.map(parse_relation_events_line).filter(lambda x: x is not None),
        StructType([
            StructField("timestamp", LongType()),
            StructField("user_id", LongType()),
            StructField("track_id", LongType()),
            StructField("playtime", IntegerType())
        ])
    )
    create_and_insert_table(spark, events_df, "ods_events")

    # love.idomaar
    love_path = f"{relations_base_path}/love.idomaar"
    love_df = spark.createDataFrame(
        spark.sparkContext.textFile(love_path).map(parse_relation_love_line).filter(lambda x: x is not None),
        StructType([
            StructField("user_id", LongType()),
            StructField("track_id", LongType()),
            StructField("value", StringType())
        ])
    )
    create_and_insert_table(spark, love_df, "ods_love")

    # sessions.idomaar
    sessions_path = f"{relations_base_path}/sessions.idomaar"
    sessions_df = spark.createDataFrame(
        spark.sparkContext.textFile(sessions_path).map(parse_relation_session_line).filter(lambda x: x is not None),
        StructType([
            StructField("timestamp", LongType()),
            StructField("user_id", LongType()),
            StructField("numtracks", IntegerType()),
            StructField("playtime", IntegerType())
        ])
    )
    create_and_insert_table(spark, sessions_df, "ods_sessions")

    # ========== DW: 清洗层举例 ==========
    events_dw = events_df.withColumn("event_time", from_unixtime(col("timestamp")))
    create_and_insert_table(spark, events_dw, "dw_events")

    sessions_dw = sessions_df.withColumn("session_time", from_unixtime(col("timestamp")))
    create_and_insert_table(spark, sessions_dw, "dw_sessions")

    love_dw = love_df.withColumn("value", col("value"))
    create_and_insert_table(spark, love_dw, "dw_love")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: thirtymusic_to_dw.py <entities_base_path> <relations_base_path>", file=sys.stderr)
        sys.exit(1)

    main(sys.argv[1], sys.argv[2])
