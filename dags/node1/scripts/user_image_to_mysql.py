import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, explode, collect_list
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType

if __name__ == "__main__":
    if len(sys.argv) != 9:
        print("""
        Usage: user_image_to_mysql.py <user_file> <session_file> <track_file> <mysql_url> <mysql_user> <mysql_password> <mysql_driver> <mysql_target_table>
        Example: user_image_to_mysql.py users.idomaar sessions.idomaar tracks.idomaar jdbc:mysql://host:port/db_name user password com.mysql.jdbc.Driver user_image
        """, file=sys.stderr)
        sys.exit(-1)

    user_file = sys.argv[1]
    session_file = sys.argv[2]
    track_file = sys.argv[3]
    mysql_url = sys.argv[4]
    mysql_user = sys.argv[5]
    mysql_password = sys.argv[6]
    mysql_driver = sys.argv[7]
    mysql_target_table = sys.argv[8]

    spark = SparkSession.builder \
        .appName("User Image to MySQL") \
        .getOrCreate()

    try:
        # 2. 读取并解析用户信息
        def parse_user(line):
            try:
                parts = line.split("\t")
                if len(parts) < 4:
                    return None
                user_id = int(parts[1])
                props = json.loads(parts[3])
                age = int(props.get("age")) if props.get("age") else None
                gender = props.get("gender", "")
                return (user_id, age, gender)
            except Exception as e:
                return None

        user_rdd = spark.sparkContext.textFile(user_file).map(parse_user).filter(lambda x: x is not None)
        user_schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("age", IntegerType(), True),
            StructField("gender", StringType(), True),
        ])
        user_df = spark.createDataFrame(user_rdd, user_schema)

        # 测试输出：user_df
        print("=== user_df ===")
        user_df.show(10, truncate=False)

        # 在创建DataFrame前添加验证（临时代码）
        print("==== 原始数据验证 ====")

        # 检查session文件
        print("\n1. Session文件前3行:")
        session_samples = spark.sparkContext.textFile(session_file).take(3)
        for i, line in enumerate(session_samples):
            print(f"样本{i + 1}: {line[:200]}...")  # 只打印前200字符防止过长

        # 检查track文件
        print("\n2. Track文件前3行:")
        track_samples = spark.sparkContext.textFile(track_file).take(3)
        for i, line in enumerate(track_samples):
            print(f"样本{i + 1}: {line[:200]}...")

        # 3. 读取并解析 sessions
        # 在parse_session函数内添加详细日志
        def parse_session(line):
            try:
                parts = line.split("\t")
                if len(parts) < 5:
                    print(f"⚠️ 字段不足: 只有{len(parts)}个字段（需要≥5）")
                    return None

                print(f"\n原始JSON[3]: {parts[3]}")  # playtime数据
                print(f"原始JSON[4]: {parts[4]}")  # user/track数据

                playtime = json.loads(parts[3])
                user_data = json.loads(parts[4])

                print(f"解析后playtime: {playtime}")
                print(f"解析后user_data: {user_data}")

                subjects = user_data.get("subjects", [])
                if not subjects:
                    print("⚠️ subjects为空")
                    return None

                first_subject = subjects[0]
                if "id" not in first_subject:
                    print(f"⚠️ subject缺少id字段: {first_subject}")
                    return None

                user_id = int(first_subject["id"])
                duration = int(playtime.get("playtime", 0))

                print(f"✅ 成功解析: user_id={user_id}, duration={duration}")
                return (user_id, duration)
            except Exception as e:
                print(f"❌ 解析异常: {str(e)}")
                return None

        # 临时测试解析函数
        test_line = spark.sparkContext.textFile(session_file).first()
        print("\n测试parse_session:")
        parse_session(test_line)


        session_rdd = spark.sparkContext.textFile(session_file).map(parse_session).filter(lambda x: x is not None)
        session_schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("session_time", IntegerType(), True),
        ])
        session_df = spark.createDataFrame(session_rdd, session_schema)

        # 在创建session_df后添加
        print("\n==== Session DataFrame验证 ====")
        print(f"有效记录数: {session_df.count()}")
        if session_df.count() > 0:
            print("Schema:")
            session_df.printSchema()
            print("数据示例:")
            session_df.show(5, truncate=False)
        else:
            # 检查RDD内容
            print("RDD内容样本:")
            print(session_rdd.take(5))

        session_agg_df = session_df.groupBy("user_id").agg(
            count("*").alias("session_count"),
            _sum("session_time").alias("total_play_time"),
            avg("session_time").alias("avg_session_time")
        )

        session_agg_df = session_agg_df.withColumn(
            "user_type",
            (col("total_play_time") > 7200).cast("string")
        ).replace({"true": "重度", "false": "轻度"}, subset=["user_type"])

        # 测试输出：session_agg_df
        print("=== session_agg_df ===")
        session_agg_df.show(10, truncate=False)

        # 4. 读取并解析 tracks，提取标签
        def parse_track(line):
            try:
                parts = line.split("\t")
                if len(parts) < 5:
                    return None
                track_id = int(parts[1])
                tag_list = json.loads(parts[4]).get("tags", [])
                tag_names = [t.get("value", "") for t in tag_list if t.get("value")]
                if tag_names:
                    return (track_id, tag_names)
                return None
            except Exception as e:
                return None

        track_rdd = spark.sparkContext.textFile(track_file).map(parse_track).filter(lambda x: x is not None)
        track_schema = StructType([
            StructField("track_id", IntegerType(), True),
            StructField("tags", ArrayType(StringType()), True),
        ])
        track_df = spark.createDataFrame(track_rdd, track_schema)

        # 测试输出：track_df
        print("=== track_df ===")
        track_df.show(10, truncate=False)

        # 5. 构建 user-track-tag 关系
        def extract_user_track(line):
            try:
                parts = line.split("\t")
                if len(parts) < 5:
                    return []

                # 解析用户数据
                user_data = json.loads(parts[4])
                subjects = user_data.get("subjects", [])

                # 验证并提取用户ID
                if not subjects or subjects[0].get("type") != "user":
                    return []

                user_id = int(subjects[0]["id"])

                # 解析track对象
                objects = user_data.get("objects", [])
                return [(user_id, int(obj["id"]))
                        for obj in objects
                        if obj.get("type") == 'track' and isinstance(obj.get("id"), (int, str))]
            except Exception as e:
                print(f"User-track关系提取错误: {str(e)} - 行内容: {line}")
                return []

        user_track_rdd = spark.sparkContext.textFile(session_file).flatMap(extract_user_track).filter(lambda x: x is not None)
        user_track_schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("track_id", IntegerType(), True),
        ])
        user_track_df = spark.createDataFrame(user_track_rdd, user_track_schema)

        # 测试输出：user_track_df
        print("=== user_track_df ===")
        user_track_df.show(10, truncate=False)

        user_tag_df = user_track_df.join(track_df, on="track_id", how="left") \
            .select("user_id", explode("tags").alias("tag")) \
            .groupBy("user_id", "tag").count() \
            .orderBy("user_id", col("count").desc())

        top_tags_df = user_tag_df.groupBy("user_id").agg(
            collect_list("tag").alias("tag_list")
        ).withColumn("top_tags", col("tag_list").cast("string"))

        # 测试输出：top_tags_df
        print("=== top_tags_df ===")
        top_tags_df.show(10, truncate=False)

        # 1. 验证原始数据
        print("原始session数据示例:")
        print(spark.sparkContext.textFile(session_file).take(3))

        # 2. 验证解析后的DataFrame
        session_df.createOrReplaceTempView("session_data")
        spark.sql("""
                  SELECT COUNT(*)                                    as total_count,
                         COUNT(CASE WHEN user_id IS NULL THEN 1 END) as null_user_ids,
                         AVG(session_time)                           as avg_duration
                  FROM session_data
                  """).show()

        # 3. 验证join条件
        print("用户ID匹配情况:")
        user_df.join(session_agg_df, "user_id", "left") \
            .groupBy(col("session_count").isNull()) \
            .count() \
            .show()

        # 6. 合并所有数据
        session_agg_df = session_agg_df.fillna({
            "session_count": 0,
            "total_play_time": 0,
            "avg_session_time": 0,
        })

        top_tags_df = top_tags_df.fillna({
            "top_tags": "[]",
        })

        # 在final_df创建前添加
        print("\n==== 用户ID匹配测试 ====")

        # 用户表中的ID
        user_ids = user_df.select("user_id").distinct()
        print(f"用户表唯一ID数: {user_ids.count()}")

        # 会话表中的ID
        session_user_ids = session_agg_df.select("user_id").distinct()
        print(f"会话表唯一ID数: {session_user_ids.count()}")

        # 检查交集
        matched_ids = user_ids.intersect(session_user_ids)
        print(f"匹配的ID数: {matched_ids.count()}")
        matched_ids.show(5)

        final_df = user_df.join(session_agg_df, on="user_id", how="left") \
            .join(top_tags_df.select("user_id", "top_tags"), on="user_id", how="left")

        # 测试输出：final_df
        print("=== final_df ===")
        final_df.show(10, truncate=False)

        # 7. 写入 MySQL
        final_df.write \
            .format("jdbc") \
            .option("url", mysql_url) \
            .option("driver", mysql_driver) \
            .option("dbtable", mysql_target_table) \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .mode("overwrite") \
            .save()

        print(f"Successfully wrote user image to MySQL table: {mysql_target_table}")

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    spark.stop()
    print("Process finished successfully.")