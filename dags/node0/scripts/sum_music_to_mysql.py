import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, explode

if __name__ == "__main__":
    # 需要更多参数来连接 MySQL
    if len(sys.argv) != 7:
        print("""
        Usage: sum_music_to_mysql.py <dw_table_name> <mysql_url> <mysql_user> <mysql_password> <mysql_driver> <mysql_target_table>
        Example: sum_music_to_mysql.py dw.dw_music jdbc:mysql://host:port/db_name user password com.mysql.cj.jdbc.Driver top_20_businesses
        """, file=sys.stderr)
        sys.exit(-1)

    dw_table_name = sys.argv[1]
    mysql_url = sys.argv[2]
    mysql_user = sys.argv[3]
    mysql_password = sys.argv[4]
    mysql_driver = sys.argv[5]
    mysql_target_table = sys.argv[6]

    # 1. 创建支持 Hive 的 SparkSession
    spark = SparkSession.builder \
        .appName(f"Sum Music to MySQL") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        # 2. 从 DW Hive 表读取数据
        dw_df = spark.table(dw_table_name)
        print(f"Successfully read data from DW table: {dw_table_name}")

        # 3. 计算各个年份歌曲总数
        # sum_music_df = dw_df.groupBy(
        #     "musicbrainz_songs.year[0]"
        # ).count().alias("song_count")

        # sum_music_df = dw_df.groupBy(
        #     "`musicbrainz_songs.year`"
        # ).agg(
        #     count("*").alias("song_count")
        # )

        # 使用 explode 函数展开 musicbrainz_songs 数组
        exploded_df = dw_df.withColumn("mb_song", explode(col("musicbrainz_songs")))

        # 提取 year 字段
        year_df = exploded_df.withColumn("year", col("mb_song.year"))

        # 按 year 字段进行分组和聚合
        sum_music_df = year_df.groupBy("year").agg(count("*").alias("song_count"))

        print("Sum music calculated:")
        sum_music_df.show()

        # 4. 将结果写入 MySQL
        # 使用 .write.jdbc() 方法
        # mode("overwrite") 会在写入前 TRUNCATE TABLE，这对于结果表来说很常用
        sum_music_df.write \
            .format("jdbc") \
            .option("url", mysql_url) \
            .option("driver", mysql_driver) \
            .option("dbtable", mysql_target_table) \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .mode("overwrite") \
            .save()

        print(f"Successfully wrote sum music to MySQL table: {mysql_target_table}")

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    # 5. 停止 SparkSession
    spark.stop()
    print("Process finished successfully.")