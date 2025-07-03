import sys
import logging
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, unix_timestamp, current_timestamp, exp, explode

def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(__name__)

    if len(sys.argv) != 8:
        logger.error(
            "Usage: cf_recommend_to_mysql.py <events_path> <love_path> <mysql_url> <mysql_user> <mysql_password> <tracks_path> <users_path>")
        sys.exit(-1)

    events_path, love_path, mysql_url, mysql_user, mysql_password, tracks_path, users_path = sys.argv[1:]
    logger.info(f"Starting CF recommend job with events_path={events_path}, love_path={love_path}")

    spark = SparkSession.builder.appName("CFRecommendToMySQL").getOrCreate()

    try:
        # 1. 读取 dw_events，过滤 playtime=-1，加入时间衰减因子
        events_df_raw = spark.read.parquet(events_path).filter(
            (col("playtime").isNotNull()) & (col("playtime") > 0) & (col("timestamp").isNotNull())
        )
        logger.info(f"Events data count: {events_df_raw.count()}")

        lambda_val = 0.01
        events_df = events_df_raw.withColumn("days_ago",
            (unix_timestamp(current_timestamp()) - col("timestamp")) / 86400.0
        ).withColumn("decay", exp(-lambda_val * col("days_ago"))) \
         .withColumn("rating", (col("playtime") * col("decay")).cast("float")) \
         .select(col("user_id").cast("int"), col("track_id").cast("int"), "rating")

        # 2. 读取 dw_love，构造偏好度为2.0
        love_df = spark.read.parquet(love_path) \
            .select(col("user_id").cast("int"), col("track_id").cast("int")) \
            .withColumn("rating", col("user_id") * 0 + 2.0)
        logger.info(f"Love data count: {love_df.count()}")

        # 3. 合并两个评分来源
        rating_df = events_df.union(love_df).dropna()
        logger.info(f"Total rating data count after union: {rating_df.count()}")

        # 4. 构建ALS模型
        als = ALS(
            userCol="user_id",
            itemCol="track_id",
            ratingCol="rating",
            rank=10,
            maxIter=10,
            regParam=0.1,
            coldStartStrategy="drop"
        )
        model = als.fit(rating_df)
        logger.info("ALS model training completed")

        # 5. 获取每个用户的Top 10推荐
        recommend_df = model.recommendForAllUsers(10)
        logger.info("Recommendation generation completed")

        # 6. 展开推荐结果
        final_df = recommend_df.withColumn("rec", explode("recommendations")) \
            .select(
                col("user_id"),
                col("rec.track_id").alias("track_id"),
                col("rec.rating").alias("pred_score")
            )

        # 7. 读取 tracks 表并 join
        tracks_df = spark.read.parquet(tracks_path) \
            .select(col("track_id").cast("int"), col("name").alias("track_name"))

        # 8. 读取 users 表并 join
        users_df = spark.read.parquet(users_path) \
            .select(col("user_id").cast("int"), col("lastfm_username").alias("user_name"))

        final_df = final_df \
            .join(tracks_df, on="track_id", how="left") \
            .join(users_df, on="user_id", how="left")

        # 9. 指定字段顺序
        final_df = final_df.select("user_id", "user_name", "track_id", "track_name", "pred_score")

        # 10. 写入 MySQL
        final_df.write \
            .format("jdbc") \
            .option("url", mysql_url) \
            .option("dbtable", "cf_recommend_result") \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("batchsize", 10000) \
            .option("numPartitions", 10) \
            .mode("overwrite") \
            .save()

        logger.info("协同过滤推荐结果（含用户名与歌曲名）已成功写入 MySQL 表 cf_recommend_result")

    except Exception as e:
        logger.error("任务执行失败，异常信息:", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("SparkSession 已停止")

if __name__ == "__main__":
    main()
