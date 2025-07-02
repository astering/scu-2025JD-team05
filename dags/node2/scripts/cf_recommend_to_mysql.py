import sys
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, unix_timestamp, current_timestamp, exp

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: cf_recommend_to_mysql.py <events_path> <love_path> <mysql_url> <mysql_user> <mysql_password>", file=sys.stderr)
        sys.exit(-1)

    events_path, love_path, mysql_url, mysql_user, mysql_password = sys.argv[1:]

    spark = SparkSession.builder.appName("CFRecommendToMySQL").getOrCreate()

    # 1. 读取 dw_events，过滤 playtime=-1，加入时间衰减因子
    events_df_raw = spark.read.parquet(events_path).filter(
        (col("playtime").isNotNull()) & (col("playtime") > 0) & (col("timestamp").isNotNull())
    )

    # 计算衰减因子：decay = exp(-lambda * delta_days)
    lambda_val = 0.01  # 控制衰减速度的系数，可调节
    events_df = events_df_raw.withColumn("days_ago",
        (unix_timestamp(current_timestamp()) - col("timestamp")) / 86400.0
    ).withColumn("decay", exp(-lambda_val * col("days_ago"))) \
     .withColumn("rating", (col("playtime") * col("decay")).cast("float")) \
     .select(col("user_id").cast("int"), col("track_id").cast("int"), "rating")

    # 2. 读取 dw_love，构造偏好度为1.5
    love_df = spark.read.parquet(love_path) \
        .select(col("user_id").cast("int"), col("track_id").cast("int")) \
        .withColumn("rating", col("user_id") * 0 + 1.5)

    # 3. 合并两个评分来源
    rating_df = events_df.union(love_df).dropna()

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

    # 5. 获取每个用户的Top 10推荐
    recommend_df = model.recommendForAllUsers(10)

    # 6. 展开推荐结果
    from pyspark.sql.functions import explode
    final_df = recommend_df.withColumn("rec", explode("recommendations")) \
        .select(
            col("user_id"),
            col("rec.track_id").alias("track_id"),
            col("rec.rating").alias("pred_score")
        )

    # 7. 写入 MySQL
    final_df.write \
        .format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", "cf_recommend_result") \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("overwrite") \
        .save()

    print("协同过滤推荐结果已成功写入 MySQL 表 cf_recommend_result")
    spark.stop()

