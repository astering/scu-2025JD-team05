import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, collect_list, desc, row_number, udf
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.ml.feature import CountVectorizer, VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("AutoKMeansUserClustering") \
        .enableHiveSupport() \
        .getOrCreate()

    dw_users = spark.read.table("dw.dw_users")
    dw_love = spark.read.table("ods.ods_love")
    dw_events = spark.read.table("dw.dw_events")
    dw_tags = spark.read.table("ods_tags")

    user_event_count = dw_events.groupBy("user_id").agg(count("*").alias("event_count"))
    user_love_count = dw_love.groupBy("user_id").agg(count("*").alias("love_count"))
    user_tracks = dw_events.select("user_id", "track_id").distinct()

    track_tags = dw_tags.selectExpr("url", "value as tag") \
        .withColumn("track_id", col("url").substr(-7, 7).cast("bigint")) \
        .drop("url")

    user_tags = user_tracks.join(track_tags, on="track_id", how="inner") \
        .groupBy("user_id", "tag").count()

    window_spec = Window.partitionBy("user_id").orderBy(desc("count"))
    top_user_tags = user_tags.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= 3) \
        .groupBy("user_id") \
        .agg(collect_list("tag").alias("top_tags"))

    user_df = dw_users.select("user_id").distinct() \
        .join(user_event_count, on="user_id", how="left") \
        .join(user_love_count, on="user_id", how="left") \
        .join(top_user_tags, on="user_id", how="left") \
        .fillna({"event_count": 0, "love_count": 0, "top_tags": []})

    def build_user_features(tags, love_count, event_count):
        if event_count == 0:
            love_rate = 0.0
        else:
            love_rate = round(love_count / event_count, 3)
        return json.dumps({
            "top_tags": tags,
            "love_rate": love_rate,
            "event_count": event_count
        })

    build_features_udf = udf(build_user_features, StringType())

    user_df = user_df.withColumn("user_features", build_features_udf(
        col("top_tags"), col("love_count"), col("event_count"))
    )

    user_df = user_df.withColumn("event_count_norm", col("event_count") / 10000.0)
    user_df = user_df.withColumn("love_rate", col("love_count") / (col("event_count") + 1))

    cv = CountVectorizer(inputCol="top_tags", outputCol="tag_vec")
    cv_model = cv.fit(user_df)
    user_df = cv_model.transform(user_df)

    assembler = VectorAssembler(
        inputCols=["tag_vec", "love_rate", "event_count_norm"],
        outputCol="features"
    )
    feature_df = assembler.transform(user_df).cache()

    cost_list = []
    min_k = 2
    max_k = 10
    for k in range(min_k, max_k + 1):
        kmeans = KMeans(k=k, seed=42, featuresCol="features")
        model = kmeans.fit(feature_df)
        cost = model.summary.trainingCost
        cost_list.append((k, cost))
        print(f"K={k}, cost={cost}")

    best_k = min_k
    min_delta = float('inf')
    for i in range(1, len(cost_list)):
        delta = cost_list[i-1][1] - cost_list[i][1]
        if delta < min_delta:
            break
        else:
            min_delta = delta
            best_k = cost_list[i][0]

    print(f"Best K selected: {best_k}")

    final_kmeans = KMeans(k=best_k, seed=42, featuresCol="features", predictionCol="cluster")
    final_model = final_kmeans.fit(feature_df)
    clustered_df = final_model.transform(feature_df).select("user_id", "user_features", "cluster")

    final_model.write().overwrite().save("/mir/kmeans_model_auto")

    dw_users_updated = dw_users.join(clustered_df, on="user_id", how="left")
    dw_users_updated.write.mode("overwrite").saveAsTable("dw.dw_users")

    print("聚类结果已写入 dw_users 表")
    spark.stop()
