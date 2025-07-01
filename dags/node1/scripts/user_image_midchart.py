from pyspark.sql import SparkSession
import json

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("ExtractUserImage") \
    .getOrCreate()

# 读取参数
import sys
input_path = sys.argv[1]  # 例如: hdfs://node-master:9000/mir/ThirtyMusic/relations/session.idomaar
output_path = sys.argv[2]  # 例如: hdfs://node-master:9000/user_image_midchart.txt

# 读取 session.idomaar 文件
rdd = spark.sparkContext.textFile(input_path)

def extract_info(line):
    try:
        parts = line.strip().split('\t')
        if len(parts) < 5:
            return []
        raw_json = parts[4]
        json_data = json.loads(raw_json)

        subject_id = None
        for subject in json_data.get("subjects", []):
            if subject.get("type") == "user":
                subject_id = subject.get("id")
                break
        if subject_id is None:
            return []

        results = []
        for obj in json_data.get("objects", []):
            obj_fields = [f"{k}:{v}" for k, v in obj.items()]
            line_str = f"user_id:{subject_id} " + " ".join(obj_fields)
            results.append(line_str)
        return results
    except Exception as e:
        return []

# 提取用户与播放记录
result_rdd = rdd.flatMap(extract_info)

# 保存结果
result_rdd.saveAsTextFile(output_path)

spark.stop()
