import pandas as pd
import re
from datetime import datetime

# 配置文件路径和列名
CSV_FILE = r"D:\data\中国航空航班数据.csv"  # 修改为你的实际路径
DATE_COLUMN = "日期"  # 修改为你的实际日期列名

def find_invalid_dates():
    # 读取 CSV 文件
    df = pd.read_csv(CSV_FILE, encoding="utf-8-sig", sep=",", low_memory=False)

    # 确保日期列存在
    if DATE_COLUMN not in df.columns:
        print(f"没有找到日期列 '{DATE_COLUMN}'")
        return

    invalid_rows = []  # 用来存储包含无效日期的记录
    # 输出前几行日期数据，检查可能存在的问题
    print(f"前 10 行日期数据预览：")
    print(df[DATE_COLUMN].head(10))

    for i, row in df.iterrows():
        date_str = str(row[DATE_COLUMN]).strip()

        # 检查日期是否是有效日期格式
        try:
            # 清除日期字符串后面的非日期部分（如：2024/12/32之后可能有空格或字符）
            date_str_cleaned = re.sub(r"\s+.*", "", date_str)
            # 尝试解析日期
            date_obj = datetime.strptime(date_str_cleaned, "%Y/%m/%d")
        except ValueError:
            # 如果解析失败，记录下该行
            invalid_rows.append((i, date_str))

    # 输出包含无效日期的记录
    if invalid_rows:
        print(f"共找到 {len(invalid_rows)} 条无效日期记录：")
        for idx, date_str in invalid_rows:
            print(f"第 {idx+1} 行 日期: {date_str}")
    else:
        print("没有找到无效日期记录。")

# 执行测试
find_invalid_dates()
