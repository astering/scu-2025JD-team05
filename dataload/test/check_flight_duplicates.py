# -*- coding: utf-8 -*-
"""
check_flight_duplicates.py

功能：
1) 以 (航班班次, 日期, 起飞时间) 为 key
2) 找出 CSV 中完全重复的记录
3) 输出这些重复记录在 CSV 中的原始行号（从 1 开始，不含表头）
"""

import pandas as pd
from collections import defaultdict

# ===============================
# 配置
# ===============================
CSV_FILE = r"D:\data\中国航空航班数据.csv"
ENCODING = "utf-8-sig"
SEP = ","

TOP_K = 20          # 最多输出多少个重复 key
MAX_LINES_SHOW = 50 # 每个 key 最多显示多少个行号

# ===============================
# 主逻辑
# ===============================
def main():
    print("[load] reading csv ...")
    df = pd.read_csv(CSV_FILE, encoding=ENCODING, sep=SEP, low_memory=False)

    # 自动识别列
    col_fno = next(c for c in ["航班班次", "航班号", "航班", "flight_no"] if c in df.columns)
    col_date = "日期"
    col_dep_time = "起飞时间"

    print(f"[cols] flight_no={col_fno}, date={col_date}, dep_time={col_dep_time}")

    total_rows = len(df)

    # key -> [row_numbers]
    key_to_lines = defaultdict(list)

    print("[scan] scanning rows ...")
    for idx, row in df.iterrows():
        # CSV 行号说明：
        # idx 从 0 开始；+2 = Excel 里看到的行号（1 是表头）
        csv_line_no = idx + 2

        key = (
            str(row[col_fno]).strip(),
            str(row[col_date]).strip(),
            str(row[col_dep_time]).strip()
        )
        key_to_lines[key].append(csv_line_no)

    # 筛选真正重复的 key
    dup_items = [(k, v) for k, v in key_to_lines.items() if len(v) > 1]
    dup_items.sort(key=lambda x: len(x[1]), reverse=True)

    print("\n===== 统计结果 =====")
    print(f"CSV 原始总行数: {total_rows}")
    print(f"重复 key 数量: {len(dup_items)}")
    print(f"涉及重复的记录总行数: {sum(len(v) for _, v in dup_items)}")

    print("\n===== 重复 key 及其 CSV 行号（TOP） =====")
    for i, (key, lines) in enumerate(dup_items[:TOP_K], start=1):
        fno, date, dep_time = key
        print(
            f"\n[{i}] 重复次数={len(lines)} | "
            f"航班={fno} | 日期={date} | 起飞时间={dep_time}"
        )

        if len(lines) <= MAX_LINES_SHOW:
            print("  CSV 行号:", lines)
        else:
            print("  CSV 行号(前部分):", lines[:MAX_LINES_SHOW], "...")

    print("\n[done] duplicate scan finished.")

if __name__ == "__main__":
    main()
