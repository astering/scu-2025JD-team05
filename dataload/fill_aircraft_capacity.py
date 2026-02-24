# -*- coding: utf-8 -*-
"""
fill_aircraft_capacity_from_csv.py
- 读取 data.csv（包含列：机型、人数）
- 统计：同一机型 -> 最大人数
- 写入 MySQL 表 aircraft_capacity
"""

import os
import re
import math
import pandas as pd
import pymysql

# ========= 1) 配置：改这里 =========
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "bishe",
    "port": 3306,
    "charset": "utf8mb4",
    "collation": "utf8mb4_0900_ai_ci",
    "autocommit": False,
}


CSV_PATH = r"D:\data\中国航空航班数据.csv"   # 改成你的csv路径
TABLE_NAME = "aircraft_capacity"

AIRCRAFT_COL = "机型"
PEOPLE_COL = "人数"

# 批量写入大小
BATCH_SIZE = 2000
# ==================================


def read_csv_robust(path: str) -> pd.DataFrame:
    """尝试多种常见编码读取 CSV。"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"找不到CSV文件：{path}")

    encodings = ["utf-8", "utf-8-sig"]
    last_err = None
    for enc in encodings:
        try:
            df = pd.read_csv(path, encoding=enc)
            return df
        except Exception as e:
            last_err = e
    raise RuntimeError(f"读取CSV失败（已尝试 {encodings}）：{last_err}")


def norm_aircraft(x) -> str | None:
    """机型清洗：去空格、统一空白；空值返回 None。"""
    if x is None:
        return None
    if isinstance(x, float) and math.isnan(x):
        return None
    s = str(x).strip()
    if not s:
        return None
    s = re.sub(r"\s+", " ", s)
    return s


def parse_people(x) -> int | None:
    """
    人数清洗：从 '215' / '215人' / '约215' / 215.0 等抽取整数。
    返回 None 表示无效。
    """
    if x is None:
        return None
    if isinstance(x, float) and math.isnan(x):
        return None
    s = str(x).strip()
    if not s:
        return None
    m = re.search(r"\d+", s)
    if not m:
        return None
    v = int(m.group(0))
    if v <= 0:
        return None
    return v


def chunked(iterable, n: int):
    buf = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf


def upsert_aircraft_capacity(conn, rows: list[tuple[str, int]], source_file: str | None):
    """
    rows: [(aircraft_type, max_people), ...]
    UPSERT：同机型更新为更大的 max_people
    """
    sql = f"""
    INSERT INTO {TABLE_NAME} (aircraft_type, max_people, source_file)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
        max_people = GREATEST(max_people, VALUES(max_people)),
        source_file = VALUES(source_file);
    """

    with conn.cursor() as cur:
        for batch in chunked(rows, BATCH_SIZE):
            cur.executemany(sql, [(a, p, source_file) for a, p in batch])
    conn.commit()


def main():
    df = read_csv_robust(CSV_PATH)
    print(f"[INFO] CSV rows={len(df)} cols={len(df.columns)}")

    # 检查必需列
    missing = [c for c in [AIRCRAFT_COL, PEOPLE_COL] if c not in df.columns]
    if missing:
        raise KeyError(f"CSV 缺少列：{missing}\n实际列名：{list(df.columns)}")

    # 清洗
    df["_aircraft"] = df[AIRCRAFT_COL].apply(norm_aircraft)
    df["_people"] = df[PEOPLE_COL].apply(parse_people)

    df2 = df.dropna(subset=["_aircraft", "_people"]).copy()
    print(f"[INFO] valid rows (aircraft & people) = {len(df2)}")

    if df2.empty:
        print("[WARN] 没有可用数据（机型/人数均有效的行数为0），不写入数据库。")
        return

    # 聚合：同一机型取最大人数
    agg = (
        df2.groupby("_aircraft", as_index=False)["_people"]
        .max()
        .rename(columns={"_aircraft": "aircraft_type", "_people": "max_people"})
        .sort_values("max_people", ascending=False)
    )

    rows = list(agg.itertuples(index=False, name=None))  # [(aircraft_type, max_people), ...]
    print(f"[INFO] unique aircraft types = {len(rows)}")
    print("[INFO] top 10:")
    print(agg.head(10).to_string(index=False))

    # 写入 MySQL
    conn = pymysql.connect(**DB_CONFIG)
    try:
        upsert_aircraft_capacity(conn, rows, source_file=os.path.basename(CSV_PATH))
        print(f"[OK] upsert done: {len(rows)} rows -> {TABLE_NAME}")
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
