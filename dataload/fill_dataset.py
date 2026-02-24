# -*- coding: utf-8 -*-
"""
csv_to_dataset.py

功能：
- 读取 CSV
- 按 (flight_no, flight_date, departure_time) 去重，仅保留第一条（与 dataset 唯一键一致）
- 自动拆分 airline_iata_code / flight_number
- 解析 avg_delay_min（支持 "42分钟" 等）
- 解析准点率/里程/价格/人数等数值，统一 NaN -> NULL
- 生成 week_bitmap（bit0=周一 ... bit6=周日）
- 批量写入 dataset 表（INSERT IGNORE：遇到重复键自动跳过）
"""

import math
import re
import sys
import time
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

import pandas as pd
import pymysql

# ===============================
# 0) 配置
# ===============================
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "bishe",
    "charset": "utf8mb4",
    "collation": "utf8mb4_0900_ai_ci",
    "autocommit": False,
}

CSV_FILE = r"D:\data\中国航空航班数据.csv"
CSV_ENCODING = "utf-8-sig"
CSV_SEP = ","

BATCH_ROWS = 20000
PROGRESS_EVERY = 50000

PRICE_Q = Decimal("0.01")

# ===============================
# 1) 工具函数
# ===============================
def safe_print(*args):
    try:
        print(*args)
    except UnicodeEncodeError:
        sys.stdout.buffer.write((" ".join(map(str, args)) + "\n").encode("utf-8", "replace"))

def nan_to_none(x):
    if x is None:
        return None
    if isinstance(x, float) and math.isnan(x):
        return None
    if pd.isna(x):
        return None
    return x

def split_flight_no(fno):
    if fno is None:
        return None, None, None
    s = str(fno).strip().upper()
    m = re.match(r"^([A-Z0-9]{2})([0-9A-Z]{1,6})$", s)
    if not m:
        return None, None, None
    return s, m.group(1), m.group(2)

def parse_date(s):
    s = str(s).strip()
    if "/" in s:
        return datetime.strptime(s, "%Y/%m/%d").date()
    return datetime.strptime(s, "%Y-%m-%d").date()

def parse_time(s):
    s = str(s).strip()
    if re.match(r"^\d:\d\d:\d\d$", s):
        s = "0" + s
    return datetime.strptime(s, "%H:%M:%S").time()

def parse_int(x, default=None):
    if x is None or pd.isna(x):
        return default
    s = str(x).strip()
    if not s:
        return default
    m = re.search(r"-?\d+", s)
    if not m:
        return default
    try:
        return int(m.group(0))
    except Exception:
        return default

def parse_float(x, default=None):
    if x is None or pd.isna(x):
        return default
    s = str(x).strip()
    if not s:
        return default
    try:
        return float(s)
    except Exception:
        return default

def parse_float01(x, default=None):
    v = parse_float(x, default=default)
    if v is None:
        return default
    if v < 0:
        v = 0.0
    if v > 1:
        v = 1.0
    return v

def parse_delay_min(x, default=None):
    if x is None or pd.isna(x):
        return default
    s = str(x).strip()
    if not s:
        return default
    m = re.search(r"(\d+)", s)
    if not m:
        return default
    v = int(m.group(1))
    if v < 0 or v > 2000:
        return default
    return v

def parse_price_decimal(x, default=None):
    if x is None or pd.isna(x):
        return default
    s = str(x).strip()
    if not s:
        return default
    m = re.search(r"(\d+(\.\d+)?)", s)
    if not m:
        return default
    try:
        return Decimal(m.group(1)).quantize(PRICE_Q, rounding=ROUND_HALF_UP)
    except Exception:
        return default

def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"缺少列：{candidates}，当前列：{list(df.columns)}")

# ===============================
# ✅ 修复点：班期判断
# ===============================
def _has_schedule(cell) -> bool:
    """
    你的数据形态：
      - '周一有班期'
      - '周二没有班期'
    关键：'没有班期' 也包含子串 '有班期'，因此必须排除。
    """
    if cell is None or pd.isna(cell):
        return False
    s = str(cell).strip()
    return ("有班期" in s) and ("没有班期" not in s)

def build_week_bitmap(row):
    bm = 0
    if _has_schedule(row.get("周一班期")): bm |= 1 << 0
    if _has_schedule(row.get("周二班期")): bm |= 1 << 1
    if _has_schedule(row.get("周三班期")): bm |= 1 << 2
    if _has_schedule(row.get("周四班期")): bm |= 1 << 3
    if _has_schedule(row.get("周五班期")): bm |= 1 << 4
    if _has_schedule(row.get("周六班期")): bm |= 1 << 5
    if _has_schedule(row.get("周日班期")): bm |= 1 << 6
    return bm

# ===============================
# 2) SQL
# ===============================
SQL_INSERT_DATASET = """
INSERT IGNORE INTO dataset (
    csv_row_no, ingest_batch_id,
    flight_date, flight_no, airline_iata_code, flight_number,
    departure_time, arrival_time,
    dep_city_name, dep_province_name,
    arr_city_name, arr_province_name,
    dep_airport_name, dep_airport_lat, dep_airport_lon,
    arr_airport_name, arr_airport_lat, arr_airport_lon,
    dep_city_lat, dep_city_lon,
    arr_city_lat, arr_city_lon,
    distance_km,
    on_time_rate, avg_delay_min, price, people,
    week_bitmap
) VALUES (
    %s,%s,
    %s,%s,%s,%s,
    %s,%s,
    %s,%s,
    %s,%s,
    %s,%s,%s,
    %s,%s,%s,
    %s,%s,
    %s,%s,
    %s,
    %s,%s,%s,%s,
    %s
)
"""

# ===============================
# 3) 主流程
# ===============================
def main():
    safe_print("[load] reading csv ...")
    df = pd.read_csv(CSV_FILE, encoding=CSV_ENCODING, sep=CSV_SEP, low_memory=False)

    col_fno = pick_col(df, ["航班班次", "航班号", "航班", "flight_no"])
    col_date = pick_col(df, ["日期"])
    col_dep_time = pick_col(df, ["起飞时间"])
    col_arr_time = pick_col(df, ["降落时间"])

    col_dep_city = next((c for c in ["出发城市", "城市_x"] if c in df.columns), None)
    col_arr_city = next((c for c in ["到达城市", "城市_y"] if c in df.columns), None)
    col_dep_prov = next((c for c in ["出发省份"] if c in df.columns), None)
    col_arr_prov = next((c for c in ["到达省份"] if c in df.columns), None)

    col_dep_airport = next((c for c in ["起飞机场"] if c in df.columns), None)
    col_arr_airport = next((c for c in ["降落机场"] if c in df.columns), None)
    col_dep_airport_lat = next((c for c in ["起飞机场y"] if c in df.columns), None)
    col_dep_airport_lon = next((c for c in ["起飞机场x"] if c in df.columns), None)
    col_arr_airport_lat = next((c for c in ["降落机场y"] if c in df.columns), None)
    col_arr_airport_lon = next((c for c in ["降落机场x"] if c in df.columns), None)

    col_dep_city_lat = next((c for c in ["出发城市y"] if c in df.columns), None)
    col_dep_city_lon = next((c for c in ["出发城市x"] if c in df.columns), None)
    col_arr_city_lat = next((c for c in ["到达城市y"] if c in df.columns), None)
    col_arr_city_lon = next((c for c in ["到达城市x"] if c in df.columns), None)

    col_dist = next((c for c in ["里程（公里）", "里程(公里)", "里程"] if c in df.columns), None)
    col_otr = next((c for c in ["准点率", "on_time_rate"] if c in df.columns), None)
    col_delay = next((c for c in ["平均误点时间", "平均延误时间", "avg_delay"] if c in df.columns), None)
    col_price = next((c for c in ["价格(元)", "价格（元）", "价格"] if c in df.columns), None)
    col_people = next((c for c in ["人数", "乘机人数", "people"] if c in df.columns), None)

    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor()

    ingest_batch_id = datetime.now().strftime("csv_%Y%m%d_%H%M%S")

    seen_keys = set()
    payload = []

    total = len(df)
    inserted_attempt = 0
    inserted_effective = 0
    dup_skip_mem = 0
    dup_skip_db = 0
    bad_fno = 0
    bad_date = 0
    bad_time = 0

    # 额外：统计 bitmap 分布，方便你快速验证不再全是 127
    bitmap_cnt = {}

    t0 = time.time()

    try:
        for idx, row in df.iterrows():
            csv_row_no = idx + 1

            flight_no, airline_iata, flight_number = split_flight_no(row.get(col_fno))
            if not flight_no:
                bad_fno += 1
                continue

            try:
                d = parse_date(row.get(col_date))
            except Exception:
                bad_date += 1
                continue

            try:
                dep_t = parse_time(row.get(col_dep_time))
                arr_t = parse_time(row.get(col_arr_time))
            except Exception:
                bad_time += 1
                continue

            key = (flight_no, d, dep_t)
            if key in seen_keys:
                dup_skip_mem += 1
                continue
            seen_keys.add(key)

            week_bitmap = build_week_bitmap(row)
            bitmap_cnt[week_bitmap] = bitmap_cnt.get(week_bitmap, 0) + 1

            distance_km = parse_int(row.get(col_dist), default=None) if col_dist else None
            on_time_rate = parse_float01(row.get(col_otr), default=None) if col_otr else None
            avg_delay_min = parse_delay_min(row.get(col_delay), default=None) if col_delay else None
            price = parse_price_decimal(row.get(col_price), default=None) if col_price else None
            people = parse_int(row.get(col_people), default=None) if col_people else None

            payload.append((
                csv_row_no, ingest_batch_id,
                d, flight_no, airline_iata, flight_number,
                dep_t, arr_t,

                nan_to_none(row.get(col_dep_city)) if col_dep_city else None,
                nan_to_none(row.get(col_dep_prov)) if col_dep_prov else None,

                nan_to_none(row.get(col_arr_city)) if col_arr_city else None,
                nan_to_none(row.get(col_arr_prov)) if col_arr_prov else None,

                nan_to_none(row.get(col_dep_airport)) if col_dep_airport else None,
                nan_to_none(row.get(col_dep_airport_lat)) if col_dep_airport_lat else None,
                nan_to_none(row.get(col_dep_airport_lon)) if col_dep_airport_lon else None,

                nan_to_none(row.get(col_arr_airport)) if col_arr_airport else None,
                nan_to_none(row.get(col_arr_airport_lat)) if col_arr_airport_lat else None,
                nan_to_none(row.get(col_arr_airport_lon)) if col_arr_airport_lon else None,

                nan_to_none(row.get(col_dep_city_lat)) if col_dep_city_lat else None,
                nan_to_none(row.get(col_dep_city_lon)) if col_dep_city_lon else None,

                nan_to_none(row.get(col_arr_city_lat)) if col_arr_city_lat else None,
                nan_to_none(row.get(col_arr_city_lon)) if col_arr_city_lon else None,

                distance_km,
                on_time_rate, avg_delay_min, price, people,
                int(week_bitmap),
            ))

            if len(payload) >= BATCH_ROWS:
                cur.executemany(SQL_INSERT_DATASET, payload)
                conn.commit()

                inserted_attempt += len(payload)
                inserted_effective += cur.rowcount
                dup_skip_db += (len(payload) - cur.rowcount)
                payload.clear()

            if (idx + 1) % PROGRESS_EVERY == 0:
                elapsed = time.time() - t0
                speed = (idx + 1) / elapsed if elapsed > 0 else 0
                safe_print(
                    f"[progress] {idx+1}/{total} "
                    f"attempt={inserted_attempt} inserted={inserted_effective} "
                    f"dup_skip_mem={dup_skip_mem} dup_skip_db≈{dup_skip_db} "
                    f"bad_fno={bad_fno} bad_date={bad_date} bad_time={bad_time} "
                    f"speed={speed:.1f} rows/s"
                )

        if payload:
            cur.executemany(SQL_INSERT_DATASET, payload)
            conn.commit()
            inserted_attempt += len(payload)
            inserted_effective += cur.rowcount
            dup_skip_db += (len(payload) - cur.rowcount)
            payload.clear()

        elapsed = time.time() - t0
        safe_print("\n=== DONE ===")
        safe_print(f"CSV rows                 : {total}")
        safe_print(f"Unique keys kept(mem)    : {len(seen_keys)}")
        safe_print(f"Inserted attempt         : {inserted_attempt}")
        safe_print(f"Inserted effective       : {inserted_effective}")
        safe_print(f"Duplicate skipped (mem)  : {dup_skip_mem}")
        safe_print(f"Duplicate skipped (DB≈)  : {dup_skip_db}")
        safe_print(f"Bad flight_no            : {bad_fno}")
        safe_print(f"Bad date                 : {bad_date}")
        safe_print(f"Bad time                 : {bad_time}")
        safe_print(f"Elapsed                  : {elapsed:.1f}s")
        safe_print(f"Avg speed                : {total/elapsed:.1f} rows/s")

        # 打印 bitmap TOP 分布
        top = sorted(bitmap_cnt.items(), key=lambda x: x[1], reverse=True)[:15]
        safe_print("\n=== week_bitmap TOP ===")
        for bm, cnt in top:
            safe_print(f"{bm:3d} (0b{bm:07b}) : {cnt}")

    except Exception as e:
        conn.rollback()
        safe_print("[ERROR] import failed:", repr(e))
        raise
    finally:
        try:
            cur.close()
        finally:
            conn.close()

if __name__ == "__main__":
    main()
