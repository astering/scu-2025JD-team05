# -*- coding: utf-8 -*-
"""
fill_flight_only.py
- 读取航班数据（csv/excel）
- 使用 AirportResolver 解析 dep/arr 机场 -> airport_id
- 写入 flight 表（含 aircraft_type、flight_popularity 初始计算）
【注意】本版本：只写 flight，不写 city_hotness_monthly、不回填 city_airport
"""

import re
import sys
import math
import time
from datetime import datetime, timedelta
from collections import defaultdict

import pandas as pd
import pymysql

from airport_resolver import AirportResolver

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

DATA_FILE = r"D:\data\中国航空航班数据.csv"
DATA_TYPE = "csv"  # "csv" or "excel"

CSV_ENCODING = "utf-8-sig"
CSV_SEP = ","

PROGRESS_EVERY = 5000
BATCH_ROWS = 20000

MISS_LOG_LIMIT = 30

# ========== flight_popularity 参数（可调）==========
MAX_DELAY_MIN = 120.0
MAX_PEOPLE = 220.0
W_ON_TIME = 0.45
W_DELAY = 0.25
W_FREQ = 0.15
W_DEMAND = 0.15

# ===============================
# 工具
# ===============================
def safe_print(*args):
    try:
        print(*args)
    except UnicodeEncodeError:
        sys.stdout.buffer.write((" ".join(map(str, args)) + "\n").encode("utf-8", "replace"))

def load_df():
    if DATA_TYPE.lower() == "csv":
        return pd.read_csv(DATA_FILE, encoding=CSV_ENCODING, sep=CSV_SEP, low_memory=False)
    return pd.read_excel(DATA_FILE)

def pick_col(df, names):
    for n in names:
        if n in df.columns:
            return n
    raise KeyError(f"缺少列：{names}，当前列：{list(df.columns)}")

_DATE_CACHE = {}
_TIME_CACHE = {}

def parse_date(s):
    key = str(s).strip()
    v = _DATE_CACHE.get(key)
    if v is not None:
        return v
    if "/" in key:
        d = datetime.strptime(key, "%Y/%m/%d").date()
    else:
        d = datetime.strptime(key, "%Y-%m-%d").date()
    _DATE_CACHE[key] = d
    return d

def parse_time(s):
    key = str(s).strip()
    v = _TIME_CACHE.get(key)
    if v is not None:
        return v
    if re.match(r"^\d:\d\d:\d\d$", key):
        key2 = "0" + key
    else:
        key2 = key
    t = datetime.strptime(key2, "%H:%M:%S").time()
    _TIME_CACHE[key] = t
    return t

def split_flight_no(fno):
    fno = str(fno).strip().upper()
    m = re.match(r"^([A-Z0-9]{2})([0-9A-Z]{1,6})$", fno)
    if not m:
        return None, None, None
    return fno, m.group(1), m.group(2)

def parse_delay_min(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return 0
    s = str(x).strip()
    if not s:
        return 0
    m = re.search(r"(\d+)", s)
    return int(m.group(1)) if m else 0

def parse_on_time_rate(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return 0.70
    s = str(x).strip()
    if not s:
        return 0.70
    try:
        v = float(s)
        if v < 0:
            v = 0.0
        if v > 1:
            v = 1.0
        return v
    except Exception:
        return 0.70

def safe_int(x, default=0):
    try:
        if x is None:
            return default
        if isinstance(x, float) and pd.isna(x):
            return default
        return int(float(x))
    except Exception:
        return default

def calc_flight_popularity(on_time_rate, avg_delay_min, freq_per_week, people):
    on_time = float(on_time_rate)
    delay_norm = min(max(float(avg_delay_min) / MAX_DELAY_MIN, 0.0), 1.0)
    freq_norm = min(max(float(freq_per_week) / 7.0, 0.0), 1.0)

    try:
        people = float(people)
        if math.isnan(people):
            people = 0.0
    except Exception:
        people = 0.0

    people_norm = min(max(people / MAX_PEOPLE, 0.0), 1.0)

    score = (
        W_ON_TIME * on_time
        + W_DELAY * (1.0 - delay_norm)
        + W_FREQ * freq_norm
        + W_DEMAND * people_norm
    )
    return float(min(max(score, 0.0), 1.0))

# ===============================
# DB SQL（仅 flight）
# ===============================
SQL_INSERT_FLIGHT = """
INSERT INTO flight
    (flight_date, flight_no, aircraft_type, airline_iata_code, flight_number,
     departure_airport_id, arrival_airport_id,
     departure_time, arrival_time, duration, is_active, flight_popularity)
VALUES
    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
    aircraft_type=VALUES(aircraft_type),
    departure_airport_id=VALUES(departure_airport_id),
    arrival_airport_id=VALUES(arrival_airport_id),
    departure_time=VALUES(departure_time),
    arrival_time=VALUES(arrival_time),
    duration=VALUES(duration),
    is_active=VALUES(is_active),
    flight_popularity=VALUES(flight_popularity)
"""

def insert_batch(conn, payload):
    with conn.cursor() as cur:
        cur.executemany(SQL_INSERT_FLIGHT, payload)
    return len(payload)

def main():
    df = load_df()
    conn = pymysql.connect(**DB_CONFIG)

    try:
        resolver = AirportResolver(conn)

        # ========== 1) 列定位 ==========
        col_dep_airport = pick_col(df, ["起飞机场"])
        col_arr_airport = pick_col(df, ["降落机场"])
        col_dep_y = pick_col(df, ["起飞机场y"])
        col_dep_x = pick_col(df, ["起飞机场x"])
        col_arr_y = pick_col(df, ["降落机场y"])
        col_arr_x = pick_col(df, ["降落机场x"])

        col_date = pick_col(df, ["日期"])
        col_dep_time = pick_col(df, ["起飞时间"])
        col_arr_time = pick_col(df, ["降落时间"])
        col_fno = pick_col(df, ["航班班次", "航班号", "航班", "flight_no"])

        col_aircraft = next((c for c in ["机型", "aircraft_type", "飞机型号"] if c in df.columns), None)
        col_on_time = next((c for c in ["准点率", "on_time_rate"] if c in df.columns), None)
        col_delay = next((c for c in ["平均误点时间", "平均延误时间", "avg_delay"] if c in df.columns), None)
        col_people = next((c for c in ["人数", "乘机人数", "people"] if c in df.columns), None)

        week_cols = [c for c in ["周一班期","周二班期","周三班期","周四班期","周五班期","周六班期","周日班期"] if c in df.columns]

        total = len(df)
        safe_print(f"[fill_flight_only] rows={total} batch={BATCH_ROWS}")

        # ========== 2) 统计容器 ==========
        inserted = 0
        miss_airport = 0
        bad_fno = 0

        dep_reason_cnt = defaultdict(int)
        arr_reason_cnt = defaultdict(int)
        miss_pair_cnt = defaultdict(int)

        miss_logged = 0
        payload = []
        t0 = time.time()

        # 为 itertuples 准备索引（更快）
        cols = list(df.columns)
        idx_dep_airport = cols.index(col_dep_airport)
        idx_arr_airport = cols.index(col_arr_airport)
        idx_dep_y = cols.index(col_dep_y)
        idx_dep_x = cols.index(col_dep_x)
        idx_arr_y = cols.index(col_arr_y)
        idx_arr_x = cols.index(col_arr_x)

        idx_date = cols.index(col_date)
        idx_dep_time = cols.index(col_dep_time)
        idx_arr_time = cols.index(col_arr_time)
        idx_fno = cols.index(col_fno)

        idx_aircraft = cols.index(col_aircraft) if col_aircraft else None
        idx_on_time = cols.index(col_on_time) if col_on_time else None
        idx_delay = cols.index(col_delay) if col_delay else None
        idx_people = cols.index(col_people) if col_people else None

        week_idx = [cols.index(c) for c in week_cols] if week_cols else []

        # ========== 3) 主循环：只写 flight ==========
        for i, row in enumerate(df.itertuples(index=False, name=None), start=1):
            dep_name = row[idx_dep_airport]
            arr_name = row[idx_arr_airport]
            dep_lat, dep_lon = row[idx_dep_y], row[idx_dep_x]
            arr_lat, arr_lon = row[idx_arr_y], row[idx_arr_x]

            dep_id, dep_reason = resolver.resolve(dep_lat, dep_lon, dep_name)
            arr_id, arr_reason = resolver.resolve(arr_lat, arr_lon, arr_name)

            dep_reason_cnt[dep_reason] += 1
            arr_reason_cnt[arr_reason] += 1
            miss_pair_cnt[(dep_reason, arr_reason)] += 1

            if dep_id is None or arr_id is None:
                miss_airport += 1
                if miss_logged < MISS_LOG_LIMIT:
                    miss_logged += 1
                    safe_print(
                        "[MISS_AIRPORT]",
                        f"i={i}",
                        f"DEP(raw={dep_name!r}, lat={dep_lat}, lon={dep_lon}, reason={dep_reason})",
                        f"ARR(raw={arr_name!r}, lat={arr_lat}, lon={arr_lon}, reason={arr_reason})",
                        f"miss_side={'DEP' if dep_id is None else ''}{'&' if dep_id is None and arr_id is None else ''}{'ARR' if arr_id is None else ''}"
                    )
                continue

            flight_no, airline_iata, flight_number = split_flight_no(row[idx_fno])
            if not flight_no:
                bad_fno += 1
                continue

            d = parse_date(row[idx_date])

            dep_t = parse_time(row[idx_dep_time])
            arr_t = parse_time(row[idx_arr_time])

            dep_dt = datetime.combine(d, dep_t)
            arr_dt = datetime.combine(d, arr_t)
            if arr_dt <= dep_dt:
                arr_dt += timedelta(days=1)
            duration_min = int(round((arr_dt - dep_dt).total_seconds() / 60.0))

            aircraft_type = "未知"
            if idx_aircraft is not None:
                v = row[idx_aircraft]
                if not (isinstance(v, float) and pd.isna(v)):
                    s = str(v).strip()
                    if s:
                        aircraft_type = s[:30]

            on_time_rate = parse_on_time_rate(row[idx_on_time]) if idx_on_time is not None else 0.70
            avg_delay_min = parse_delay_min(row[idx_delay]) if idx_delay is not None else 0

            # 周班期：直接按索引取
            if week_idx:
                freq_per_week = 0
                for wi in week_idx:
                    v = row[wi]
                    if isinstance(v, float) and pd.isna(v):
                        continue
                    if "有班期" in str(v):
                        freq_per_week += 1
            else:
                freq_per_week = 0

            people = safe_int(row[idx_people], 0) if idx_people is not None else 0
            popularity = calc_flight_popularity(on_time_rate, avg_delay_min, freq_per_week, people)

            payload.append((
                d, flight_no, aircraft_type, airline_iata, flight_number,
                int(dep_id), int(arr_id),
                dep_dt, arr_dt, duration_min,
                True, popularity
            ))

            if len(payload) >= BATCH_ROWS:
                inserted += insert_batch(conn, payload)
                conn.commit()
                payload.clear()

            if i % PROGRESS_EVERY == 0:
                elapsed = time.time() - t0
                speed = i / elapsed if elapsed > 0 else 0
                eta = (total - i) / speed / 60 if speed > 0 else -1
                safe_print(
                    f"[进度] {i}/{total} inserted={inserted} miss_airport={miss_airport} bad_fno={bad_fno} "
                    f"speed={speed:.1f} rows/s eta={eta:.1f} min"
                )

        if payload:
            inserted += insert_batch(conn, payload)
            conn.commit()

        elapsed = time.time() - t0
        safe_print(f"[done] flight inserted={inserted} miss_airport={miss_airport} bad_fno={bad_fno} time={elapsed:.1f}s")

        # ========== 4) 输出 reason 统计（保留 debug 价值）==========
        def topk(counter, k=15):
            return sorted(counter.items(), key=lambda x: x[1], reverse=True)[:k]

        safe_print("\n=== DEP reason top ===")
        for k, v in topk(dep_reason_cnt):
            safe_print(f"{k}: {v}")

        safe_print("\n=== ARR reason top ===")
        for k, v in topk(arr_reason_cnt):
            safe_print(f"{k}: {v}")

        safe_print("\n=== (DEP,ARR) reason pair top ===")
        for (k1, k2), v in topk(miss_pair_cnt):
            safe_print(f"({k1}, {k2}): {v}")

        safe_print("\n[OK] 仅 flight 表写入完成（未计算/写入热度相关表）。")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
