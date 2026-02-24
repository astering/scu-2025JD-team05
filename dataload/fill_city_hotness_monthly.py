# -*- coding: utf-8 -*-
"""
fill_city_hotness_monthly.py  (Route 1, supports airport->multiple cities)

目标：
1) 基于 flight + dataset + city_airport 计算每月城市热度 city_hotness_monthly
2) 用“最后一个月”的结果回填 city_airport.airport_hotness / city_airport.city_hotness

前提：
- flight 已填充且唯一键： (airline_iata_code, flight_no, flight_date, departure_time)
- dataset 已去重填充且唯一键： (flight_no, flight_date, departure_time)
- city_airport 映射可靠，但允许 airport_id -> 多个 city_id

口径：
- 一条航班对 出发机场、到达机场 各贡献一次“机场热度”
- 城市热度通过 city_airport：机场关联到哪些 city，就给这些 city 都累加
"""

import sys
import time
from collections import defaultdict

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
    "autocommit": False,
}

FETCH_SIZE = 50000      # 流式读取 fetchmany 大小
UPSERT_BATCH = 20000    # 写 city_hotness_monthly 批量
UPDATE_BATCH = 20000    # 回填 airport_hotness 批量
CALC_VERSION = 1

# 热度参数（可调，保持可解释）
BASE_FLIGHT = 1.0
PEOPLE_W = 1.0

WEEK_MIN = 0.60
WEEK_MAX = 1.00

QUALITY_MIN = 0.80
QUALITY_MAX = 1.00


# ===============================
# 1) 工具
# ===============================
def safe_print(*args):
    try:
        print(*args)
    except UnicodeEncodeError:
        sys.stdout.buffer.write((" ".join(map(str, args)) + "\n").encode("utf-8", "replace"))

def clamp(v, lo, hi):
    return lo if v < lo else hi if v > hi else v

def popcount7(x) -> int:
    try:
        return (int(x) & 0x7F).bit_count()
    except Exception:
        return 0

def to_year_month(d) -> str:
    return f"{d.year:04d}-{d.month:02d}"


# ===============================
# 2) SQL
# ===============================
SQL_LOAD_CITY_AIRPORT = "SELECT airport_id, city_id FROM city_airport"

SQL_STREAM = """
SELECT
    f.flight_date,
    f.departure_airport_id,
    f.arrival_airport_id,
    f.flight_popularity,
    d.people,
    d.week_bitmap
FROM flight f
JOIN dataset d
  ON d.flight_no = f.flight_no
 AND d.flight_date = f.flight_date
 AND d.departure_time = TIME(f.departure_time)
ORDER BY f.flight_date
"""

# 修复点：不要在 INSERT 列里写 updated_at（它有默认/自动更新）
# 6列 -> 6个%s，避免 not enough arguments for format string
SQL_UPSERT_CITY_MONTH = """
INSERT INTO city_hotness_monthly
    (`city_id`, `year_month`, `flight_count`, `total_people`, `hotness`, `calc_version`)
VALUES
    (%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
    flight_count = VALUES(flight_count),
    total_people = VALUES(total_people),
    hotness = VALUES(hotness),
    calc_version = VALUES(calc_version),
    updated_at = CURRENT_TIMESTAMP
"""

# 方案A：不使用临时表，直接批量更新 airport_hotness
SQL_UPDATE_AIRPORT_HOTNESS = """
UPDATE city_airport
SET airport_hotness = %s
WHERE airport_id = %s
"""

SQL_UPDATE_CITY_AIRPORT_CITY_HOTNESS = """
UPDATE city_airport ca
JOIN city_hotness_monthly ch
  ON ca.city_id = ch.city_id
SET ca.city_hotness = ch.hotness
WHERE ch.year_month = %s
"""


# ===============================
# 3) 主流程
# ===============================
def main():
    # 3.1 加载 airport_id -> [city_id...]
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            safe_print("[load] loading city_airport mapping (airport -> cities) ...")
            cur.execute(SQL_LOAD_CITY_AIRPORT)
            rows = cur.fetchall()

        airport_to_cities = defaultdict(list)
        for airport_id, city_id in rows:
            airport_to_cities[int(airport_id)].append(int(city_id))

        safe_print(f"[load] city_airport rows={len(rows)} unique_airports={len(airport_to_cities)}")
    finally:
        conn.close()

    # 3.2 聚合容器
    # city_month[(city_id, ym)] = [flight_count, total_people, hot_sum]
    city_month = defaultdict(lambda: [0, 0, 0.0])
    # airport_month[(airport_id, ym)] = hot_sum
    airport_month = defaultdict(float)

    # 3.3 流式扫描 flight join dataset
    t0 = time.time()
    scanned = 0
    miss_dep_city_map = 0
    miss_arr_city_map = 0

    stream_conn = pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.SSCursor)
    try:
        with stream_conn.cursor() as cur:
            safe_print("[scan] streaming flight x dataset ...")
            cur.execute(SQL_STREAM)

            while True:
                batch = cur.fetchmany(FETCH_SIZE)
                if not batch:
                    break

                for flight_date, dep_aid, arr_aid, flight_pop, people, week_bm in batch:
                    scanned += 1

                    # people
                    try:
                        ppl = int(people) if people is not None else 0
                    except Exception:
                        ppl = 0
                    if ppl < 0:
                        ppl = 0

                    # week factor
                    week_bits = popcount7(week_bm)
                    week_factor = WEEK_MIN + (WEEK_MAX - WEEK_MIN) * (week_bits / 7.0)

                    # quality factor
                    try:
                        fp = float(flight_pop) if flight_pop is not None else 0.0
                    except Exception:
                        fp = 0.0
                    fp = clamp(fp, 0.0, 1.0)
                    quality_factor = QUALITY_MIN + (QUALITY_MAX - QUALITY_MIN) * fp

                    contrib = (BASE_FLIGHT + PEOPLE_W * ppl) * week_factor * quality_factor
                    ym = to_year_month(flight_date)

                    dep_aid = int(dep_aid)
                    arr_aid = int(arr_aid)

                    # airport side (each flight contributes to both dep and arr airports)
                    airport_month[(dep_aid, ym)] += contrib
                    airport_month[(arr_aid, ym)] += contrib

                    # city side: airport -> multiple cities
                    dep_cities = airport_to_cities.get(dep_aid)
                    if not dep_cities:
                        miss_dep_city_map += 1
                    else:
                        for cid in dep_cities:
                            rec = city_month[(cid, ym)]
                            rec[0] += 1
                            rec[1] += ppl
                            rec[2] += contrib

                    arr_cities = airport_to_cities.get(arr_aid)
                    if not arr_cities:
                        miss_arr_city_map += 1
                    else:
                        for cid in arr_cities:
                            rec = city_month[(cid, ym)]
                            rec[0] += 1
                            rec[1] += ppl
                            rec[2] += contrib

                if scanned % (FETCH_SIZE * 2) == 0:
                    elapsed = time.time() - t0
                    safe_print(
                        f"[scan] scanned={scanned} speed={scanned/elapsed:.1f} rows/s "
                        f"city_month_keys={len(city_month)} airport_month_keys={len(airport_month)} "
                        f"miss_city_map(dep={miss_dep_city_map},arr={miss_arr_city_map})"
                    )
    finally:
        stream_conn.close()

    elapsed = time.time() - t0
    safe_print(
        f"[scan-done] scanned={scanned} time={elapsed:.1f}s "
        f"city_month_keys={len(city_month)} airport_month_keys={len(airport_month)} "
        f"miss_city_map(dep={miss_dep_city_map},arr={miss_arr_city_map})"
    )

    if not city_month:
        safe_print("[abort] no aggregation result. Check joins between flight and dataset.")
        return

    # 3.4 找最后一个月
    all_months = sorted({ym for (_, ym) in city_month.keys()})
    last_month = all_months[-1]
    safe_print(f"[month] last_year_month = {last_month} (for city_airport backfill)")

    # 3.5 写入 city_hotness_monthly
    safe_print("[write] upserting city_hotness_monthly ...")
    conn2 = pymysql.connect(**DB_CONFIG)
    try:
        with conn2.cursor() as cur:
            payload = []
            written = 0

            for (city_id, ym), (flight_cnt, total_ppl, hot_sum) in city_month.items():
                payload.append((int(city_id), ym, int(flight_cnt), int(total_ppl), float(hot_sum), int(CALC_VERSION)))

                if len(payload) >= UPSERT_BATCH:
                    cur.executemany(SQL_UPSERT_CITY_MONTH, payload)
                    conn2.commit()
                    written += len(payload)
                    payload.clear()
                    safe_print(f"[write] city_hotness_monthly upserted={written}")

            if payload:
                cur.executemany(SQL_UPSERT_CITY_MONTH, payload)
                conn2.commit()
                written += len(payload)

        safe_print(f"[write-done] city_hotness_monthly rows upserted={written}")
    finally:
        conn2.close()

    # 3.6 回填 city_airport（最后一个月）——方案A：直接批量 update
    safe_print("[backfill] updating city_airport using last_month ...")

    # last_month airport hotness
    airport_last = defaultdict(float)
    for (aid, ym), hot in airport_month.items():
        if ym == last_month:
            airport_last[int(aid)] += float(hot)

    safe_print(f"[backfill] last_month airport_count={len(airport_last)}")

    conn3 = pymysql.connect(**DB_CONFIG)
    try:
        with conn3.cursor() as cur:
            # 1) airport_hotness：批量更新（airport_id 可能对应多行 city_airport，会一起被更新）
            payload = []
            upd = 0

            for aid, hot in airport_last.items():
                payload.append((float(hot), int(aid)))
                if len(payload) >= UPDATE_BATCH:
                    cur.executemany(SQL_UPDATE_AIRPORT_HOTNESS, payload)
                    conn3.commit()
                    upd += len(payload)
                    payload.clear()
                    safe_print(f"[backfill] airport_hotness updated(batch)={upd}")

            if payload:
                cur.executemany(SQL_UPDATE_AIRPORT_HOTNESS, payload)
                conn3.commit()
                upd += len(payload)

            safe_print(f"[backfill] airport_hotness updated total_airports={upd}")

            # 2) city_hotness：按 last_month 一次性 join 回填
            cur.execute(SQL_UPDATE_CITY_AIRPORT_CITY_HOTNESS, (last_month,))
            conn3.commit()
            safe_print(f"[backfill] city_hotness updated from city_hotness_monthly({last_month})")

    finally:
        conn3.close()

    safe_print("[OK] fill_city_hotness_monthly done.")


if __name__ == "__main__":
    main()
