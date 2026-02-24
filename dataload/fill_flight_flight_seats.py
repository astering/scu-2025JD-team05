# -*- coding: utf-8 -*-
"""
fill_flight_seats_v2.py
- 扫描 CSV/Excel，按 flight 唯一键聚合：(flight_date, airline_iata_code, flight_no, departure_time)
- 批量从 flight 表 lookup flight_id
- 流式生成 seat_rows，分批 upsert 到 flight_seats
- 增强中间进度输出、分批 commit，避免“长时间无输出”
"""

import re
import sys
import math
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

DATA_FILE = r"D:\data\中国航空航班数据.csv"
DATA_TYPE = "csv"  # "csv" or "excel"
CSV_ENCODING = "utf-8-sig"
CSV_SEP = ","

# CSV 分块读取
CHUNKSIZE = 200000

# lookup flight_id 的 IN tuples 单次大小
LOOKUP_STEP = 1000

# 一次性 upsert seat_rows 的目标批大小（更大更快，但太大可能卡）
SEAT_ROWS_BATCH = 50000

# executemany 内部分段（避免单次包太大）
UPSERT_STEP = 20000

# 扫描时进度
PROGRESS_EVERY_KEYS = 200000

PRICE_MULT = {"E": Decimal("1.0"), "B": Decimal("1.3"), "F": Decimal("1.5")}
SEAT_SHARE_DEFAULT = {"E": 0.82, "B": 0.14, "F": 0.04}
PRICE_Q = Decimal("0.01")

DEFAULT_SEAT_POPULARITY = 1.0
DEFAULT_REMAIN_SEATS = 0


# ===============================
# 工具
# ===============================
def safe_print(*args):
    try:
        print(*args)
    except UnicodeEncodeError:
        sys.stdout.buffer.write((" ".join(map(str, args)) + "\n").encode("utf-8", "replace"))

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

def safe_int(x, default=0):
    try:
        if x is None:
            return default
        if isinstance(x, float) and pd.isna(x):
            return default
        return int(float(x))
    except Exception:
        return default

def safe_decimal(x, default=Decimal("0.00")):
    try:
        if x is None:
            return default
        if isinstance(x, float) and pd.isna(x):
            return default
        s = str(x).strip()
        if not s:
            return default
        return Decimal(s).quantize(PRICE_Q, rounding=ROUND_HALF_UP)
    except Exception:
        return default

def quantize_price(x: Decimal) -> Decimal:
    return x.quantize(PRICE_Q, rounding=ROUND_HALF_UP)

def calc_seat_allocation(total: int, share=SEAT_SHARE_DEFAULT):
    total = int(total)
    if total <= 0:
        return {"E": 0, "B": 0, "F": 0}
    if total < 3:
        return {"E": total, "B": 0, "F": 0}
    if total < 10:
        b = max(1, int(round(total * 0.15)))
        e = total - b
        return {"E": e, "B": b, "F": 0}

    e = max(1, int(math.floor(total * share["E"])))
    b = max(1, int(math.floor(total * share["B"])))
    f = max(1, int(math.floor(total * share["F"])))

    s = e + b + f
    if s > total:
        diff = s - total
        take = min(diff, e - 1); e -= take; diff -= take
        if diff > 0:
            take = min(diff, b - 1); b -= take; diff -= take
        if diff > 0:
            take = min(diff, f - 1); f -= take; diff -= take
    elif s < total:
        e += (total - s)

    return {"E": e, "B": b, "F": f}

def derive_class_prices(avg_price: Decimal, seat_alloc: dict):
    total = seat_alloc["E"] + seat_alloc["B"] + seat_alloc["F"]
    if total <= 0:
        return {"E": Decimal("0.00"), "B": Decimal("0.00"), "F": Decimal("0.00")}

    w = (
        Decimal(seat_alloc["E"]) * PRICE_MULT["E"]
        + Decimal(seat_alloc["B"]) * PRICE_MULT["B"]
        + Decimal(seat_alloc["F"]) * PRICE_MULT["F"]
    ) / Decimal(total)

    pe = (avg_price / w) if w != 0 else avg_price
    return {
        "E": quantize_price(pe),
        "B": quantize_price(pe * PRICE_MULT["B"]),
        "F": quantize_price(pe * PRICE_MULT["F"]),
    }


# ===============================
# 读数据（分块）
# ===============================
def load_df_iter():
    if DATA_TYPE.lower() == "excel":
        yield pd.read_excel(DATA_FILE)
        return
    for chunk in pd.read_csv(
        DATA_FILE,
        encoding=CSV_ENCODING,
        sep=CSV_SEP,
        low_memory=False,
        chunksize=CHUNKSIZE
    ):
        yield chunk

def pick_col(df, names):
    for n in names:
        if n in df.columns:
            return n
    raise KeyError(f"缺少列：{names}，当前列：{list(df.columns)}")


# ===============================
# DB SQL
# ===============================
SQL_LOOKUP_FLIGHT_IDS_PREFIX = """
SELECT id, flight_date, airline_iata_code, flight_no, departure_time
FROM flight
WHERE (flight_date, airline_iata_code, flight_no, departure_time) IN
"""

SQL_UPSERT_SEATS = """
INSERT INTO flight_seats
    (flight_id, flight_date, seat_class, price, total_seats, remain_seats, seat_popularity)
VALUES
    (%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
    price=VALUES(price),
    total_seats=VALUES(total_seats),
    remain_seats=VALUES(remain_seats),
    seat_popularity=VALUES(seat_popularity)
"""

def lookup_flight_ids(conn, keys):
    if not keys:
        return {}
    out = {}
    with conn.cursor() as cur:
        for s in range(0, len(keys), LOOKUP_STEP):
            part = keys[s:s+LOOKUP_STEP]
            placeholders = ",".join(["(%s,%s,%s,%s)"] * len(part))
            sql = SQL_LOOKUP_FLIGHT_IDS_PREFIX + f" ({placeholders})"
            flat = []
            for (d, airline, fno, dep_dt) in part:
                flat.extend([d, airline, fno, dep_dt])
            cur.execute(sql, flat)
            for fid, d, airline, fno, dep_dt in cur.fetchall():
                out[(d, airline, fno, dep_dt)] = int(fid)
    return out

def upsert_seats(conn, seat_rows, total_written, total_target, t0_batch):
    """
    seat_rows: list of rows to upsert
    """
    if not seat_rows:
        return 0
    with conn.cursor() as cur:
        for s in range(0, len(seat_rows), UPSERT_STEP):
            cur.executemany(SQL_UPSERT_SEATS, seat_rows[s:s+UPSERT_STEP])

    conn.commit()

    dt = time.time() - t0_batch
    total_written += len(seat_rows)

    speed = len(seat_rows) / dt if dt > 0 else 0.0
    overall_dt = time.time() - START_TS
    overall_speed = total_written / overall_dt if overall_dt > 0 else 0.0
    eta = (total_target - total_written) / overall_speed / 60 if overall_speed > 0 else -1

    safe_print(
        f"[upsert] +{len(seat_rows)} rows commit_ok "
        f"| batch_time={dt:.1f}s batch_speed={speed:.0f} r/s "
        f"| written={total_written}/{total_target} overall_speed={overall_speed:.0f} r/s eta={eta:.1f} min"
    )
    return len(seat_rows)


# ===============================
# 主流程
# ===============================
START_TS = 0.0

def main():
    global START_TS
    START_TS = time.time()

    conn = pymysql.connect(**DB_CONFIG)

    # 聚合：key -> {"price": Decimal, "people": int, "dup": int}
    agg = {}
    bad_fno = bad_date = bad_time = 0
    scanned_rows = 0

    try:
        # 1) 扫描并聚合
        first = True
        t_scan0 = time.time()
        for df in load_df_iter():
            if first:
                col_date = pick_col(df, ["日期"])
                col_dep_time = pick_col(df, ["起飞时间"])
                col_fno = pick_col(df, ["航班班次", "航班号", "航班", "flight_no"])
                col_price = next((c for c in ["价格(元)", "价格", "price"] if c in df.columns), None)
                col_people = next((c for c in ["人数", "乘机人数", "people"] if c in df.columns), None)
                if not col_price:
                    safe_print("[WARN] CSV 未找到价格列，将以 0.00 作为价格。")
                if not col_people:
                    safe_print("[WARN] CSV 未找到人数列，将以 0 作为总座位数。")
                first = False

            cols = list(df.columns)
            idx_date = cols.index(col_date)
            idx_dep_time = cols.index(col_dep_time)
            idx_fno = cols.index(col_fno)
            idx_price = cols.index(col_price) if col_price else None
            idx_people = cols.index(col_people) if col_people else None

            for row in df.itertuples(index=False, name=None):
                scanned_rows += 1
                flight_no, airline_iata, _ = split_flight_no(row[idx_fno])
                if not flight_no:
                    bad_fno += 1
                    continue
                try:
                    d = parse_date(row[idx_date])
                except Exception:
                    bad_date += 1
                    continue
                try:
                    dep_t = parse_time(row[idx_dep_time])
                except Exception:
                    bad_time += 1
                    continue

                dep_dt = datetime.combine(d, dep_t)
                price = safe_decimal(row[idx_price], Decimal("0.00")) if idx_price is not None else Decimal("0.00")
                people = safe_int(row[idx_people], 0) if idx_people is not None else 0

                key = (d, airline_iata, flight_no, dep_dt)
                if key not in agg:
                    agg[key] = {"price": price, "people": people, "dup": 1}
                else:
                    agg[key]["dup"] += 1
                    if price > Decimal("0.00"):
                        agg[key]["price"] = price
                    if people > agg[key]["people"]:
                        agg[key]["people"] = people

                if len(agg) % PROGRESS_EVERY_KEYS == 0:
                    elapsed = time.time() - t_scan0
                    safe_print(
                        f"[scan] scanned_rows={scanned_rows} agg_keys={len(agg)} "
                        f"bad_fno={bad_fno} bad_date={bad_date} bad_time={bad_time} "
                        f"elapsed={elapsed:.1f}s"
                    )

        safe_print(f"[scan-done] scanned_rows={scanned_rows} agg_keys={len(agg)} bad_fno={bad_fno} bad_date={bad_date} bad_time={bad_time}")

        # 2) lookup flight_id
        keys = list(agg.keys())
        t_lookup0 = time.time()
        flight_id_map = lookup_flight_ids(conn, keys)
        safe_print(
            f"[lookup] matched_flights={len(flight_id_map)}/{len(keys)} not_found={len(keys)-len(flight_id_map)} "
            f"time={time.time()-t_lookup0:.1f}s"
        )

        # 3) 估算最终 seat 行数（用于 ETA）
        #    每个 key 通常会生成 1~3 行（E/B/F），人数很小时可能只 E 或 E+B
        #    这里先快速估一下：按 people 分配计算太慢，我们用经验：默认 3
        est_total_seat_rows = len(flight_id_map) * 3
        safe_print(f"[plan] estimated_seat_rows≈{est_total_seat_rows} (for ETA)")

        # 4) 流式生成 seats + 分批 upsert
        total_written = 0
        total_target = est_total_seat_rows  # 只是估算
        not_found = 0

        seat_batch = []
        t_batch0 = time.time()

        processed_keys = 0
        t_gen0 = time.time()

        for key, info in agg.items():
            processed_keys += 1
            fid = flight_id_map.get(key)
            if fid is None:
                not_found += 1
                continue

            d, airline, fno, dep_dt = key
            avg_price = info["price"]
            total_people = info["people"]

            alloc = calc_seat_allocation(total_people, SEAT_SHARE_DEFAULT)
            prices = derive_class_prices(avg_price, alloc)

            for cls in ("E", "B", "F"):
                if alloc[cls] <= 0:
                    continue
                seat_batch.append((
                    fid, d, cls,
                    str(prices[cls]),
                    int(alloc[cls]),
                    int(DEFAULT_REMAIN_SEATS),
                    float(DEFAULT_SEAT_POPULARITY),
                ))

            # 每累计一定量就写入
            if len(seat_batch) >= SEAT_ROWS_BATCH:
                upsert_seats(conn, seat_batch, total_written, total_target, t_batch0)
                total_written += len(seat_batch)
                seat_batch.clear()
                t_batch0 = time.time()

            # 生成阶段也打点（不等写入）
            if processed_keys % 200000 == 0:
                dt = time.time() - t_gen0
                speed = processed_keys / dt if dt > 0 else 0
                safe_print(f"[gen] processed_keys={processed_keys}/{len(agg)} gen_speed={speed:.0f} keys/s not_found={not_found}")

        # 写剩余
        if seat_batch:
            upsert_seats(conn, seat_batch, total_written, total_target, t_batch0)
            total_written += len(seat_batch)
            seat_batch.clear()

        elapsed = time.time() - START_TS
        safe_print("\n=== DONE ===")
        safe_print(f"agg_keys={len(agg)} matched_flights={len(flight_id_map)} not_found={not_found}")
        safe_print(f"flight_seats rows upserted (actual) = {total_written}")
        safe_print(f"elapsed={elapsed:.1f}s avg_upsert_speed={total_written/elapsed:.0f} rows/s")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
