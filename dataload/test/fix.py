# -*- coding: utf-8 -*-
"""
并行批量将 flight_seats.seat_popularity 更新为 1.0
"""

import pymysql
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# ======================
# 数据库配置
# ======================
DB_CONF = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "bishe",
    "charset": "utf8mb4",
    "autocommit": False
}

# ======================
# 可调参数
# ======================
BATCH_SIZE = 50000     # 每批更新多少条
MAX_WORKERS = 8        # 并行线程数


def get_id_range():
    conn = pymysql.connect(**DB_CONF)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MIN(id), MAX(id) FROM flight_seats")
            result = cur.fetchone()
            return result[0], result[1]
    finally:
        conn.close()


def update_batch(start_id, end_id):
    conn = pymysql.connect(**DB_CONF)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE flight_seats
                SET seat_popularity = 1.0
                WHERE id BETWEEN %s AND %s
                """,
                (start_id, end_id)
            )
        conn.commit()
        return (start_id, end_id)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def main():
    print("开始批量更新 flight_seats.seat_popularity = 1.0")

    min_id, max_id = get_id_range()

    if not min_id:
        print("表为空，结束")
        return

    print(f"id 范围: {min_id} ~ {max_id}")

    ranges = []
    current = min_id

    while current <= max_id:
        end = min(current + BATCH_SIZE - 1, max_id)
        ranges.append((current, end))
        current = end + 1

    total_batches = len(ranges)
    print(f"共 {total_batches} 批，每批 {BATCH_SIZE} 条左右")

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(update_batch, r[0], r[1]) for r in ranges]

        completed = 0
        for future in as_completed(futures):
            result = future.result()
            completed += 1
            print(f"[{completed}/{total_batches}] 更新完成 id {result[0]} ~ {result[1]}")

    elapsed = time.time() - start_time
    print(f"全部完成，耗时 {elapsed:.2f} 秒")


if __name__ == "__main__":
    main()
