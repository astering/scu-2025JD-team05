# -*- coding: utf-8 -*-
"""
test.py
输出前端所需结构：
{
  "北京": {
    cityCode: "BJS",
    airports: [
      { code: "PEK", name: "首都国际机场" },
      ...
    ]
  },
  ...
}

规则：
- cityCode：取该 city 的第一个机场 iata_code（按 airport_id 升序的第一个）
- airports：该 city 的所有机场（按 airport_id 升序）
"""

import json
import pymysql

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "bishe",
    "port": 3306,
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}

ONLY_ACTIVE_AIRPORT = True  # True 则只取 is_active=1 的机场


def fetch_rows(conn):
    where_active = "WHERE a.is_active = 1" if ONLY_ACTIVE_AIRPORT else ""
    sql = f"""
    SELECT
        ca.city_id,
        c.city_name,
        ca.airport_id,
        a.iata_code,
        a.airport_name
    FROM city_airport ca
    JOIN city c ON c.id = ca.city_id
    JOIN airport a ON a.id = ca.airport_id
    {where_active}
    ORDER BY ca.city_id ASC, ca.airport_id ASC
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def build_city_index(rows):
    """
    rows 已按 city_id, airport_id 排序，因此：
    - 每个 city 的第一条机场记录就是“第一个机场”
    """
    city_index = {}
    last_city_id = None

    for r in rows:
        city_id = r.get("city_id")
        city_name = (r.get("city_name") or "").strip()
        iata = (r.get("iata_code") or "").strip().upper()
        airport_name = (r.get("airport_name") or "").strip()

        # 跳过无效行（没有城市名或没有三字码）
        if not city_name or not iata:
            continue

        # 如果是第一次遇到这个 city，初始化结构并设置 cityCode
        if city_id != last_city_id:
            if city_name not in city_index:
                city_index[city_name] = {
                    "cityCode": iata,     # ✅第一个机场 iata_code 作为 cityCode
                    "airports": []
                }
            last_city_id = city_id

        # 追加机场（去重：同城同 iata 只加一次）
        airports = city_index[city_name]["airports"]
        if not any(a["code"] == iata for a in airports):
            airports.append({"code": iata, "name": airport_name})

    return city_index


def print_js_object(city_index):
    """
    输出为 JS 风格（key 不加引号，字符串用双引号）
    例如：
    export const CITY_INDEX = {
      "北京": { cityCode: "BJS", airports: [ ... ] },
      ...
    }
    """
    # 先用 JSON dump 出去（字符串/中文都正确）
    # 然后把 "cityCode"/"airports" 这类字段名的引号去掉，变成 JS 对象风格
    s = json.dumps(city_index, ensure_ascii=False, indent=2)
    s = s.replace('"cityCode"', 'cityCode')
    s = s.replace('"airports"', 'airports')
    s = s.replace('"code"', 'code')
    s = s.replace('"name"', 'name')

    print("\n" + "=" * 80)
    print("JS object (copy to frontend)")
    print("=" * 80)
    print("export const CITY_INDEX = " + s + ";")


def main():
    conn = pymysql.connect(**DB_CONFIG)
    try:
        rows = fetch_rows(conn)
        print(f"[OK] fetched rows = {len(rows)}")

        city_index = build_city_index(rows)
        print(f"[OK] cities = {len(city_index)}")

        # 1) 打印纯 JSON（如果你想存文件/调试）
        print("\n" + "=" * 80)
        print("JSON output")
        print("=" * 80)
        print(json.dumps(city_index, ensure_ascii=False, indent=2))

        # 2) 打印 JS 风格导出（你要复制进前端）
        print_js_object(city_index)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
