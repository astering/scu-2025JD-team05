# -*- coding: utf-8 -*-
import requests
import pymysql
import time
import re
import json
import sys
from decimal import Decimal, ROUND_HALF_UP

# ===============================
# 1. 配置区
# ===============================
AMAP_KEY = "4dfdbf258610d22223a6fa490edc8770"
BAIDU_AK = "IqnIQvpNODJjPAVIyPPKjwvZDAcelUpc"
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "bishe",
    "charset": "utf8mb4",
    "collation": "utf8mb4_0900_ai_ci",
}

# ===============================
# 2. 安全打印（解决 Windows 控制台 GBK 编码报错）
# ===============================
def safe_print(*args, **kwargs):
    """
    Windows 控制台常见是 GBK：遇到冷僻字会 UnicodeEncodeError。
    这里统一用 backslashreplace，保证不会因为 print 把程序打崩。
    """
    sep = kwargs.get("sep", " ")
    end = kwargs.get("end", "\n")
    text = sep.join(str(a) for a in args)
    try:
        print(text, end=end)
    except UnicodeEncodeError:
        #用backslashreplace将无法编码的字符转成 \uXXXX
        encoded = text.encode(sys.stdout.encoding or "gbk", errors="backslashreplace")
        sys.stdout.buffer.write(encoded + end.encode(sys.stdout.encoding or "gbk", errors="ignore"))

# ===============================
# 3. 工具函数 —— 提取纯中文机场名
# ===============================
def extract_chinese_name(name):
    chinese_parts = re.findall(r'[\u4e00-\u9fa5]+', name or "")
    return ''.join(chinese_parts).strip()

# ===============================
# 4. 高德地图查询函数（adname+address 拼接 + 类型兜底）
# ===============================
def query_amap_location(keyword):
    """
    返回: (full_address, lng, lat)
    full_address = adname + address（若 address 已含 adname 则不重复）
    """
    url = "https://restapi.amap.com/v3/place/text"
    params = {
        "keywords": keyword,
        "key": AMAP_KEY,
        "extensions": "all",
        "output": "JSON",
        "types": "150104",      # 飞机场
        "citylimit": "false",
        "offset": 20,
        "page": 1
    }

    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.encoding = "utf-8"
        data = resp.json()

        # 原始返回：用 ensure_ascii=True，避免打印冷僻字炸控制台
        safe_print(f"[高德API原始返回] {keyword} → {json.dumps(data, ensure_ascii=True)}")

        if data.get("status") != "1":
            return None, None, None

        pois = data.get("pois", [])
        if not pois:
            return None, None, None

        best = pois[0]

        # 1) 经纬度
        location = (best.get("location") or "").strip()
        if not location or "," not in location:
            return None, None, None
        lng_str, lat_str = location.split(",", 1)

        # 2) 取 adname + address，并处理 address 可能是 [] 的情况
        adname = (best.get("adname") or "").strip()   # 区/县，如：朝阳区/黄平县/吴川市

        raw_address = best.get("address", "")
        if isinstance(raw_address, list):
            address = ""   # 有些 POI 会返回 []，直接当空
        else:
            address = str(raw_address or "").strip()

        # 3) 拼接逻辑：你要 “adname + address”
        if adname and address:
            full_address = address if adname in address else f"{adname}{address}"
        else:
            # address 缺失时，至少不要写空：用省市区兜底
            pname = str(best.get("pname") or "").strip()
            cityname = str(best.get("cityname") or "").strip()
            full_address = address or (pname + cityname + adname)

        safe_print(f"[高德解析] adname={adname} | address={json.dumps(address, ensure_ascii=True)} "
                   f"| full_address={json.dumps(full_address, ensure_ascii=True)} | location={location}")

        return full_address, float(lng_str), float(lat_str)

    except Exception as e:
        # 这里也用 safe_print，避免异常信息里包含无法编码字符导致再次崩
        safe_print("[高德API异常]", repr(e))
        return None, None, None

# ===============================
# 5. 百度地图查询函数（兜底）
# ===============================
def query_baidu_location(keyword):
    """
    返回: (address, lng, lat)
    """
    url = "https://api.map.baidu.com/place/v2/search"
    params = {
        "query": keyword,
        "region": "全国",
        "output": "json",
        "ak": BAIDU_AK
    }

    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.encoding = "utf-8"
        data = resp.json()

        # 原始返回同样用 ensure_ascii，避免控制台炸
        safe_print(f"[百度API原始返回] {keyword} → {json.dumps(data, ensure_ascii=True)}")

        if data.get("status") != 0:
            return None, None, None

        results = data.get("results", [])
        if not results:
            return None, None, None

        best = results[0]

        # 地址优先 formatted_address（如果有），否则 address
        address = best.get("formatted_address") or best.get("address") or ""

        loc = best.get("location") or {}
        lng = loc.get("lng")
        lat = loc.get("lat")

        if lng is None or lat is None:
            return None, None, None

        return str(address), float(lng), float(lat)

    except Exception as e:
        safe_print("[百度API异常]", repr(e))
        return None, None, None

# ===============================
# 6. 数据库连接
# ===============================
def get_conn():
    return pymysql.connect(**DB_CONFIG)

# ===============================
# 7. 主逻辑：高德优先，失败再百度
# ===============================
def fill_airport_geo():
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, airport_name
        FROM airport
        WHERE longitude IS NULL OR latitude IS NULL
    """)
    airports = cursor.fetchall()

    safe_print(f"待补全机场数量: {len(airports)}")

    for airport_id, airport_name in airports:
        chinese_name = extract_chinese_name(airport_name)
        query_keyword = chinese_name if chinese_name else airport_name

        safe_print("\n====================================")
        safe_print(f"[机场] {airport_name}")
        safe_print(f"[提取中文] {chinese_name}")
        safe_print(f"[使用查询关键词] {query_keyword}")

        # ---- 1) 高德查询（中文关键词）
        address, lng, lat = query_amap_location(query_keyword)

        # ---- 2) 高德若无结果，且中文 != 原名，则用原名重试
        if lng is None or lat is None:
            if query_keyword != airport_name:
                safe_print("[重试] 高德中文名无结果 → 改用原始完整名查询")
                address, lng, lat = query_amap_location(airport_name)

        # ---- 3) 高德仍无 → 百度兜底（先中文，再原名）
        if lng is None or lat is None:
            safe_print("[兜底] 高德仍无有效坐标 → 改用百度API查询")
            address, lng, lat = query_baidu_location(query_keyword)

            if (lng is None or lat is None) and query_keyword != airport_name:
                safe_print("[重试] 百度中文名无结果 → 改用原始完整名查询")
                address, lng, lat = query_baidu_location(airport_name)

        # ---- 4) 写库
        if lng is not None and lat is not None:
            if not (-180 <= float(lng) <= 180 and -90 <= float(lat) <= 90):
                safe_print("[无效坐标] 超出经纬度范围，跳过")
                continue
            # 量化到 8 位小数，避免 float 误差
            lng_d = Decimal(str(lng)).quantize(Decimal("0.00000000"), rounding=ROUND_HALF_UP)
            lat_d = Decimal(str(lat)).quantize(Decimal("0.00000000"), rounding=ROUND_HALF_UP)
            safe_print(f"[写入] 地址={json.dumps(address, ensure_ascii=True)}")
            safe_print(f"       经度={lng} | 纬度={lat}")

            update_sql = """
                UPDATE airport
                SET address=%s, longitude=%s, latitude=%s
                WHERE id=%s
            """
            cursor.execute(update_sql, (address, str(lng_d), str(lat_d), airport_id))
            conn.commit()
        else:
            safe_print("[无结果] 高德+百度 均未返回有效坐标，跳过")

        time.sleep(0.3)

    cursor.close()
    conn.close()
    safe_print("\n=== airport 表坐标补全完成 ===")

# ===============================
# 8. 入口
# ===============================
if __name__ == "__main__":
    fill_airport_geo()
