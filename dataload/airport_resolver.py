# -*- coding: utf-8 -*-
"""
airport_resolver.py

机场解析模块（修正版）：
- 坐标优先匹配（网格索引 + haversine）
- 名称清洗匹配 + is_active 优先
- 歧义时使用高德地图 API
- 支持别名映射（对别名 key 做 clean 后匹配，解决 T1/T2/航站楼/括号 等干扰）
"""

import re
import math
import time
import requests
from collections import defaultdict
import pandas as pd

# ===============================
# 参数配置
# ===============================
AIRPORT_MATCH_RADIUS_KM = 5.0
GRID_DEG = 0.10

GAODE_KEY = "4dfdbf258610d22223a6fa490edc8770"  # ← 建议放到环境变量/配置文件

# ===== 机场别名映射（原始表）=====
ALIAS_MAP = {
    "流亭机场": "青岛胶东国际机场",
    "蓝田机场": "泸州云龙机场",
    "南苑机场": "北京大兴国际机场",
    "白塔埠机场": "连云港花果山国际机场",
    "右江机场": "百色巴马机场",
    "苏南硕放机场": "无锡硕放机场",
    "二十里铺机场": "延安南泥湾机场",
    "菜坝机场": "宜宾五粮液机场",
    "遵义机场": "遵义新舟机场",
}

# ===============================
# 工具函数
# ===============================
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0088
    phi1, phi2 = map(math.radians, [lat1, lat2])
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))

def grid_key(lat, lon):
    return (int(lat / GRID_DEG), int(lon / GRID_DEG))

def clean_name(name):
    """
    名称归一化，用于名称级匹配
    """
    if name is None:
        return None
    n = str(name).strip()
    if not n:
        return None
    n = re.sub(r"T\d+", "", n)
    n = re.sub(r"航站楼.*", "", n)
    n = re.sub(r"[（）()]", "", n)
    n = re.sub(r"(国际)?机场", "", n)
    n = n.strip()
    return n if n else None

def to_float_or_none(x):
    if x is None or pd.isna(x):
        return None
    try:
        return float(x)
    except Exception:
        return None

def coord_valid(lat, lon):
    if lat is None or lon is None:
        return False
    return (-90.0 <= lat <= 90.0) and (-180.0 <= lon <= 180.0)

# ===============================
# 高德地图 API（歧义兜底）
# ===============================
def gaode_nearest_airport(lat, lon):
    """
    返回 (name, err_reason)
    """
    url = "https://restapi.amap.com/v3/place/around"
    params = {
        "key": GAODE_KEY,
        "location": f"{lon},{lat}",
        "keywords": "机场",
        "radius": 30000,
        "sortrule": "distance",
    }
    try:
        r = requests.get(url, params=params, timeout=5)
        j = r.json()
        pois = j.get("pois", [])
        if not pois:
            return None, "gaode_no_pois"
        return pois[0].get("name"), None
    except Exception as e:
        return None, f"gaode_exception={type(e).__name__}:{e}"

# ===============================
# Resolver 类
# ===============================
class AirportResolver:
    """
    机场解析器（可复用）
    """

    def __init__(self, conn):
        self.name_map = defaultdict(list)   # db_name -> [id...]
        self.active_map = {}                # id -> is_active
        self.grid = defaultdict(list)       # (grid_i,grid_j) -> [(id,lat,lon,db_name),...]

        # 缓存：大量重复数据时显著提速且更稳定
        self.coord_cache = {}               # (lat,lon) -> (id|None, reason)
        self.name_cache = {}                # clean_name -> (id|None, reason)

        # 关键修正：建立“clean_key -> target_name”的别名映射
        self.alias_clean = {}
        for k, v in ALIAS_MAP.items():
            ck = clean_name(k)
            if ck:
                self.alias_clean[ck] = v

        self._load_airports(conn)

    def _load_airports(self, conn):
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, airport_name, latitude, longitude, is_active
                FROM airport
            """)
            rows = cur.fetchall()

        for aid, name, lat, lon, is_active in rows:
            aid = int(aid)
            name = str(name)
            self.name_map[name].append(aid)
            self.active_map[aid] = int(is_active) if is_active is not None else 0

            lat_f = to_float_or_none(lat)
            lon_f = to_float_or_none(lon)
            if lat_f is not None and lon_f is not None and coord_valid(lat_f, lon_f):
                self.grid[grid_key(lat_f, lon_f)].append((aid, lat_f, lon_f, name))

    # ---------------------------
    # 核心解析接口
    # ---------------------------
    def resolve(self, lat, lon, raw_name):
        """
        返回：
            airport_id 或 None
            reason 字符串（用于调试/统计）
        """

        # 0) 预处理：经纬度 & clean_name
        lat_f = to_float_or_none(lat)
        lon_f = to_float_or_none(lon)
        cname = clean_name(raw_name)

        # 1) 别名映射（clean-key）
        #    解决：T2流亭机场 -> clean=流亭 -> 命中 "流亭机场" 的别名目标
        if cname and cname in self.alias_clean:
            mapped = self.alias_clean[cname]   # 如 "青岛胶东国际机场"
            # 直接把 raw_name 替换成 mapped，再走正常流程（坐标仍优先）
            raw_name = mapped
            cname = clean_name(raw_name)

        # 2) 坐标匹配
        aid, reason = self._coord_match(lat_f, lon_f)
        if aid is not None:
            return aid, reason

        # 3) 名称匹配（含 is_active + 歧义高德）
        aid, reason = self._name_match(lat_f, lon_f, raw_name)
        if aid is not None:
            return aid, reason

        return None, "miss"

    # ---------------------------
    # 子策略：坐标匹配
    # ---------------------------
    def _coord_match(self, lat_f, lon_f):
        if lat_f is None or lon_f is None or not coord_valid(lat_f, lon_f):
            return None, "no_coord"

        key = (lat_f, lon_f)
        if key in self.coord_cache:
            return self.coord_cache[key]

        g0 = grid_key(lat_f, lon_f)
        candidates = []
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                candidates += self.grid.get((g0[0] + di, g0[1] + dj), [])

        best, best_d = None, None
        for aid, alat, alon, _ in candidates:
            d = haversine_km(lat_f, lon_f, alat, alon)
            if best_d is None or d < best_d:
                best, best_d = aid, d

        if best is not None and best_d is not None and best_d <= AIRPORT_MATCH_RADIUS_KM:
            self.coord_cache[key] = (best, "coord-ok")
            return best, "coord-ok"

        self.coord_cache[key] = (None, "coord-no-match")
        return None, "coord-no-match"

    # ---------------------------
    # 子策略：名称匹配（含 is_active + 高德歧义兜底）
    # ---------------------------
    def _name_match(self, lat_f, lon_f, raw_name):
        cname = clean_name(raw_name)
        if not cname:
            return None, "empty_name"

        # name cache 用 clean_name 作为 key（更稳）
        if cname in self.name_cache:
            return self.name_cache[cname]

        hits = []
        for name, aids in self.name_map.items():
            db_clean = clean_name(name)
            if db_clean and cname in db_clean:
                hits.extend(aids)

        if not hits:
            self.name_cache[cname] = (None, "no_hits")
            return None, "no_hits"

        # 1) is_active 优先
        active_hits = [aid for aid in hits if self.active_map.get(aid) == 1]
        if len(active_hits) == 1:
            self.name_cache[cname] = (active_hits[0], "active-only-ok")
            return active_hits[0], "active-only-ok"

        if len(hits) == 1:
            self.name_cache[cname] = (hits[0], "name-ok")
            return hits[0], "name-ok"

        # 2) 歧义 → 高德兜底（仅在坐标可用时）
        if lat_f is not None and lon_f is not None and coord_valid(lat_f, lon_f):
            time.sleep(0.10)
            gname, gerr = gaode_nearest_airport(lat_f, lon_f)
            if gname:
                gclean = clean_name(gname)
                if gclean:
                    for name, aids in self.name_map.items():
                        db_clean = clean_name(name)
                        if db_clean and (gclean == db_clean or gclean in db_clean):
                            # 不写入 name_cache：因为歧义情况下不同坐标可能得到不同结果
                            return aids[0], "gaode-ok"
            # 走到这里说明高德没解开歧义
            return None, f"ambiguous({len(hits)})"

        return None, f"ambiguous({len(hits)})"
