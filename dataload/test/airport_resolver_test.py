# -*- coding: utf-8 -*-
import re
import math
import sys
import time
import requests
from collections import defaultdict
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
    "autocommit": False,
}

DATA_FILE = r"D:\data\中国航空航班数据.csv"
DATA_TYPE = "csv"

AIRPORT_MATCH_RADIUS_KM = 5.0
GRID_DEG = 0.10
PROGRESS_EVERY = 5000

GAODE_KEY = "4dfdbf258610d22223a6fa490edc8770"  # ← 换成你自己的

# ===== 映射字典 =====
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
# 工具
# ===============================
def safe_print(*args):
    try:
        print(*args)
    except UnicodeEncodeError:
        sys.stdout.buffer.write(
            " ".join(map(str, args)).encode("utf-8", "replace") + b"\n"
        )

def load_df():
    return pd.read_csv(DATA_FILE, low_memory=False)

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0088
    phi1, phi2 = map(math.radians, [lat1, lat2])
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dl/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def grid_key(lat, lon):
    return (int(lat / GRID_DEG), int(lon / GRID_DEG))

def clean_name(name):
    if not name:
        return None
    n = str(name)
    n = re.sub(r"T\d+", "", n)
    n = re.sub(r"航站楼.*", "", n)
    n = re.sub(r"[（）()]", "", n)
    n = re.sub(r"(国际)?机场", "", n)
    return n.strip()

def is_nan(x):
    try:
        return pd.isna(x)
    except Exception:
        return False

def to_float_or_none(x):
    if x is None or is_nan(x):
        return None
    try:
        return float(x)
    except Exception:
        return None

def coord_valid(lat, lon):
    """经纬度基本合法性检查（全球范围硬校验）"""
    if lat is None or lon is None:
        return False
    return (-90.0 <= lat <= 90.0) and (-180.0 <= lon <= 180.0)

# ===============================
# 加载机场（含 is_active）
# ===============================
def load_airports(conn):
    name_map = defaultdict(list)
    active_map = {}
    grid = defaultdict(list)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, airport_name, latitude, longitude, is_active
            FROM airport
        """)
        rows = cur.fetchall()

    for aid, name, lat, lon, is_active in rows:
        name_map[name].append(aid)
        active_map[aid] = is_active
        lat = to_float_or_none(lat)
        lon = to_float_or_none(lon)
        if lat is not None and lon is not None:
            grid[grid_key(lat, lon)].append((aid, lat, lon, name))

    return name_map, active_map, grid

# ===============================
# 高德地图 API（歧义兜底）
# ===============================
def gaode_nearest_airport(lat, lon):
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
        return pois[0]["name"], None
    except Exception as e:
        return None, f"gaode_exception={type(e).__name__}:{e}"

# ===============================
# 机场匹配主逻辑（带详细 debug）
# ===============================
def find_airport(lat, lon, raw_name, name_map, active_map, grid):
    """
    返回：
      aid, why, debug_dict
    """
    debug = {
        "raw": raw_name,
        "cname": clean_name(raw_name),
        "lat": lat,
        "lon": lon,
        "lat_f": None,
        "lon_f": None,
        "coord_ok": False,
        "coord_candidates": 0,
        "coord_best_id": None,
        "coord_best_d_km": None,
        "coord_reason": None,
        "name_hits_count": 0,
        "name_hits_preview": [],
        "name_reason": None,
        "gaode_called": False,
        "gaode_name": None,
        "gaode_clean": None,
        "gaode_reason": None,
        "alias_tried": False,
        "alias_hit": False,
        "alias_key": None,
        "alias_target": None,
        "alias_reason": None,
        "final_reason": None,
    }

    lat_f = to_float_or_none(lat)
    lon_f = to_float_or_none(lon)
    debug["lat_f"] = lat_f
    debug["lon_f"] = lon_f

    # ---- 0) 坐标可用性判断 ----
    if lat_f is None or lon_f is None:
        debug["coord_reason"] = "no_coord(lat/lon is None/NaN/not-float)"
    elif not coord_valid(lat_f, lon_f):
        debug["coord_reason"] = "coord_out_of_range"
    else:
        debug["coord_ok"] = True

    # ---- 1) 坐标匹配 ----
    if debug["coord_ok"]:
        g0 = grid_key(lat_f, lon_f)
        candidates = []
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                candidates += grid.get((g0[0] + di, g0[1] + dj), [])
        debug["coord_candidates"] = len(candidates)

        if not candidates:
            debug["coord_reason"] = "coord_no_candidates_in_grid"
        else:
            best, best_d = None, None
            for aid, alat, alon, _ in candidates:
                d = haversine_km(lat_f, lon_f, alat, alon)
                if best_d is None or d < best_d:
                    best, best_d = aid, d

            debug["coord_best_id"] = best
            debug["coord_best_d_km"] = best_d

            if best and best_d is not None and best_d <= AIRPORT_MATCH_RADIUS_KM:
                return best, "coord-ok", debug
            else:
                if best_d is None:
                    debug["coord_reason"] = "coord_best_d_none"
                else:
                    debug["coord_reason"] = f"coord_too_far(best_d={best_d:.3f}km > {AIRPORT_MATCH_RADIUS_KM}km)"

    # ---- 2) 名称匹配 ----
    cname = debug["cname"]
    hits = []
    hit_names = []

    if not cname:
        debug["name_reason"] = "empty_name_after_clean"
    else:
        for name, aids in name_map.items():
            cn_db = clean_name(name)
            if cn_db and cname in cn_db:
                hits.extend(aids)
                hit_names.append(name)

        debug["name_hits_count"] = len(hits)
        debug["name_hits_preview"] = hit_names[:5]

        if not hits:
            debug["name_reason"] = "name_zero_hits_in_db"
        else:
            # ---- 2.1) 歧义 → 优先 is_active ----
            active_hits = [aid for aid in hits if active_map.get(aid) == 1]
            if len(active_hits) == 1:
                return active_hits[0], "active-only-ok", debug
            if len(hits) == 1:
                return hits[0], "name-ok", debug
            debug["name_reason"] = f"name_ambiguous(hits={len(hits)}, active_hits={len(active_hits)})"

    # ---- 3) 歧义 → 高德 ----
    if debug["name_hits_count"] > 1 and debug["coord_ok"]:
        debug["gaode_called"] = True
        time.sleep(0.1)
        gname, gerr = gaode_nearest_airport(lat_f, lon_f)
        debug["gaode_name"] = gname
        debug["gaode_clean"] = clean_name(gname) if gname else None
        debug["gaode_reason"] = gerr

        if not gname:
            debug["gaode_reason"] = "gaode_no_pois_or_error"
        else:
            gclean = debug["gaode_clean"]
            if not gclean:
                debug["gaode_reason"] = "gaode_name_clean_empty"
            else:
                for name, aids in name_map.items():
                    db_clean = clean_name(name)
                    if db_clean and (gclean == db_clean or gclean in db_clean):
                        return aids[0], "gaode-ok", debug
                debug["gaode_reason"] = "gaode_name_not_matched_in_db"

    elif debug["name_hits_count"] > 1 and not debug["coord_ok"]:
        debug["gaode_reason"] = "gaode_skipped(no_valid_coord)"
    else:
        debug["gaode_reason"] = "gaode_skipped(not_ambiguous)"

    # ---- 4) 映射字典（清洗后匹配 key，解决 T1/T2 等）----
    debug["alias_tried"] = True
    c_raw = debug["cname"]
    if not c_raw:
        debug["alias_reason"] = "alias_skip(empty_clean_name)"
    else:
        key_hit = None
        for k in ALIAS_MAP.keys():
            if clean_name(k) == c_raw:
                key_hit = k
                break

        if not key_hit:
            debug["alias_reason"] = "alias_key_not_matched_by_clean_name"
        else:
            debug["alias_key"] = key_hit
            alias_target = ALIAS_MAP[key_hit]
            debug["alias_target"] = alias_target

            alias_clean = clean_name(alias_target)
            if not alias_clean:
                debug["alias_reason"] = "alias_target_clean_empty"
            else:
                for name, aids in name_map.items():
                    db_clean = clean_name(name)
                    if db_clean and alias_clean in db_clean:
                        debug["alias_hit"] = True
                        return aids[0], "alias-ok", debug
                debug["alias_reason"] = "alias_target_not_found_in_db"

    # ---- 最终 miss：归因 ----
    if not debug["coord_ok"] and debug["name_hits_count"] == 0 and not debug["alias_hit"]:
        debug["final_reason"] = "MISS(no_coord + name_zero_hits + alias_fail)"
    elif debug["coord_ok"] and debug["coord_reason"] and debug["name_hits_count"] == 0:
        debug["final_reason"] = f"MISS({debug['coord_reason']} + name_zero_hits + alias_fail)"
    elif debug["name_hits_count"] > 1:
        debug["final_reason"] = f"MISS(name_ambiguous + {debug['gaode_reason']})"
    else:
        debug["final_reason"] = "MISS(unclassified)"

    return None, "miss", debug

# ===============================
# 主流程
# ===============================
def main():
    df = load_df()
    conn = pymysql.connect(**DB_CONFIG)

    name_map, active_map, grid = load_airports(conn)

    # 起飞列
    dep_airport_col = "起飞机场"
    dep_lat_col = "起飞机场y"
    dep_lon_col = "起飞机场x"

    # 降落列（如果你CSV列名不同，改这里）
    arr_airport_col = "降落机场"
    arr_lat_col = "降落机场y"
    arr_lon_col = "降落机场x"

    dep_miss = 0
    arr_miss = 0

    for i, r in df.iterrows():
        # ---- 起飞匹配 ----
        dep_id, dep_why, dep_dbg = find_airport(
            r.get(dep_lat_col),
            r.get(dep_lon_col),
            r.get(dep_airport_col),
            name_map,
            active_map,
            grid
        )
        if not dep_id:
            dep_miss += 1
            if dep_miss <= 10:
                safe_print(
                    "[DEP MISS]",
                    f"raw={dep_dbg['raw']!r}",
                    f"clean={dep_dbg['cname']!r}",
                    f"lat={dep_dbg['lat_f']}",
                    f"lon={dep_dbg['lon_f']}",
                    f"coord_ok={dep_dbg['coord_ok']}",
                    f"coord_candidates={dep_dbg['coord_candidates']}",
                    f"best_d_km={dep_dbg['coord_best_d_km']}",
                    f"coord_reason={dep_dbg['coord_reason']}",
                    f"name_hits={dep_dbg['name_hits_count']}",
                    f"hits_preview={dep_dbg['name_hits_preview']}",
                    f"gaode={dep_dbg['gaode_called']}:{dep_dbg['gaode_clean']}",
                    f"gaode_reason={dep_dbg['gaode_reason']}",
                    f"alias_reason={dep_dbg['alias_reason']}",
                    f"alias_target={dep_dbg['alias_target']}",
                    f"final={dep_dbg['final_reason']}",
                )

        # ---- 降落匹配 ----
        arr_id, arr_why, arr_dbg = find_airport(
            r.get(arr_lat_col),
            r.get(arr_lon_col),
            r.get(arr_airport_col),
            name_map,
            active_map,
            grid
        )
        if not arr_id:
            arr_miss += 1
            if arr_miss <= 10:
                safe_print(
                    "[ARR MISS]",
                    f"raw={arr_dbg['raw']!r}",
                    f"clean={arr_dbg['cname']!r}",
                    f"lat={arr_dbg['lat_f']}",
                    f"lon={arr_dbg['lon_f']}",
                    f"coord_ok={arr_dbg['coord_ok']}",
                    f"coord_candidates={arr_dbg['coord_candidates']}",
                    f"best_d_km={arr_dbg['coord_best_d_km']}",
                    f"coord_reason={arr_dbg['coord_reason']}",
                    f"name_hits={arr_dbg['name_hits_count']}",
                    f"hits_preview={arr_dbg['name_hits_preview']}",
                    f"gaode={arr_dbg['gaode_called']}:{arr_dbg['gaode_clean']}",
                    f"gaode_reason={arr_dbg['gaode_reason']}",
                    f"alias_reason={arr_dbg['alias_reason']}",
                    f"alias_target={arr_dbg['alias_target']}",
                    f"final={arr_dbg['final_reason']}",
                )

        if (i + 1) % PROGRESS_EVERY == 0:
            safe_print(f"[进度] {i+1}/{len(df)} dep_miss={dep_miss} arr_miss={arr_miss}")

    conn.close()

if __name__ == "__main__":
    main()
