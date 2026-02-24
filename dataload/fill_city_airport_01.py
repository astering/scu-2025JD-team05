# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import pymysql
import re

# ===== 数据库配置 =====
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "bishe",
    "charset": "utf8mb4",
    "collation": "utf8mb4_0900_ai_ci",
}

WIKI_URL = "https://zh.wikipedia.org/wiki/中华人民共和国机场列表"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# ===== 直辖市映射 =====
municipalities = {"北京市":"北京","天津市":"天津","上海市":"上海","重庆市":"重庆","香港特别行政区": "香港",
    "澳门特别行政区": "澳门",}

# ===== 直辖市机场-区县映射 =====
MUNICIPALITY_AIRPORT_DISTRICTS = {
    "北京首都国际机场": ["朝阳区", "顺义区"],
    "北京大兴国际机场": ["大兴区"],
    "上海浦东国际机场": ["浦东新区"],
    "上海虹桥国际机场": ["闵行区", "长宁区"],
    "天津滨海国际机场": ["东丽区"],
    "重庆江北国际机场": ["渝北区"],
    "重庆新机场（第二机场）": ["璧山县"],
    "香港国际机场": ["离岛区"],
    "澳门国际机场": ["氹仔"],
}

# ===== 人工修正映射 =====
CITY_NAME_MAPPING = {
    "长白山管委会": "白山市",
    "建三江管委会": "三江侗族自治县",
    "抚远市": "抚远县",
    "东宁市": "东宁县",
    "漠河市": "漠河县",
    "襄阳市": "襄阳郡",
    "武隆区": "武隆县",
    "马尔康市": "马尔康县",
    "康定市": "康定县",
    "西昌市": "西昌",
    "盘州市": "盘县",
    "腾冲市": "腾冲县",
    "普洱市": "普洱",
    "文山市": "文山县",
    "芒市": "潞西市",
    "香格里拉市": "香格里拉县",
    "蒙自市": "蒙自县",
    "泸水市": "怒江傈僳族自治州",
    "昌都市": "昌都县",
    "林芝市": "林芝县",
    "玉树市": "玉树县",
    "茫崖市": "茫崖行委",
    "同仁市": "同仁县",
    "巴音布鲁克镇":"和静县",
    "库车市": "库车县",
    "铁门关市": "巴音郭楞蒙古自治州",
    "亚布力镇": "哈尔滨市",
    "澜沧拉祜族自治县": "普洱",
    "且末县": "巴音郭楞蒙古自治州",
}

# ===== 特殊拆分规则 =====
SPECIAL_SPLIT_CASES = {
    "加格达奇区鄂伦春自治旗": ["大兴安岭地区加格达奇区", "鄂伦春自治旗"],
    "铜仁市凤凰县": ["铜仁市", "凤凰县"],
}

# ===== 数据库连接 =====
def get_conn():
    return pymysql.connect(**db_config)

def get_airport_id(airport_name):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM airport WHERE airport_name=%s", (airport_name,))
    r = cur.fetchone()
    if not r:
        # 完全匹配失败，尝试模糊匹配
        print(f"[日志] 无法完全匹配机场: {airport_name}，进行模糊匹配")
        cur.execute("SELECT id FROM airport WHERE airport_name LIKE %s", ('%' + airport_name + '%',))
        r = cur.fetchone()
    cur.close()
    conn.close()
    return r[0] if r else None

def get_city_id(city_name):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM city WHERE city_name=%s", (city_name,))
    r = cur.fetchone()
    cur.close()
    conn.close()
    return r[0] if r else None

def get_province_id(province_name):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM province WHERE province_name=%s", (province_name,))
    r = cur.fetchone()
    cur.close()
    conn.close()
    return r[0] if r else None

def get_parent_city_from_area_info(area_name):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT area_parent_id 
        FROM area_info 
        WHERE area_name=%s AND area_deep=5
    """, (area_name,))
    r = cur.fetchone()
    if not r:
        cur.close()
        conn.close()
        return None
    parent_id = r[0]
    cur.execute("SELECT area_name FROM area_info WHERE area_id=%s", (parent_id,))
    r2 = cur.fetchone()
    cur.close()
    conn.close()
    return r2[0] if r2 else None

def insert_city_airport(airport_id, city_id):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT IGNORE INTO city_airport(airport_id, city_id,airport_hotness, city_hotness)
            VALUES(%s,%s,%s,%s)
        """, (airport_id, city_id, 0.0 ,0.0))
        conn.commit()
        print(f"写入 city_airport airport_id={airport_id}, city_id={city_id}")
    except Exception as e:
        print("写入失败:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def split_city_names(city_str):
    if "市" in city_str and len(city_str) > 3:
        return re.findall(r".+?市", city_str)
    return [city_str]

def normalize_city_str(city_str):
    city_str = city_str.strip()
    if city_str in SPECIAL_SPLIT_CASES:
        return SPECIAL_SPLIT_CASES[city_str]
    city_str = CITY_NAME_MAPPING.get(city_str, city_str)
    city_list = split_city_names(city_str)
    city_list = [CITY_NAME_MAPPING.get(c, c) for c in city_list]
    return city_list

# ===== 主爬取与处理 =====
def fill_city_airport():
    print("开始解析维基机场城市信息...")
    resp = requests.get(WIKI_URL, headers=HEADERS, timeout=15)
    soup = BeautifulSoup(resp.text, "html.parser")
    tables = soup.find_all("table", {"class": "wikitable"})

    for table in tables:
        rows = table.find_all("tr")
        if not rows:
            continue
        headers = [th.get_text(strip=True) for th in rows[0].find_all(["th","td"])]
        col_index = {}
        for i,h in enumerate(headers):
            if "机场名称" in h: col_index["airport_name"]=i
            if "主服务城市" in h: col_index["city"]=i

        for row in rows[1:]:
            cols = row.find_all("td")
            if len(cols) < len(col_index):
                continue

            airport_name_raw = cols[col_index["airport_name"]].get_text(strip=True)
            city_str = cols[col_index["city"]].get_text(strip=True)
            if not airport_name_raw or not city_str:
                continue

            # 提取中文机场名（去掉英文部分）
            airport_name_cn = re.match(r"[\u4e00-\u9fa5]+", airport_name_raw)
            airport_name_cn = airport_name_cn.group(0) if airport_name_cn else airport_name_raw

            airport_id = get_airport_id(airport_name_cn)
            if not airport_id:
                print(f"[跳过] 数据库不存在机场 {airport_name_cn}")
                continue

            # ---- 先检查是否是 MUNICIPALITY_AIRPORT_DISTRICTS 映射机场 ----
            if airport_name_cn in MUNICIPALITY_AIRPORT_DISTRICTS:
                districts = MUNICIPALITY_AIRPORT_DISTRICTS[airport_name_cn]
                city_list = districts
            else:
                city_list = normalize_city_str(city_str)

            for city_name in city_list:
                print(f"[处理] {airport_name_cn} → {city_name}")

                # ---- 遇到澳门特别行政区，先插入再停止 ----
                if city_name == "澳门特别行政区" or city_name == "澳门":
                    city_id = get_city_id(city_name)
                    if city_id:
                        insert_city_airport(airport_id, city_id)
                    print(">>> 已遇到澳门特别行政区，停止后续爬取")
                    return  # 停止整个函数

                city_id = None

                # ---- 直辖市处理 ----
                if city_name in municipalities:
                    prov = municipalities[city_name]
                    city_id = get_province_id(prov)
                    if city_id:
                        print(f"  直辖市 → province({prov}) id={city_id}")

                # ---- 普通 city 表 ----
                if not city_id:
                    city_id = get_city_id(city_name)
                    if city_id:
                        print(f"  city表命中 id={city_id}")

                # ---- area_info 父级 ----
                if not city_id:
                    parent = get_parent_city_from_area_info(city_name)
                    if parent:
                        print(f"  area_info 父级 {parent}")
                        city_id = get_city_id(parent)

                if not city_id:
                    print(f"城市无法匹配，跳过")
                    continue

                insert_city_airport(airport_id, city_id)

    print("city_airport 填充完成")

# ===== 入口 =====
if __name__ == "__main__":
    fill_city_airport()
