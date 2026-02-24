# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import pymysql
import re

# ====== 数据库配置 ======
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "bishe",
    "charset": "utf8mb4",
    "collation": "utf8mb4_0900_ai_ci",
}

WIKI_URL = "https://zh.wikipedia.org/wiki/中华人民共和国机场列表"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Referer": "https://zh.wikipedia.org/",
}

# ====== 清洗 IATA / ICAO ======
def clean_code(text):
    t = (text or "").strip().upper()
    if t in ["", "-", "—"]:
        return None
    return t

# ====== 清洗英文名：去脚注[1]、[a]、去多余空白、去括号内容(可选) ======
def clean_english_name(en: str):
    if not en:
        return None
    s = str(en)

    # 1) 去掉维基常见脚注：[1] [a] [12] 等（允许多个）
    s = re.sub(r"\[[^\]]*\]", "", s)

    # 2) 去掉上标脚注（有时会混入类似 ¹²³ 或 Unicode 上标字母）
    # 这里只做一个“保守版”：删除常见上标数字与符号
    s = re.sub(r"[¹²³⁴⁵⁶⁷⁸⁹⁰]+", "", s)

    # 3) 去掉括号里的补充说明（可选：如果你希望保留括号内容，把这行注释掉）
    #    例："Tianjin Binhai International Airport (ZBTJ)" -> "Tianjin Binhai International Airport"
    s = re.sub(r"\s*[\(\（][^)\）]*[\)\）]\s*", " ", s)

    # 4) 合并多余空白
    s = re.sub(r"\s+", " ", s).strip()

    return s or None

# ====== 拆分机场名：中文 / 英文 ======
def split_airport_name(raw_name: str):
    """
    输入: 例如 "天津滨海国际机场Tianjin Binhai International Airport"
          或 "北京首都国际机场 Beijing Capital International Airport"
          或带脚注："天津滨海国际机场[1] Tianjin ...[a]"
    输出: (cn, en_clean)
    """
    s = (raw_name or "").strip()
    if not s:
        return None, None

    # 1) 先去掉所有脚注 [..]（中文里也可能出现）
    s = re.sub(r"\[[^\]]*\]", "", s)

    # 2) 合并空白
    s = re.sub(r"\s+", " ", s).strip()

    # 3) 找到第一个英文/数字出现的位置（中文在前，英文在后）
    m = re.search(r"[A-Za-z0-9]", s)
    if not m:
        return s or None, None

    idx = m.start()
    cn = s[:idx].strip()
    en = s[idx:].strip()

    en = clean_english_name(en)
    return (cn or None), en

# ====== 判断颜色和运营状态 ======
def determine_is_active(row):
    tr_style = row.get("style", "")
    color = "white"
    is_active = True

    if "background-color" in tr_style:
        m = re.search(r"background-color:\s*(#[0-9A-Fa-f]{6})", tr_style)
        if m:
            hex_color = m.group(1).upper()
            color = hex_color
            # 黄色=建设中, 蓝色=规划中, 红色=停航 → 都视为非运营
            if hex_color in ["#FFFF99", "#BFDFFF", "#FFCCCC"]:
                is_active = False

    color_map = {
        "#FFFF99": "黄色",
        "#BFDFFF": "蓝色",
        "#FFCCCC": "红色",
        "white": "白色"
    }
    return color_map.get(color, color), is_active

# ====== 爬取机场数据 ======
def fetch_airports():
    print(f"开始爬取: {WIKI_URL}")
    response = requests.get(WIKI_URL, headers=HEADERS, timeout=15)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")

    airports = []

    tables = soup.find_all("table", {"class": "wikitable"})
    if not tables:
        print("未找到机场表格")
        return airports

    for table in tables:
        rows = table.find_all("tr")
        if not rows:
            continue

        # 解析表头索引
        header_cols = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
        col_index = {}
        for i, name in enumerate(header_cols):
            if "机场名称" in name:
                col_index["airport_name"] = i
            elif "IATA" in name:
                col_index["iata"] = i
            elif "ICAO" in name:
                col_index["icao"] = i
            elif "主服务城市" in name:
                col_index["city"] = i

        # 读取数据行
        for row in rows[1:]:
            cols = row.find_all(["td", "th"])
            if len(cols) < len(col_index):
                continue

            airport_name_raw = cols[col_index["airport_name"]].get_text(strip=True)
            airport_name_cn, airport_name_en = split_airport_name(airport_name_raw)

            city_name = cols[col_index["city"]].get_text(strip=True)
            iata_code = clean_code(cols[col_index["iata"]].get_text())
            icao_code = clean_code(cols[col_index["icao"]].get_text())

            color, is_active = determine_is_active(row)

            print(f"[获取] {airport_name_cn} | EN:{airport_name_en} | 城市:{city_name} | "
                  f"IATA:{iata_code} | ICAO:{icao_code} | "
                  f"颜色:{color} | 状态:{'运营' if is_active else '非运营'}")

            # 插入airport表时，将 address、longitude、latitude 置空
            airports.append((iata_code, icao_code, airport_name_cn, airport_name_en, None, is_active))

            # 遇到澳门特别行政区 → 停止后续爬取
            if "澳门特别行政区" in city_name:
                print(">>> 已遇到澳门特别行政区，停止后续爬取")
                return airports

    return airports

# ====== 插入数据库（运营优先去重） ======
def insert_airports(airports):
    if not airports:
        print("没有机场数据可插入")
        return

    # 用字典按 ICAO/IATA 去重，运营优先
    selected = {}
    replaced_log = []

    for rec in airports:
        iata, icao, name_cn, name_en, addr, active = rec
        key = icao or iata or name_cn or name_en  # 至少保证有key

        if key not in selected:
            selected[key] = rec
            continue

        old = selected[key]
        old_active = old[5]

        # 若新的是运营且旧的是非运营 → 替换
        if active and not old_active:
            replaced_log.append(old)
            selected[key] = rec
        else:
            replaced_log.append(rec)

    final_records = list(selected.values())

    # ====== 写入数据库 ======
    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()

    try:
        sql = """
            INSERT INTO airport (iata_code, icao_code, airport_name, airport_name_en, address, is_active)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                airport_name=VALUES(airport_name),
                airport_name_en=VALUES(airport_name_en),
                is_active=VALUES(is_active)
        """
        cursor.executemany(sql, final_records)
        conn.commit()

        print("\n====== 插入日志 ======")
        print("准备数据条数:", len(airports))
        print("去重后条数:", len(final_records))
        print("数据库受影响行数:", cursor.rowcount)

        if replaced_log:
            print("\n====== 以下记录因冲突被丢弃或替换 ======")
            for r in replaced_log:
                print(f"[忽略] {r[2]} | EN:{r[3]} | IATA:{r[0]} | ICAO:{r[1]} | 状态:{'运营' if r[5] else '非运营'}")
        print("===================================")

    except Exception as exception:
        print("数据库写入失败:", exception)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# ====== 主程序 ======
if __name__ == "__main__":
    try:
        airport_list = fetch_airports()
        insert_airports(airport_list)
    except Exception as e:
        print("运行失败:", e)
