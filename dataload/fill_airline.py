# -*- coding: utf-8 -*-
import re
import time
import requests
import pymysql
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# ===============================
# 1) 配置
# ===============================
URL_MAINLAND = "https://zh.wikipedia.org/wiki/中国大陆航空公司列表"
URL_HK = "https://zh.wikipedia.org/wiki/香港航空公司列表"
URL_MACAU = "https://zh.wikipedia.org/wiki/澳门航空公司列表"
URL_TAIWAN = "https://zh.wikipedia.org/wiki/臺灣航空公司列表"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9",
}

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "bishe",
    "charset": "utf8mb4",
    "collation": "utf8mb4_0900_ai_ci",

}

DEFAULT_PRICE_LABEL = "medium"                 # enum: cheap/medium/luxury
DEFAULT_LOGO_URL = "http://airline.com"        # 表默认值
SLEEP_SEC = 0.3                                # 给维基一点喘息


# ===============================
# 2) 清洗工具
# ===============================
def clean_text(s: str) -> str:
    """去脚注[1]、合并空白、去括号说明（保守）"""
    if s is None:
        return ""
    s = str(s)
    s = re.sub(r"\[[^\]]*\]", "", s)                       # 去维基脚注
    s = re.sub(r"[¹²³⁴⁵⁶⁷⁸⁹⁰]+", "", s)                   # 去常见上标数字
    s = re.sub(r"\s*[\(\（][^)\）]*[\)\）]\s*", " ", s)     # 去括号说明
    s = re.sub(r"\s+", " ", s).strip()
    return s


def is_na_like(s: str) -> bool:
    t = (s or "").strip().upper()
    return t in {"", "-", "—", "N/A", "NA", "NONE", "NULL"}


def extract_iata(raw: str):
    """抽 2 位 IATA（允许数字：如 8L / 3U / I9），但过滤 N/A"""
    s0 = clean_text(raw).upper()
    if is_na_like(s0):
        return None
    # 防止 "N/A" 被误抽成 "NA"
    if "N/A" in (raw or "").upper():
        return None
    m = re.search(r"\b([A-Z0-9]{2})\b", s0)
    return m.group(1) if m else None


def extract_icao(raw: str):
    """抽 3 位 ICAO（允许数字），但过滤 N/A"""
    s0 = clean_text(raw).upper()
    if is_na_like(s0):
        return None
    if "N/A" in (raw or "").upper():
        return None
    m = re.search(r"\b([A-Z0-9]{3})\b", s0)
    return m.group(1) if m else None


# ===============================
# 3) 解析 logo_url（维基“图像/图/Logo”列）
# ===============================
def normalize_wiki_img_url(img_url: str) -> str:
    """
    - //upload.wikimedia.org/... -> https://upload.wikimedia.org/...
    - /thumb/.../120px-XXX.png -> 还原原图 /.../XXX
    """
    if not img_url:
        return ""

    u = img_url.strip()

    if u.startswith("//"):
        u = "https:" + u
    elif u.startswith("/"):
        u = urljoin("https://zh.wikipedia.org/", u)

    # 缩略图还原成原图
    if "/thumb/" in u:
        try:
            prefix, rest = u.split("/thumb/", 1)
            parts = rest.split("/")
            if len(parts) >= 3:
                u = prefix + "/" + "/".join(parts[:3])
        except Exception:
            pass

    return u


def extract_logo_url_from_cell(cell, base_url: str) -> str:
    """从单元格里拿 img 的 data-src/src/srcset"""
    if cell is None:
        return ""

    img = cell.find("img")
    if not img:
        return ""

    cand = img.get("data-src") or img.get("src") or ""

    if not cand:
        srcset = img.get("srcset") or ""
        if srcset:
            cand = srcset.split(",")[0].strip().split(" ")[0].strip()

    if not cand:
        return ""

    return normalize_wiki_img_url(cand)


# ===============================
# 4) 表头识别与表格遍历
# ===============================
def table_header_map(table) -> dict:
    """从表头定位 name/iata/icao/logo 列"""
    head_row = table.find("tr")
    if not head_row:
        return {}
    heads = [clean_text(th.get_text(" ", strip=True)) for th in head_row.find_all(["th", "td"])]

    col = {}
    for i, h in enumerate(heads):
        hu = h.upper()
        if "IATA" in hu:
            col["iata"] = i
        elif "ICAO" in hu:
            col["icao"] = i
        elif "航空公司" in h or h in ("公司", "航空公司名稱", "航空公司名称"):
            col["name"] = i
        # ✅ 大陆页面表头是“图”，也要识别；并保留你之前的“图像/Logo/标志”等
        elif ("LOGO" in hu) or any(k in h for k in ["图", "图像", "圖片", "徽标", "標誌", "标志", "標章"]):
            col["logo"] = i

    return col


def iter_table_rows(table):
    rows = table.find_all("tr")
    for r in rows[1:]:
        cols = r.find_all(["td", "th"])
        if cols:
            yield cols


# ===============================
# 关键修复：取“标题路径”，而不是只取最近一个标题
# ===============================
def _get_prev_heading_text(node, tag_name: str) -> str:
    """取表格上方最近的某级标题(h2/h3/h4)的文本（优先 mw-headline）"""
    h = node.find_previous(tag_name)
    if not h:
        return ""
    sp = h.find("span", class_="mw-headline")
    txt = sp.get_text(" ", strip=True) if sp else h.get_text(" ", strip=True)
    return clean_text(txt)


def nearest_section_title(node) -> str:
    """
    ✅ 返回 h2/h3/h4 的“路径”，例如：
    - 客运航空公司 / 主要
    - 客运航空公司 / 非主要
    - 已停止运行
    这样大陆页就不会漏
    """
    h2 = _get_prev_heading_text(node, "h2")
    h3 = _get_prev_heading_text(node, "h3")
    h4 = _get_prev_heading_text(node, "h4")

    parts = [p for p in [h2, h3, h4] if p]
    return " / ".join(parts)


def parse_tables_by_rule(url: str, accept_table_fn, source_tag: str):
    """
    返回 records:
    [(iata, icao, airline_name, logo_url, source_tag)]
    """
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    records = []
    tables = soup.find_all("table", class_="wikitable")

    for table in tables:
        # ✅ 现在 sec_title 是 “h2/h3/h4 路径”
        sec_title = nearest_section_title(table)

        if not accept_table_fn(table, sec_title):
            continue

        col = table_header_map(table)
        if not {"name", "iata", "icao"}.issubset(col.keys()):
            continue

        for cols in iter_table_rows(table):
            if len(cols) <= max(col.values()):
                continue

            name_raw = cols[col["name"]].get_text(" ", strip=True)
            iata_raw = cols[col["iata"]].get_text(" ", strip=True)
            icao_raw = cols[col["icao"]].get_text(" ", strip=True)

            name = clean_text(name_raw)
            iata = extract_iata(iata_raw)
            icao = extract_icao(icao_raw)

            if not name or not iata or not icao:
                continue
            if len(iata) != 2 or len(icao) != 3:
                continue

            logo_url = ""
            if "logo" in col and col["logo"] < len(cols):
                logo_url = extract_logo_url_from_cell(cols[col["logo"]], url)

            records.append((iata, icao, name, logo_url, source_tag))

    # 同 (IATA, ICAO) 去重，保留“有 logo 的优先”
    uniq = {}
    for iata, icao, name, logo_url, tag in records:
        k = (iata, icao)
        if k not in uniq:
            uniq[k] = (iata, icao, name, logo_url, tag)
        else:
            old = uniq[k]
            if (not old[3]) and logo_url:
                uniq[k] = (iata, icao, name, logo_url, tag)

    return list(uniq.values())


# ===============================
# 5) 规则：大陆/香港/澳门/台湾
# ===============================
def accept_mainland_passenger(_table, section_title: str) -> bool:
    """
    section_title 现在形如：
      客运航空公司 / 主要
      客运航空公司 / 非主要
      已停止运行 / ...
    所以这个判断会正确命中“主要+非主要”
    """
    return ("客运航空公司" in section_title) and ("已停止" not in section_title) and ("货运" not in section_title)


def accept_hk_scheduled(_table, section_title: str) -> bool:
    return "定期航班" in section_title


def accept_macau_scheduled(_table, section_title: str) -> bool:
    return "定期航班" in section_title


def accept_taiwan_all(_table, section_title: str) -> bool:
    bad = ("已停止" in section_title) or ("停止" in section_title) or ("已结业" in section_title) or ("已結業" in section_title)
    return not bad


# ===============================
# 6) 入库
# ===============================
def insert_airlines(all_records):
    if not all_records:
        print("没有可插入的航空公司记录。")
        return

    # 全局去重（跨地区）
    uniq = {}
    for iata, icao, name, logo_url, tag in all_records:
        k = (iata, icao)
        if k not in uniq:
            uniq[k] = (iata, icao, name, logo_url, tag)
        else:
            old = uniq[k]
            if (not old[3]) and logo_url:
                uniq[k] = (iata, icao, name, logo_url, tag)

    final_records = list(uniq.values())

    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        sql = """
        INSERT INTO airline
            (airline_iata_code, airline_icao_code, airline_name, logo_url, price_label, incident)
        VALUES
            (%s, %s, %s, %s, %s, 0)
        ON DUPLICATE KEY UPDATE
            airline_name = VALUES(airline_name),
            logo_url     = VALUES(logo_url),
            price_label  = VALUES(price_label)
        """

        payload = []
        for iata, icao, name, logo_url, _tag in final_records:
            logo_to_store = logo_url if logo_url else DEFAULT_LOGO_URL
            payload.append((iata, icao, name, logo_to_store, DEFAULT_PRICE_LABEL))

        cursor.executemany(sql, payload)
        conn.commit()

        print("写入完成：")
        print("  解析总数(去重前)：", len(all_records))
        print("  去重后写入数：    ", len(final_records))
        print("  受影响行数(含更新)：", cursor.rowcount)

        stat = {}
        for _iata, _icao, _name, _logo, tag in final_records:
            stat[tag] = stat.get(tag, 0) + 1
        print("  来源统计：", stat)

        ex = [r for r in final_records if r[3] and r[3] != DEFAULT_LOGO_URL][:5]
        if ex:
            print("\nlogo_url 示例（前5条）：")
            for r in ex:
                print(f"  {r[2]} | {r[0]}/{r[1]} | {r[3]}")

    except Exception as e:
        conn.rollback()
        print("数据库写入失败：", repr(e))
    finally:
        cursor.close()
        conn.close()


# ===============================
# 7) 主程序
# ===============================
def main():
    all_records = []

    print("抓取：中国大陆（仅客运航空公司，含主要/非主要）...")
    all_records += parse_tables_by_rule(URL_MAINLAND, accept_mainland_passenger, "CN-Mainland-Passenger")
    time.sleep(SLEEP_SEC)

    print("抓取：香港（定期航班）...")
    all_records += parse_tables_by_rule(URL_HK, accept_hk_scheduled, "HK-Scheduled")
    time.sleep(SLEEP_SEC)

    print("抓取：澳门（定期航班）...")
    all_records += parse_tables_by_rule(URL_MACAU, accept_macau_scheduled, "MO-Scheduled")
    time.sleep(SLEEP_SEC)

    print("抓取：台湾（航空列表）...")
    all_records += parse_tables_by_rule(URL_TAIWAN, accept_taiwan_all, "TW-Airlines")
    time.sleep(SLEEP_SEC)

    print("写入 airline 表...")
    insert_airlines(all_records)


if __name__ == "__main__":
    main()
