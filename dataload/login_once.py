# -*- coding: utf-8 -*-
import os
import csv
import time
import random
import requests
import pymysql

# =======================
# 0. 基本配置
# =======================
CSV_PATH = "everyday_get.csv"

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "bishe",
    "port": 3306,
    "charset": "utf8mb4",
}

# CSV 表头（严格与你给的一致）
CSV_HEADERS = [
    "出发城市", "出发城市y", "出发城市x",
    "到达城市", "到达城市y", "到达城市x",
    "里程（公里）",
    "航班班次", "航空公司", "机型",
    "起飞机场", "起飞机场y", "起飞机场x",
    "降落机场", "降落机场y", "降落机场x",
    "准点率", "平均误点时间",
    "周一班期", "周二班期", "周三班期", "周四班期", "周五班期", "周六班期", "周日班期",
    "城市_x", "出发省份", "城市_y", "到达省份",
    "起飞时间", "降落时间", "日期", "价格(元)", "人数"
]

# =======================
# 1. 城市 → 三字码映射（请放完整）
# =======================
city_code_map = {'兴安盟': 'YIE', '阿克苏地区': 'AKU', '阿拉善右旗': 'RHT', '阿拉善左旗': 'AXF', '阿勒泰': 'AAT', '阿里': 'NGQ',
            '澳门': 'MFM',
            '安庆': 'AQG', '安顺': 'AVA', '鞍山': 'AOG', '巴彦淖尔': 'RLK', '百色': 'AEB', '包头': 'BAV', '保山': 'BSD',
            '北海': 'BHY',
            '北京': 'BJS', '白城': 'DBC', '白山': 'NBS', '毕节': 'BFJ', '博乐': 'BPL', '重庆': 'CKG', '昌都': 'BPX',
            '常德': 'CGD',
            '常州': 'CZX', '朝阳市': 'CHG', '成都': 'CTU', '池州': 'JUH', '赤峰': 'CIF', '揭阳': 'SWA', '长春': 'CGQ',
            '长沙': 'CSX',
            '长治': 'CIH', '承德': 'CDE', '沧源': 'CWJ', '达县': 'DAX', '大理': 'DLU', '大连': 'DLC', '大庆': 'DQA',
            '大同': 'DAT',
            '丹东': 'DDG', '稻城': 'DCY', '东营': 'DOY', '敦煌': 'DNH', '芒市': 'LUM', '额济纳旗': 'EJN',
            '鄂尔多斯': 'DSN', '恩施': 'ENH',
            '二连浩特': 'ERL', '佛山': 'FUO', '福州': 'FOC', '抚远': 'FYJ', '阜阳': 'FUG', '赣州': 'KOW',
            '格尔木': 'GOQ', '固原': 'GYU',
            '广元': 'GYS', '广州': 'CAN', '贵阳': 'KWE', '桂林': 'KWL', '哈尔滨': 'HRB', '哈密': 'HMI', '海口': 'HAK',
            '海拉尔': 'HLD',
            '邯郸': 'HDG', '汉中': 'HZG', '杭州': 'HGH', '合肥': 'HFE', '和田': 'HTN', '黑河': 'HEK', '呼和浩特': 'HET',
            '淮安': 'HIA',
            '怀化': 'HJJ', '黄山': 'TXN', '惠州': 'HUZ', '鸡西': 'JXA', '济南': 'TNA', '济宁': 'JNG', '加格达奇': 'JGD',
            '佳木斯': 'JMU',
            '嘉峪关': 'JGN', '金昌': 'JIC', '金门': 'KNH', '锦州': 'JNZ', '嘉义': 'CYI', '西双版纳': 'JHG',
            '建三江': 'JSJ', '晋江': 'JJN',
            '井冈山': 'JGS', '景德镇': 'JDZ', '九江': 'JIU', '九寨沟': 'JZH', '喀什': 'KHG', '凯里': 'KJH',
            '康定': 'KGT', '克拉玛依': 'KRY',
            '库车': 'KCA', '库尔勒': 'KRL', '昆明': 'KMG', '拉萨': 'LXA', '兰州': 'LHW', '黎平': 'HZH', '丽江': 'LJG',
            '荔波': 'LLB',
            '连云港': 'LYG', '六盘水': 'LPF', '临汾': 'LFQ', '林芝': 'LZY', '临沧': 'LNJ', '临沂': 'LYI', '柳州': 'LZH',
            '泸州': 'LZO',
            '洛阳': 'LYA', '吕梁': 'LLV', '澜沧': 'JMJ', '龙岩': 'LCX', '满洲里': 'NZH', '梅州': 'MXZ', '绵阳': 'MIG',
            '漠河': 'OHE',
            '牡丹江': 'MDG', '马祖': 'MFK', '南昌': 'KHN', '南充': 'NAO', '南京': 'NKG', '南宁': 'NNG', '南通': 'NTG',
            '南阳': 'NNY',
            '宁波': 'NGB', '宁蒗': 'NLH', '攀枝花': 'PZI', '普洱': 'SYM', '齐齐哈尔': 'NDG', '黔江': 'JIQ',
            '且末': 'IQM', '秦皇岛': 'BPE',
            '青岛': 'TAO', '庆阳': 'IQN', '衢州': 'JUZ', '日喀则': 'RKZ', '日照': 'RIZ', '三亚': 'SYX', '厦门市': 'XMN',
            '上海': 'SHA',
            '深圳': 'SZX', '神农架': 'HPG', '沈阳': 'SHE', '石家庄市': 'SJW', '塔城': 'TCG', '台州': 'HYN', '太原': 'TYN',
            '扬州': 'YTY',
            '唐山市': 'TVS', '腾冲': 'TCZ', '天津': 'TSN', '天水': 'THQ', '通辽': 'TGO', '铜仁': 'TEN', '吐鲁番': 'TLQ',
            '万州': 'WXN',
            '威海': 'WEH', '潍坊': 'WEF', '温州': 'WNZ', '文山': 'WNH', '乌海': 'WUA', '乌兰浩特': 'HLH',
            '乌鲁木齐': 'URC', '无锡': 'WUX',
            '梧州': 'WUZ', '武汉': 'WUH', '武夷山': 'WUS', '西安': 'SIA', '西昌': 'XIC', '西宁': 'XNN',
            '锡林浩特': 'XIL',
            '香格里拉(迪庆)': 'DIG',
            '襄阳': 'XFN', '兴义': 'ACX', '徐州': 'XUZ', '香港': 'HKG', '烟台': 'YNT', '延安': 'ENY', '延吉': 'YNJ',
            '盐城': 'YNZ',
            '伊春': 'LDS',
            '伊宁': 'YIN', '宜宾': 'YBP', '宜昌': 'YIH', '宜春': 'YIC', '义乌': 'YIW', '银川': 'INC', '永州': 'LLF',
            '榆林': 'UYN',
            '玉树': 'YUS',
            '运城': 'YCU', '湛江': 'ZHA', '张家界': 'DYG', '张家口': 'ZQZ', '张掖': 'YZY', '昭通': 'ZAT', '郑州': 'CGO',
            '中卫': 'ZHY',
            '舟山': 'HSN',
            '珠海市': 'ZUH', '遵义市': 'ZYI'}

# 反向：三字码 → 中文城市名
code_to_city = {v: k for k, v in city_code_map.items()}

# =======================
# 2. 机型映射（你确认的规则）
# =======================
AIRCRAFT_TYPE_MAP = {
    "321": "空客350(大)",
    "33J": "空客330(大)",
    "359": "空客330(大)",
    "JET": "空客321-200(中)",
    "320": "空客320(中)",
    "323": "空客320(中)",
    "32J": "空客320(中)",
    "32Q": "空客321(中)",
    "32A": "空客321(中)",
    "789": "空客321(中)",
    "332": "空客321(中)",
    "73K": "波音737(中)",
    "73M": "波音737(中)",
    "73G": "波音737(中)",
    "73V": "波音737(中)",
    "32N": "空客320(中)",
    "7CB": "波音738(中)",
    "325": "波音738(中)",
    "73E": "波音738(中)",
    "773": "波音777(大)",
    "738": "波音737(中)",
    "909": "C909(小)",
    "333": "空客330-300(大)",
    "919": "空客321(中)",
}

def map_aircraft_type(raw):
    if not raw:
        return ""
    return AIRCRAFT_TYPE_MAP.get(str(raw).strip().upper(), raw)

# =======================
# 3. 携程时刻表接口
# =======================
def get_schedule(dep_code, arr_code):
    s = requests.Session()
    ua = random.choice([
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121 Safari/537.36",
    ])

    page = f"https://flights.ctrip.com/schedule/{dep_code.lower()}.{arr_code.lower()}.html"

    #GET页面，种cookie
    try:
        s.get(page, headers={"User-Agent": ua}, timeout=10)
    except Exception as e:
        raise RuntimeError(f"preload page failed: {e}")

    url = "https://flights.ctrip.com/schedule/getScheduleByCityPair"
    headers = {
        "User-Agent": ua,
        "Accept": "application/json",
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": "https://flights.ctrip.com",
        "Referer": page,
    }
    payload = {
        "departureCityCode": dep_code,
        "arriveCityCode": arr_code
    }

    r = s.post(url, json=payload, headers=headers, timeout=10)

    #状态码兜底
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")

    #Content-Type 兜底
    ct = r.headers.get("Content-Type", "")
    if "application/json" not in ct:
        preview = r.text[:200].replace("\n", " ")
        raise RuntimeError(f"Non-JSON response: {ct}, preview={preview}")

    #JSON解析兜底
    try:
        return r.json()
    except Exception as e:
        preview = r.text[:200].replace("\n", " ")
        raise RuntimeError(f"JSON parse failed: {e}, preview={preview}")

# =======================
# 4. CSV 工具
# =======================
def ensure_csv():
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, "w", newline="", encoding="utf-8-sig") as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writeheader()

def load_existing_keys():
    keys = set()
    if not os.path.exists(CSV_PATH):
        return keys
    with open(CSV_PATH, "r", newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            keys.add((
                r.get("航班班次",""),
                r.get("日期",""),
                r.get("起飞时间",""),
                r.get("起飞机场",""),
                r.get("降落机场",""),
                r.get("出发城市",""),
                r.get("到达城市",""),
            ))
    return keys

# =======================
# 5. DB：机型 → 人数
# =======================
def load_aircraft_capacity():
    mp = {}
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT aircraft_type, aircraft_type_norm, max_people
                FROM aircraft_capacity
            """)
            for a, n, p in cur.fetchall():
                if a:
                    mp[a] = p
                if n:
                    mp[n] = p
    finally:
        conn.close()
    return mp

# =======================
# 6. DB：Top10 hotness → 聚合城市名
# =======================
def get_top10_target_cities():
    """
    返回：['北京','上海','广州',...]
    """
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT
                  CASE
                    WHEN p.province_name IN ('北京','天津','上海','重庆')
                      THEN p.province_name
                    ELSE c.city_name
                  END AS target_name
                FROM (
                  SELECT city_id
                  FROM city_airport
                  ORDER BY city_hotness DESC, city_id ASC
                  LIMIT 10
                ) t
                JOIN city c ON c.id = t.city_id
                JOIN province p ON p.id = c.province_id
            """)
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

# =======================
# 7. 数据清洗工具
# =======================
def parse_on_time_rate(s):
    if not s:
        return ""
    s = str(s).strip()
    if s.endswith("%"):
        try:
            return round(float(s[:-1]) / 100, 4)
        except:
            return ""
    return ""

def week_text(day, v):
    return f"{day}{'有班期' if v else '没有班期'}"

# =======================
# 8. 单条航班 → CSV 行
# =======================
def schedule_to_row(item, dep_city, arr_city, cap_map):
    row = {h: "" for h in CSV_HEADERS}
    week = item.get("currentWeekSchedule") or {}

    aircraft_cn = map_aircraft_type(item.get("aircraftType"))
    people = cap_map.get(aircraft_cn, "")

    row.update({
        "出发城市": dep_city,
        "到达城市": arr_city,
        "航班班次": item.get("flightNo"),
        "航空公司": item.get("airlineCompanyName"),
        "机型": aircraft_cn,
        "起飞机场": item.get("departPortName"),
        "降落机场": item.get("arrivePortName"),
        "起飞时间": item.get("departTime"),
        "降落时间": item.get("arriveTime"),
        "日期": item.get("departDate"),
        "价格(元)": item.get("price") or "",
        "准点率": parse_on_time_rate(item.get("onTimeRate")),
        "平均误点时间": "",
        "周一班期": week_text("周一", week.get("1")),
        "周二班期": week_text("周二", week.get("2")),
        "周三班期": week_text("周三", week.get("3")),
        "周四班期": week_text("周四", week.get("4")),
        "周五班期": week_text("周五", week.get("5")),
        "周六班期": week_text("周六", week.get("6")),
        "周日班期": week_text("周日", week.get("7")),
        "人数": people,
    })
    return row

# =======================
# 9. 主流程
# =======================
def main():
    dep_code = input("请输入出发城市三字码（如 BJS/CKG/SHA）：").strip().upper()
    if dep_code not in code_to_city:
        print("出发城市三字码不在字典中")
        return

    dep_city = code_to_city[dep_code]

    targets = get_top10_target_cities()
    print("Top10 热门城市（聚合后）：", targets)

    ensure_csv()
    existing = load_existing_keys()
    cap_map = load_aircraft_capacity()

    total = 0

    for arr_city in targets:
        arr_code = city_code_map.get(arr_city)
        if not arr_code:
            print(f"[跳过] 无三字码映射：{arr_city}")
            continue
        if arr_code == dep_code:
            continue

        try:
            data = get_schedule(dep_code, arr_code)
            lst = data.get("scheduleVOList") or []
        except Exception as e:
            print(f"[跳过] {dep_code} → {arr_code} 获取失败：{e}")
            continue

        rows = []
        for item in lst:
            row = schedule_to_row(item, dep_city, arr_city, cap_map)
            k = (
                row["航班班次"], row["日期"], row["起飞时间"],
                row["起飞机场"], row["降落机场"],
                row["出发城市"], row["到达城市"]
            )
            if k in existing:
                continue
            rows.append(row)
            existing.add(k)

        if rows:
            with open(CSV_PATH, "a", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=CSV_HEADERS)
                w.writerows(rows)
            print(f"[写入] {dep_city} → {arr_city} : {len(rows)} 条")
            total += len(rows)

        time.sleep(random.uniform(5.0, 10.0))

    print(f"完成，共写入 {total} 条记录 → {CSV_PATH}")

# =======================
# 入口
# =======================
if __name__ == "__main__":
    main()
