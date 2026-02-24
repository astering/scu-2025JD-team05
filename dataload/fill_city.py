# -*- coding: utf-8 -*-
import re
import pymysql

# ====== 数据库配置 ======
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "bishe",
    "charset": "utf8mb4",
    "collation": "utf8mb4_0900_ai_ci",
}

AREA_SQL_FILE = "area_info.sql"

# ====== 过滤城市 ======
FILTER_CITY_NAMES = []  # 过滤城市

# ====== ISO2 → ISO3 补丁映射 ======
ISO2_TO_ISO3_PATCH = {
    "CN": "CHN",
}

# ---------------------------------------------------
# 解析 area_info.sql
# 你给的示例：
# INSERT INTO `area_info` VALUES
# ('4088','367',',4,140,367,4088,','4','斯普林菲尔德','Springfield','sipulinfeierde','SPI');
# 字段含义我们只用到：
# 1 area_id, 2 parent_id, 4 deep, 5 中文名, 7 拼音, 8 code(国家=ISO2/ISO3；城市=IATA等)
# ---------------------------------------------------
def parse_area_info(sql_file):
    area_records = {}

    # 更稳的正则：把第8列也当成可带引号/可为null的值处理
    insert_pattern = re.compile(
        r"^INSERT INTO `area_info` VALUES \("
        r"'(\d+)',\s*"          # 1 area_id
        r"'(\d+)',\s*"          # 2 parent_id
        r"'([^']*)',\s*"        # 3 path
        r"'(\d+)',\s*"          # 4 deep
        r"'([^']*)',\s*"        # 5 name_cn
        r"'([^']*)',\s*"        # 6 name_en
        r"'([^']*)',\s*"        # 7 name_pinyin
        r"(null|'[^']*')"       # 8 code (null or 'XXX')
        r"\);$"
    )

    with open(sql_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line.startswith("INSERT INTO"):
                continue

            m = insert_pattern.match(line)
            if not m:
                continue

            area_id = int(m.group(1))
            parent_id = int(m.group(2))
            area_deep = int(m.group(4))
            area_name_cn = m.group(5).strip()
            area_pinyin = m.group(7).strip()

            code_raw = m.group(8).strip()
            if code_raw.lower() == "null":
                code = None
            else:
                code = code_raw.strip("'").strip()

            area_records[area_id] = {
                "parent_id": parent_id,
                "deep": area_deep,
                "name": area_name_cn,
                "pinyin": area_pinyin,
                "code": code
            }

    return area_records


# ---------------------------------------------------
# 加载 province 表 province_name + country_id → id
# ---------------------------------------------------
def load_province_map():
    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()

    cursor.execute("SELECT id, province_name, country_id FROM province")
    mapping = {(name, country_id): pid for pid, name, country_id in cursor.fetchall()}

    cursor.close()
    conn.close()
    return mapping


# ---------------------------------------------------
# 加载 country 表 ISO3 → id
# ---------------------------------------------------
def load_country_code_map():
    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()

    cursor.execute("SELECT id, country_code FROM country")
    mapping = {code: cid for cid, code in cursor.fetchall()}

    cursor.close()
    conn.close()
    return mapping


# ---------------------------------------------------
# 构建 city 数据
# ---------------------------------------------------
def build_cities(area_records, country_code_map, province_map):
    cities = []
    skipped_cities = []

    for area_id, info in area_records.items():
        # 只处理城市层级
        if info["deep"] != 4:
            continue

        city_name = info["name"]
        city_pinyin = (info.get("pinyin") or "").strip()

        # city_name_pinyin 在表里是 NOT NULL，所以这里必须保证有值
        if not city_pinyin:
            skipped_cities.append((city_name, "缺少 city_name_pinyin"))
            continue

        # 过滤特定城市
        if city_name in FILTER_CITY_NAMES:
            skipped_cities.append((city_name, "过滤名单"))
            continue

        parent_id = info["parent_id"]
        if parent_id not in area_records:
            skipped_cities.append((city_name, "找不到父级省"))
            continue

        province_info = area_records[parent_id]

        # 父级必须是省/州层级
        if province_info["deep"] != 3:
            skipped_cities.append((city_name, "父级不是省/州"))
            continue

        province_name = province_info["name"]

        # 父级的父级必须是国家
        country_parent_id = province_info["parent_id"]
        if country_parent_id not in area_records:
            skipped_cities.append((city_name, "找不到父级国家"))
            continue

        country_info = area_records[country_parent_id]
        if country_info["deep"] != 2:
            skipped_cities.append((city_name, "父级国家层级错误"))
            continue

        iso_code = country_info["code"]
        if not iso_code:
            skipped_cities.append((city_name, "国家 ISO 代码缺失"))
            continue

        if len(iso_code) == 2:
            iso_code = ISO2_TO_ISO3_PATCH.get(iso_code, iso_code)

        if iso_code not in country_code_map:
            skipped_cities.append((city_name, f"国家 ISO3 {iso_code} 不在 country 表中"))
            continue

        country_id = country_code_map[iso_code]

        # province_id
        province_key = (province_name, country_id)
        if province_key not in province_map:
            skipped_cities.append((city_name, f"找不到 province_id ({province_name})"))
            continue
        province_id = province_map[province_key]

        #现在要插入 city_name_pinyin
        cities.append((city_name, city_pinyin, province_id))
        print(f"城市: {city_name}, 深度: {info['deep']}, 父级: {parent_id}")

    return cities, skipped_cities


# ---------------------------------------------------
# 写入 city 表
# ---------------------------------------------------
def insert_cities(cities):
    if not cities:
        print("没有城市数据可插入")
        return

    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()

    try:
        # 若你希望重复跑脚本也不会重复写入，可用 INSERT IGNORE（依赖 UNIQUE(city_name, province_id)）
        sql = """
        INSERT IGNORE INTO city (city_name, city_name_pinyin, province_id)
        VALUES (%s, %s, %s)
        """
        cursor.executemany(sql, cities)
        conn.commit()
        print(f"成功插入 {len(cities)} 条城市记录")
    except Exception as e:
        print("数据库写入失败:", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


# ---------------------------------------------------
# 主程序
# ---------------------------------------------------
if __name__ == "__main__":
    print("解析 area_info.sql ...")
    area_records = parse_area_info(AREA_SQL_FILE)

    print("加载 country ISO3 映射 ...")
    country_code_map = load_country_code_map()

    print("加载 province 映射 ...")
    province_map = load_province_map()

    print("构建 city 数据 ...")
    cities, skipped_cities = build_cities(area_records, country_code_map, province_map)

    print("写入 city 表 ...")
    insert_cities(cities)

    if skipped_cities:
        print("\n以下城市未插入 city 表:")
        for city_name, reason in skipped_cities:
            print(f"{city_name} → {reason}")
