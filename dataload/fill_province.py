# -*- coding: utf-8 -*-
#fill_province.py
import re
import pymysql

#MySQL 连接
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "bishe",
    "charset": "utf8mb4",
    "collation": "utf8mb4_0900_ai_ci",
}

#从area_info.sql文件获取
AREA_SQL_FILE = "area_info.sql"

#过滤条件(暂定)
FILTER_PROVINCE_NAME = ""

#ISO2→ISO3补丁映射(area_info.sql中中国记录为ISO 2代码)
ISO2_TO_ISO3_PATCH = {
    "CN": "CHN",  # 中国
}

def parse_area_info(sql_file):
    """
    解析area_info.sql
    """
    area_records = {}
    #灵活匹配INSERT INTO area_info 的行
    insert_pattern = re.compile(
        r"INSERT INTO `area_info` VALUES\s*\(\s*'(\d+)'\s*,\s*'(\d+)'\s*,\s*'([^']*)'\s*,\s*'(\d+)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*([^)]*)\s*\)\s*;"
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
            area_name = m.group(5)
            code_raw = m.group(8).strip()
            code = None if code_raw == "null" else code_raw.strip("'")

            area_records[area_id] = {
                "parent_id": parent_id,
                "deep": area_deep,
                "name": area_name,
                "code": code
            }
    return area_records

# ---------------------------------------------------
# 读取 country 表 ISO3 → id
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
# 构建 province 数据
# ---------------------------------------------------
def build_provinces(area_records, country_code_map):
    provinces = []

    for area_id, info in area_records.items():
        # 只处理省/州层级
        if info["deep"] != 3:
            continue

        province_name = info["name"]
        if province_name == FILTER_PROVINCE_NAME:
            continue

        parent_id = info["parent_id"]
        if parent_id not in area_records:
            print(f"跳过 {province_name}，父级 {parent_id} 不在 area_records")
            continue

        parent = area_records[parent_id]
        if parent["deep"] != 2:
            print(f"跳过 {province_name}，父级 {parent['name']} deep={parent['deep']} != 2")
            continue  # 父级必须是国家

        iso_code = parent["code"]
        if not iso_code:
            print(f"跳过 {province_name}，父级 {parent['name']} code 为空")
            continue

        # ISO2 → ISO3 修正
        if len(iso_code) == 2:
            iso_code = ISO2_TO_ISO3_PATCH.get(iso_code, iso_code)

        if iso_code not in country_code_map:
            print(f"跳过 {province_name}，父级国家 code {iso_code} 未在 country 表")
            continue

        country_id = country_code_map[iso_code]
        provinces.append((province_name, country_id))


    return provinces

# ---------------------------------------------------
# 写入 province 表（不删除已有数据，避免外键冲突）
# ---------------------------------------------------
def insert_provinces(provinces):
    if not provinces:
        print("没有省份数据可插入")
        return

    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()

    try:
        sql = """
        INSERT INTO province (province_name, country_id)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE province_name=province_name
        """
        cursor.executemany(sql, provinces)
        conn.commit()
        print(f"成功插入 {len(provinces)} 条省/州记录")
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

    print("构建 province 数据 ...")
    provinces = build_provinces(area_records, country_code_map)

    print("写入 province 表 ...")
    insert_provinces(provinces)
