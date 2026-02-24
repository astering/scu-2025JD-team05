# -*- coding: utf-8 -*-
#fill_country.py
import requests
from bs4 import BeautifulSoup
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

#国家代码页面
url = "https://uutool.cn/info-nation/"

def fetch_country_list():
    """
    抓取国家和ISO-3代码
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/144.0.0.0 Safari/537.36"
    }

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.encoding = resp.apparent_encoding
        if resp.status_code != 200:
            print("请求失败:", resp.status_code)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        rows = soup.select("table tr")
        result = []

        for r in rows[1:]:  #跳过表头
            cols = r.find_all("td")
            if len(cols) >= 3:
                cn_name = cols[-2].get_text(strip=True)  #中文名称列
                alpha3 = cols[1].get_text(strip=True)   #ISO-3 列

                if cn_name and alpha3 and len(alpha3) == 3:
                    result.append((cn_name, alpha3))
        return result

    except Exception as e:
        print("抓取失败:", e)
        return []

def insert_to_db(country_data):
    """
    将国家数据插入数据库（不清表，避免外键约束冲突）
    """
    if not country_data:
        print("没有新国家数据要插入。")
        return

    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()
    try:
        #不清表，直接插入
        sql = "INSERT INTO country (country_name, country_code) VALUES (%s, %s)"
        cursor.executemany(sql, country_data)
        conn.commit()
        print("插入成功，共 %d 条记录" % len(country_data))
    except Exception as e:
        print("数据库插入失败:", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    print("抓取国家数据中")
    countries = fetch_country_list()
    if not countries:
        print("未获取到国家列表!")
    else:
        insert_to_db(countries)
