# -*- coding: utf-8 -*-
"""
main.py
顺序执行多个 ETL / fill 脚本
- 任一脚本失败则终止
- 打印耗时与错误信息
"""

import subprocess
import sys
import time
from pathlib import Path

# ============ 配置 ============
PYTHON = sys.executable          # 当前虚拟环境 python
BASE_DIR = Path(__file__).parent

SCRIPTS = [
    "fill_dataset.py",
    "fill_country.py",
    "fill_province.py",
    "fill_city.py",
    "fill_airport_01.py",
    "fill_airport_02.py",
    "fill_airport_02.py", # 补充执行一次，防止因网络问题造成遗漏
    "fill_airline.py",
    "fill_city_airport_01.py"
    "fill_flight.py",
    "fill_flight_seats.py",
    "fill_city_hotness_monthly.py",
]
# ==============================

def run_script(script):
    print(f"\n开始执行 {script}")
    start = time.time()

    result = subprocess.run(
        [PYTHON, script],
        cwd=BASE_DIR,
        stdout=sys.stdout,   # 实时打印
        stderr=sys.stderr
    )

    cost = time.time() - start
    if result.returncode != 0:
        raise RuntimeError(f"{script} 执行失败（{cost:.1f}s）")
    print(f"{script} 完成（{cost:.1f}s）")

def main():
    total_start = time.time()
    try:
        for script in SCRIPTS:
            run_script(script)
    except Exception as e:
        print(f"\n执行中断：{e}")
        sys.exit(1)

    print(f"\n全部完成，总耗时 {time.time() - total_start:.1f}s")

if __name__ == "__main__":
    main()
