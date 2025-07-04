# ai优化，尝试并行化加速，无效

import sys
import json
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

MSD_WRAPPERS_BASE = "/msd_wrappers" # 不行就改成绝对路径
sys.path.append(MSD_WRAPPERS_BASE)
# 如果只用hdf5_getters这一个，直接把hdf5_getters.py复制到同路径下就行
import hdf5_getters

def write_json(h5_path, songidx):
    # 每个线程独立打开文件，避免句柄冲突
    h5 = hdf5_getters.open_h5_file_read(h5_path)
    # get all getters
    getters = list(filter(lambda x: x[:4] == 'get_', list(hdf5_getters.__dict__.keys())))
    getters.remove("get_num_songs") # special case
    getters = np.sort(getters)

    data = {}
    for getter in getters:
        try:
            res = getattr(hdf5_getters, getter)(h5, songidx)
        except AttributeError:
            continue
        data[str(getter[4:])] = str(res)
    h5.close()
    return json.dumps(data)

def main():
    if len(sys.argv) != 3:
        print("Usage: ods_loader.py <input_path> <output_path>", file=sys.stderr)
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    h5 = hdf5_getters.open_h5_file_read(input_path)
    numSongs = hdf5_getters.get_num_songs(h5)
    h5.close()

    # 多线程处理，限制线程数，使用tqdm显示进度
    results = [None] * numSongs
    max_workers = 8  # 可根据机器性能调整
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {}
        with tqdm(total=numSongs, desc="Processing", ncols=80) as pbar:
            for i in range(numSongs):
                future = executor.submit(write_json, input_path, i)
                future_to_idx[future] = i
            for future in as_completed(future_to_idx):
                i = future_to_idx[future]
                try:
                    results[i] = future.result()
                except Exception as exc:
                    results[i] = None
                    tqdm.write(f"Error at index {i}: {exc}")
                pbar.update(1)

    # 写入文件
    with open(output_path, 'w', encoding='utf-8') as f:
        for item in results:
            if item is not None:
                f.write(item + ',\n')

if __name__ == "__main__":
    main()

