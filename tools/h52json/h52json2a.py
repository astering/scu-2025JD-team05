# ai优化，尝试加速，无效

import sys
import json
import numpy as np

MSD_WRAPPERS_BASE = "/msd_wrappers" # 不行就改成绝对路径
sys.path.append(MSD_WRAPPERS_BASE)
# 如果只用hdf5_getters这一个，直接把hdf5_getters.py复制到同路径下就行
import hdf5_getters

def write_json(h5_in, songidx_in):
    # get params
    h5 = h5_in
    songidx = songidx_in

    # get all getters
    getters = list(filter(lambda x: x[:4] == 'get_', list(hdf5_getters.__dict__.keys())))
    getters.remove("get_num_songs") # special case
    getters = np.sort(getters)

    data = {}

    # put them in json
    for getter in getters:
        try:
            res = getattr(hdf5_getters, getter)(h5, songidx)
        except AttributeError as e:
            continue
        # 优化：只对不能序列化的对象转字符串
        try:
            json.dumps(res)
            data[str(getter[4:])] = res
        except TypeError:
            data[str(getter[4:])] = str(res)
    return data

def main():
    if len(sys.argv) != 3:
        print("Usage: ods_loader.py <input_path> <output_path>", file=sys.stderr)
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    h5 = hdf5_getters.open_h5_file_read(input_path)
    numSongs = hdf5_getters.get_num_songs(h5)

    all_data = []
    for i in range(numSongs):
        data = write_json(h5, i)
        all_data.append(data)
        print(i/numSongs)

    h5.close()

    # 一次性写入文件
    with open(output_path, 'w', encoding='utf-8') as f:
        for data in all_data:
            f.write(json.dumps(data) + '\n')

if __name__ == "__main__":
    main()

