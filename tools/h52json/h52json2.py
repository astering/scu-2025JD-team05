# 处理单个大h5文件
# 能用，但是很慢，1秒转换1个，一共100万项，需要11天

import sys
import json
import numpy as np

MSD_WRAPPERS_BASE = "/msd_wrappers" # 不行就改成绝对路径
sys.path.append(MSD_WRAPPERS_BASE)
# 如果只用hdf5_getters这一个，直接把hdf5_getters.py复制到同路径下就行
import hdf5_getters

def write_json(h5_in, songidx_in, file_in):
    # get params
    h5 = h5_in
    songidx = songidx_in
    file = file_in

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
        data[str(getter[4:])] = str(res)
        continue
        if res.__class__.__name__ == 'ndarray':
            print(getter[4:] + ": shape =", res.shape)
        else:
            print(getter[4:] + ":", res)

    # 每处理一项就写入一次
    file.write(json.dumps(data) + ',\n')

def main():
    if len(sys.argv) != 3:
        print("Usage: ods_loader.py <input_path> <output_path>", file=sys.stderr)
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    h5 = hdf5_getters.open_h5_file_read(input_path)
    numSongs = hdf5_getters.get_num_songs(h5)

    # 清空输出文件
    open(output_path, 'w').close()
    f = open(output_path, 'a', encoding='utf-8')

    for i in range(numSongs):
        write_json(h5, i, f)
        print(i/numSongs)

    h5.close()
    f.close()

if __name__ == "__main__":
    main()

