# tables包走不通，换h5py试试
# 解析单个小文件没问题
# AI改的，转换单个大h5文件
# 改废了，用不了，转出来结果有问题

import h5py
import json
import os
import numpy as np

def h5_to_json(h5_path, json_path):
    """
    将单个.h5文件转换为JSON格式，每一项为独立对象，包含完整属性
    """
    def convert_h5_object(obj):
        if isinstance(obj, h5py.Dataset):
            data = obj[()]
            if data.dtype.kind in ['i', 'f', 'S', 'U']:
                return data.tolist() if data.ndim > 0 else data.item()
            elif data.dtype.kind == 'V':
                return [
                    {name: item[i] for i, name in enumerate(obj.dtype.names)}
                    for item in data
                ]
            else:
                return "UNSUPPORTED_DATA_TYPE"
        elif isinstance(obj, h5py.Group):
            return {k: convert_h5_object(obj[k]) for k in obj}
        else:
            return str(obj)

    def numpy_type_converter(obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return str(obj)

    def expand_records(data):
        """
        将含有列表（如 songs）的对象拆分为多条，每条只包含一项 songs，且包含完整属性
        """
        # 找到所有需要展开的列表字段（只展开最内层的列表）
        def find_list_keys(d):
            keys = []
            for k, v in d.items():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    keys.append(k)
            return keys

        # 只支持一级展开（如 musicbrainz.songs）
        if isinstance(data, dict):
            list_keys = find_list_keys(data)
            if not list_keys:
                return [data]
            key = list_keys[0]
            items = []
            for row in data[key]:
                new_obj = dict(data)
                new_obj[key] = [row]
                items.append(new_obj)
            return items
        elif isinstance(data, dict) and len(data) == 1:
            # 例如 {"musicbrainz": {...}}
            k = list(data.keys())[0]
            v = data[k]
            expanded = expand_records(v)
            return [{k: item} for item in expanded]
        else:
            return [data]

    try:
        with h5py.File(h5_path, 'r') as h5_file:
            json_data = convert_h5_object(h5_file)
            # 展开为多条独立对象
            expanded = expand_records(json_data)
            with open(json_path, 'w') as json_file:
                json.dump(expanded, json_file, ensure_ascii=False, default=numpy_type_converter, indent=2)
        print(f"成功转换: {h5_path} -> {json_path}")
    except Exception as e:
        print(f"转换失败 {h5_path}: {str(e)}")

# 使用示例
if __name__ == "__main__":
    h5_file_path = ".local\playground\merge.h5"  # 替换为实际文件路径
    json_output_path = os.path.splitext(h5_file_path)[0] + ".json"
    h5_to_json(h5_file_path, json_output_path)

