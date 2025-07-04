# 处理大量单个h5文件

import h5py
import json
import os
import glob
import numpy as np

def h5_to_json(h5_path, json_path):
    """
    将单个.h5文件转换为JSON格式
    参数：
        h5_path: 输入.h5文件路径
        json_path: 输出JSON文件路径
    """
    def convert_h5_object(obj, parent_key=""):
        """递归转换h5对象为字典"""
        data_dict = {}
        if isinstance(obj, h5py.Dataset):
            # 处理数据集
            data = obj[()]
            if data.dtype.kind in ['i', 'f', 'S', 'U']:  # 基础类型
                data_dict[parent_key] = data.tolist() if data.ndim > 0 else data.item()
            elif data.dtype.kind == 'V':  # 复合类型
                # 示例：处理songs数据集
                if 'songs' in parent_key:
                    data_dict[parent_key] = [
                        {name: item[i] for i, name in enumerate(obj.dtype.names)}
                        for item in data
                    ]
            else:
                # 处理特殊类型（如复合类型）
                data_dict[parent_key] = "UNSUPPORTED_DATA_TYPE"
        elif isinstance(obj, h5py.Group):
            # 处理组
            for key in obj:
                sub_obj = obj[key]
                data_dict.update(convert_h5_object(sub_obj, parent_key + "_" + key if parent_key else key))
        return data_dict

    def numpy_type_converter(obj):
        """将 numpy 类型转换为 Python 原生类型"""
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return str(obj)

    try:
        with h5py.File(h5_path, 'r') as h5_file:
            # 递归转换整个h5文件结构
            json_data = convert_h5_object(h5_file)
            
            # 写入JSON文件，处理 numpy 类型
            with open(json_path, 'w') as json_file:
                json.dump(json_data, json_file, indent=2, ensure_ascii=False, default=numpy_type_converter)
                
        print(f"成功转换: {h5_path} -> {json_path}")
        
    except Exception as e:
        print(f"转换失败 {h5_path}: {str(e)}")

# 使用示例
if __name__ == "__main__":
    # h5_file_path = "TRAAAEF128F4273421.h5"  # 替换为实际文件路径
    # json_output_path = os.path.splitext(h5_file_path)[0] + ".json"
    # h5_to_json(h5_file_path, json_output_path)

    # import glob
    # 转换目录下所有.h5文件
    input_dir = "millionsongsubset"
    output_dir = "millionsongsubset_json"

    for h5_path in glob.glob(os.path.join(input_dir, "**", "*.h5"), recursive=True):
        rel_path = os.path.relpath(h5_path, input_dir)
        json_path = os.path.splitext(os.path.join(output_dir, rel_path))[0] + ".json"
        os.makedirs(os.path.dirname(json_path), exist_ok=True)
        h5_to_json(h5_path, json_path)

