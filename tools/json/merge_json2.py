# 处理一个文件保存一次，实时写入，适合大文件

import os
import json

# 添加进度条
'''
from tqdm import tqdm

def merge_json_files(...):
    # 在收集文件时添加进度
    files = []
    for root, dirs, filenames in os.walk(...):
        for f in filenames:
            if f.endswith('.json'):
                files.append(...)
    
    with tqdm(total=len(files)) as pbar:
        for file in files:
            # 处理文件
            pbar.update(1)
'''

# 保留原始文件元数据
'''
import datetime

def get_file_metadata(path):
    stat = os.stat(path)
    return {
        "size_bytes": stat.st_size,
        "modified": datetime.datetime.fromtimestamp(stat.st_mtime).isoformat(),
        "permissions": oct(stat.st_mode)
    }
'''

# 暂时没用
# 在merge_json_files函数顶部添加：
allowed_extensions = {'.json', '.txt'}  # 可添加其他扩展名

def merge_json_files(input_dir, output_file):
    first = True
    try:
        with open(output_file, 'w', encoding='utf-8') as out_f:
            out_f.write('[\n')
            for root, dirs, files in os.walk(input_dir):
                for filename in files:
                    if filename.endswith('.json'):
                        file_path = os.path.join(root, filename)
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                # 处理一个文件保存一次，实时写入，适合大文件
                                # 使用json.load+dump，可以不提前线性化处理
                                data = json.load(f)
                                # if needed: data["_source"] = os.path.relpath(file_path, input_dir)
                                if not first:
                                    # out_f.write(',') # 单行
                                    out_f.write(',\n') # 合并的每一个文件换一行
                                else:
                                    first = False
                                json.dump(data, out_f, ensure_ascii=False)
                                print(f"已合并: {file_path} (相对路径: {os.path.relpath(file_path, input_dir)})")
                        except Exception as e:
                            print(f"警告: 处理文件 {filename} 时出错 - {str(e)}")
            out_f.write('\n]')
        print(f"\n成功合并文件到 {output_file}")
        return True
    except Exception as e:
        print(f"写入文件时出错: {str(e)}")
        return False

if __name__ == "__main__":
    BASE_DIR = '.local/'
    INPUT_DIRECTORY = BASE_DIR + "playground"
    # BASE_DIR = './'
    # INPUT_DIRECTORY = BASE_DIR + "MillionSongSubsetJson"
    OUTPUT_FILE = BASE_DIR + "millionsongsubset.json"
    
    if not os.path.exists(INPUT_DIRECTORY):
        print(f"错误：输入目录 {INPUT_DIRECTORY} 不存在")
        exit(1)
    
    success = merge_json_files(INPUT_DIRECTORY, OUTPUT_FILE)
    
    if success:
        print("递归合并完成！")
    else:
        print("合并过程中发生错误")

