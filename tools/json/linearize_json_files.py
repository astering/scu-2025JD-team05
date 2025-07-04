# ai生成的
# 其实直接json.load再dump就行了，indent=0或者不写，没必要dumps再f.write

import os
import json

def linearize_json_files(root_dir):
    """
    递归处理指定目录下的所有JSON文件，将其内容线性化为单行格式
    """
    for foldername, subfolders, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.lower().endswith('.json'):
                file_path = os.path.join(foldername, filename)
                try:
                    # 读取并解析JSON文件
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # 重新序列化为无缩进的单行格式
                    linearized_content = json.dumps(data, separators=(',', ':'))
                    
                    # 写回文件（覆盖原文件）
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(linearized_content)
                    
                    print(f"已处理: {file_path}")
                    
                except Exception as e:
                    print(f"处理 {file_path} 时出错: {str(e)}")

if __name__ == "__main__":
    # 使用示例（替换为你的目标目录）
    target_directory = "./MillionSongSubsetJson"
    linearize_json_files(target_directory)

