# 换了个AI生成，终于行了
# 速度很快，一开始会初始化，没进度，之后很快
# 关键区别是，要先提取所有字段（表的每一列），再按每个项（表的每一行）组装
# 之前是按行处理，提取一行的各个字段，组装完成再处理下一行
# 之所以这么处理，是为了复用msd官方代码的wrapper
# 读一条适合这么做，处理全部就不行了
# 估计和h5存储方式有关
# 或者是因为大量减少了文件io，因为只用读h5文件提取一次，组装是在内存中完成的

import json
import h5py

def main():
    input_path = ".local\\playground\\msd_summary_file.h5"
    output_path = ".local\\playground\\msd_summary_file.json"

    h5 = h5py.File(input_path, 'r')
    numSongs = 5
    numSongs = h5['metadata/songs'].shape[0]
    print(numSongs)

    # 批量读取所有需要的列
    analysis = h5['analysis/songs']
    metadata = h5['metadata/songs']
    durations = analysis['duration'][:numSongs]
    end_of_fade_ins = analysis['end_of_fade_in'][:numSongs]
    keys = analysis['key'][:numSongs]
    loudnesses = analysis['loudness'][:numSongs]
    tempos = analysis['tempo'][:numSongs]
    track_ids = analysis['track_id'][:numSongs]
    song_hotttnesss = metadata['song_hotttnesss'][:numSongs]

    # 构建所有数据的列表
    all_data = []
    for songidx in range(numSongs):
        data = {
            "duration": str(durations[songidx]),
            "end_of_fade_in": str(end_of_fade_ins[songidx]),
            "key": str(keys[songidx]),
            "loudness": str(loudnesses[songidx]),
            "song_hotttnesss": str(song_hotttnesss[songidx]),
            "tempo": str(tempos[songidx]),
            "track_id": str(track_ids[songidx])
        }
        all_data.append(data)
        print(songidx, songidx/numSongs)

    h5.close()

    # 一次性写入所有数据为JSON数组
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()

