# 手动“加速”，没什么用，速度差不多
# 很蠢，别这么写

import sys
import json
import numpy as np

# MSD_WRAPPERS_BASE = "/msd_wrappers" # 不行就改成绝对路径
# sys.path.append(MSD_WRAPPERS_BASE)
# 如果只用hdf5_getters这一个，直接把hdf5_getters.py复制到同路径下就行
# import hdf5_getters

import tables

# def write_json(h5_in, songidx_in, file_in, getters_in):
#     # get params
#     h5 = h5_in
#     songidx = songidx_in
#     file = file_in
#     getters = getters_in

#     data = {}

#     # put them in json
#     for getter in getters:
#         try:
#             res = getattr(hdf5_getters, getter)(h5, songidx)
#         except AttributeError as e:
#             continue
#         data[str(getter[4:])] = str(res)
#         continue
#         if res.__class__.__name__ == 'ndarray':
#             print(getter[4:] + ": shape =", res.shape)
#         else:
#             print(getter[4:] + ":", res)

#     # 每处理一项就写入一次
#     file.write(json.dumps(data) + '\n')

def main():
    if len(sys.argv) != 3:
        print("Usage: ods_loader.py <input_path> <output_path>", file=sys.stderr)
        # sys.exit(-1)

    # input_path = sys.argv[1]
    input_path = "D:\coding\github\scu-2025JD-team05\.local\playground\msd_summary_file.h5"
    # output_path = sys.argv[2]
    output_path = "D:\coding\github\scu-2025JD-team05\.local\playground\msd_summary_file.json"

    # h5 = hdf5_getters.open_h5_file_read(input_path)
    h5 = tables.open_file(input_path, mode='r')
    # numSongs = hdf5_getters.get_num_songs(h5)
    numSongs = 1000000

    # get all getters
    # getters = list(filter(lambda x: x[:4] == 'get_', list(hdf5_getters.__dict__.keys())))
    # getters.remove("get_num_songs") # special case
    # getters = np.sort(getters)

    getters = ["get_analysis_sample_rate",
                "get_artist_7digitalid",
                "get_artist_familiarity",
                "get_artist_hotttnesss",
                "get_artist_id",
                "get_artist_latitude",
                "get_artist_location",
                "get_artist_longitude",
                "get_artist_mbid",
                "get_artist_name",
                "get_artist_playmeid",
                "get_audio_md5",
                "get_danceability",
                "get_duration",
                "get_end_of_fade_in",
                "get_energy",
                "get_key",
                "get_key_confidence",
                "get_loudness",
                "get_mode",
                "get_mode_confidence",
                "get_release",
                "get_release_7digitalid",
                "get_song_hotttnesss",
                "get_song_id",
                "get_start_of_fade_out",
                "get_tempo",
                "get_time_signature",
                "get_time_signature_confidence",
                "get_title",
                "get_track_7digitalid",
                "get_track_id",
                "get_year"
                ]

    # 清空输出文件
    open(output_path, 'w').close()
    f = open(output_path, 'a', encoding='utf-8')

    for songidx in range(numSongs):
        # write_json(h5, i, f, getters)

        data = {}

        # # data["analysis_sample_rate"] = str(h5.root.analysis.songs.cols.analysis_sample_rate[songidx])
        # # data["artist_7digitalid"] = str(h5.root.metadata.songs.cols.artist_7digitalid[songidx])
        # data["artist_familiarity"] = str(h5.root.metadata.songs.cols.artist_familiarity[songidx])
        # data["artist_hotttnesss"] = str(h5.root.metadata.songs.cols.artist_hotttnesss[songidx])
        # data["artist_id"] = str(h5.root.metadata.songs.cols.artist_id[songidx])
        # # data["artist_latitude"] = str(h5.root.metadata.songs.cols.artist_latitude[songidx])
        # # data["artist_location"] = str(h5.root.metadata.songs.cols.artist_location[songidx])
        # # data["artist_longitude"] = str(h5.root.metadata.songs.cols.artist_longitude[songidx])
        # # data["artist_mbid"] = str(h5.root.metadata.songs.cols.artist_mbid[songidx])
        # data["artist_name"] = str(h5.root.metadata.songs.cols.artist_name[songidx])
        # # data["artist_playmeid"] = str(h5.root.metadata.songs.cols.artist_playmeid[songidx])
        # # data["audio_md5"] = str(h5.root.analysis.songs.cols.audio_md5[songidx])
        # # data["danceability"] = str(h5.root.analysis.songs.cols.danceability[songidx])
        data["duration"] = str(h5.root.analysis.songs.cols.duration[songidx])
        data["end_of_fade_in"] = str(h5.root.analysis.songs.cols.end_of_fade_in[songidx])
        # # data["energy"] = str(h5.root.analysis.songs.cols.energy[songidx])
        data["key"] = str(h5.root.analysis.songs.cols.key[songidx])
        # # data["key_confidence"] = str(h5.root.analysis.songs.cols.key_confidence[songidx])
        data["loudness"] = str(h5.root.analysis.songs.cols.loudness[songidx])
        # # data["mode"] = str(h5.root.analysis.songs.cols.mode[songidx])
        # # data["mode_confidence"] = str(h5.root.analysis.songs.cols.mode_confidence[songidx])
        # data["release"] = str(h5.root.metadata.songs.cols.release[songidx])
        # # data["release_7digitalid"] = str(h5.root.metadata.songs.cols.release_7digitalid[songidx])
        data["song_hotttnesss"] = str(h5.root.metadata.songs.cols.song_hotttnesss[songidx])
        # data["song_id"] = str(h5.root.metadata.songs.cols.song_id[songidx])
        # # data["start_of_fade_out"] = str(h5.root.analysis.songs.cols.start_of_fade_out[songidx])
        data["tempo"] = str(h5.root.analysis.songs.cols.tempo[songidx])
        # # data["time_signature"] = str(h5.root.analysis.songs.cols.time_signature[songidx])
        # # data["time_signature_confidence"] = str(h5.root.analysis.songs.cols.time_signature_confidence[songidx])
        # data["title"] = str(h5.root.metadata.songs.cols.title[songidx])
        # # data["track_7digitalid"] = str(h5.root.metadata.songs.cols.track_7digitalid[songidx])
        data["track_id"] = str(h5.root.analysis.songs.cols.track_id[songidx])
        # data["year"] = str(h5.root.musicbrainz.songs.cols.year[songidx])

        '''
        # put them in json
        for getter in getters:
            # try:
            #     res = getattr(hdf5_getters, getter)(h5, songidx)
            # except AttributeError as e:
            #     continue

            res = getattr(hdf5_getters, getter)(h5, songidx)
            print(getter, type(res))

            # data[str(getter[4:])] = res
            data[getter[4:]] = str(res)
            continue
            if res.__class__.__name__ == 'ndarray':
                print(getter[4:] + ": shape =", res.shape)
            else:
                print(getter[4:] + ":", res)
        '''

        # 每处理一项就写入一次
        f.write(json.dumps(data) + ',\n')

        print(songidx)
        # print(songidx/numSongs)

    h5.close()
    f.close()

if __name__ == "__main__":
    main()

