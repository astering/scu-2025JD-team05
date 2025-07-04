# 手动“加速”
# h52json2i.py 去掉冗余注释的版本

import json
import tables

def main():
    input_path = ".local\playground\msd_summary_file.h5"
    output_path = ".local\playground\msd_summary_file.json"

    h5 = tables.open_file(input_path, mode='r')
    # numSongs = h5.root.metadata.songs.nrows
    numSongs = 1000000

    # 清空输出文件
    open(output_path, 'w').close()
    f = open(output_path, 'a', encoding='utf-8')

    for songidx in range(numSongs):
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

        # 每处理一项就写入一次
        f.write(json.dumps(data) + ',\n')

        print(songidx)
        # print(songidx/numSongs)

    h5.close()
    f.close()

if __name__ == "__main__":
    main()

