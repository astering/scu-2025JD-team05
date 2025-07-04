# 源自h52json4.py，把字段补全了

import json
import h5py

def main():
    input_path = ".local\\playground\\msd_summary_file.h5"
    output_path = ".local\\playground\\msd_summary_file.json"

    h5 = h5py.File(input_path, 'r')
    numSongs = h5['metadata/songs'].shape[0]
    print(numSongs)
    # numSongs = 5

    # 批量读取所有需要的列
    analysis = h5['analysis/songs']
    print('analysis')
    metadata = h5['metadata/songs']
    print('metadata')
    musicbrainz = h5['musicbrainz/songs']
    print('musicbrainz')

    analysis_sample_rates = analysis['analysis_sample_rate'][:numSongs]
    artist_7digitalids = metadata['artist_7digitalid'][:numSongs]
    artist_familiarities = metadata['artist_familiarity'][:numSongs]
    artist_hotttnessss = metadata['artist_hotttnesss'][:numSongs]
    artist_ids = metadata['artist_id'][:numSongs]
    artist_latitudes = metadata['artist_latitude'][:numSongs]
    artist_locations = metadata['artist_location'][:numSongs]
    artist_longitudes = metadata['artist_longitude'][:numSongs]
    artist_mbids = metadata['artist_mbid'][:numSongs]
    artist_names = metadata['artist_name'][:numSongs]
    artist_playmeids = metadata['artist_playmeid'][:numSongs]
    audio_md5s = analysis['audio_md5'][:numSongs]
    print('1/3')
    danceabilities = analysis['danceability'][:numSongs]
    durations = analysis['duration'][:numSongs]
    end_of_fade_ins = analysis['end_of_fade_in'][:numSongs]
    energies = analysis['energy'][:numSongs]
    keys = analysis['key'][:numSongs]
    key_confidences = analysis['key_confidence'][:numSongs]
    loudnesses = analysis['loudness'][:numSongs]
    modes = analysis['mode'][:numSongs]
    mode_confidences = analysis['mode_confidence'][:numSongs]
    releases = metadata['release'][:numSongs]
    release_7digitalids = metadata['release_7digitalid'][:numSongs]
    print('2/3')
    song_hotttnesss = metadata['song_hotttnesss'][:numSongs]
    song_ids = metadata['song_id'][:numSongs]
    start_of_fade_outs = analysis['start_of_fade_out'][:numSongs]
    tempos = analysis['tempo'][:numSongs]
    time_signatures = analysis['time_signature'][:numSongs]
    time_signature_confidences = analysis['time_signature_confidence'][:numSongs]
    titles = metadata['title'][:numSongs]
    track_7digitalids = metadata['track_7digitalid'][:numSongs]
    track_ids = analysis['track_id'][:numSongs]
    years = musicbrainz['year'][:numSongs]
    print('3/3')

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('[\n')
        for songidx in range(numSongs):
            data = {
                "analysis_sample_rate": str(analysis_sample_rates[songidx]),
                "artist_7digitalid": str(artist_7digitalids[songidx]),
                "artist_familiarity": str(artist_familiarities[songidx]),
                "artist_hotttnesss": str(artist_hotttnessss[songidx]),
                "artist_id": str(artist_ids[songidx]),
                "artist_latitude": str(artist_latitudes[songidx]),
                "artist_location": str(artist_locations[songidx]),
                "artist_longitude": str(artist_longitudes[songidx]),
                "artist_mbid": str(artist_mbids[songidx]),
                "artist_name": str(artist_names[songidx]),
                "artist_playmeid": str(artist_playmeids[songidx]),
                "audio_md5": str(audio_md5s[songidx]),
                "danceability": str(danceabilities[songidx]),
                "duration": str(durations[songidx]),
                "end_of_fade_in": str(end_of_fade_ins[songidx]),
                "energy": str(energies[songidx]),
                "key": str(keys[songidx]),
                "key_confidence": str(key_confidences[songidx]),
                "loudness": str(loudnesses[songidx]),
                "mode": str(modes[songidx]),
                "mode_confidence": str(mode_confidences[songidx]),
                "release": str(releases[songidx]),
                "release_7digitalid": str(release_7digitalids[songidx]),
                "song_hotttnesss": str(song_hotttnesss[songidx]),
                "song_id": str(song_ids[songidx]),
                "start_of_fade_out": str(start_of_fade_outs[songidx]),
                "tempo": str(tempos[songidx]),
                "time_signature": str(time_signatures[songidx]),
                "time_signature_confidence": str(time_signature_confidences[songidx]),
                "title": str(titles[songidx]),
                "track_7digitalid": str(track_7digitalids[songidx]),
                "track_id": str(track_ids[songidx]),
                "year": str(years[songidx])
            }
            json.dump(data, f, ensure_ascii=False)
            f.write(',\n')
            print(songidx, songidx/numSongs)
        f.write(']')

    h5.close()

if __name__ == "__main__":
    main()

