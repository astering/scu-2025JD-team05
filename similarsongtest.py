import pandas as pd
from sqlalchemy import create_engine, text

# 数据库连接参数
db_user = 'hive'
db_password = 'admin'
db_host = '192.168.101.235'
db_name = 'mir_ads'
engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}')

def get_similar_songs_by_title(song_title: str,
                               unique_table: str = "full_track",
                               sim_table: str = "song_similarity",
                               max_results: int = 10):
    song_title = song_title.strip()
    with engine.connect() as conn:
        # 1. 查 track_id
        query_track = text(f"""
            SELECT track_id, track_name FROM {unique_table}
            WHERE track_name = :title
            LIMIT 1
        """)
        res = conn.execute(query_track, {"title": song_title}).fetchone()

        if not res:
            print(f"❌ 未找到歌曲《{song_title}》。")
            return None
        track_id, matched_title = res

        # 2. 查相似歌曲列表（多查以便去重后够用）
        query_similar = text(f"""
            SELECT similar_track_id, similar_score
            FROM {sim_table}
            WHERE track_id = :track_id
            ORDER BY similar_score DESC
            LIMIT {max_results * 5}
        """)
        similars = conn.execute(query_similar, {"track_id": track_id}).fetchall()

        if not similars:
            print(f"⚠️ 未找到《{matched_title}》的相似歌曲。")
            return None

        # 3. 查询详细信息并去重
        seen = set()
        songs = []
        output_count = 0

        print(f"\n🎶 与《{matched_title}》相似度最高的十首歌：\n")

        for similar_track_id, score in similars:
            query_info = text(f"""
                SELECT track_name, artist_name FROM {unique_table}
                WHERE track_id = :sid
                LIMIT 1
            """)
            info = conn.execute(query_info, {"sid": similar_track_id}).fetchone()
            if info:
                track_name, artist_name = info
                key = (track_name, artist_name)
                if key in seen:
                    continue
                seen.add(key)
                songs.append((track_name, artist_name, score))
                print(f"{output_count + 1}. {track_name} - {artist_name} 相似度：{score:.4f}")
                output_count += 1
                if output_count >= max_results:
                    break

        if output_count == 0:
            print("⚠️ 找不到任何对应的相似歌曲详细信息。")
            return None

        # 4. 让娘娘选择下一首
        while True:
            choice = input(f"\n👑 娘娘是否想继续探索其中一首歌？请输入 1~{output_count} 的编号（或输入 n 退出）：").strip()
            if choice.lower() == 'n':
                print("🌸 感谢娘娘赏阅，推荐完毕。")
                return None
            if choice.isdigit():
                idx = int(choice)
                if 1 <= idx <= output_count:
                    return songs[idx - 1][0]  # 返回下一首歌名
                else:
                    print("⚠️ 编号不在范围内，请重新输入。")
            else:
                print("⚠️ 输入有误，请重新输入。")

if __name__ == "__main__":
    current_song = input("请输入歌曲名称：").strip()
    while current_song:
        next_song = get_similar_songs_by_title(current_song)
        if next_song is None:
            break
        current_song = next_song
