from collections import defaultdict
from fastapi import Query

from fastapi import APIRouter, Depends
from sqlalchemy import func, desc
from sqlalchemy.orm import Session, aliased

from database import SessionLocal
from models.music import TopTrack, CFRecommendResult

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/recommend")
def get_recommend(user_id: int = Query(...), db: Session = Depends(get_db)):
    print(f"收到推荐请求 user_id={user_id}")

    results = (
        db.query(CFRecommendResult)
        .filter(CFRecommendResult.user_id == user_id)
        .order_by(CFRecommendResult.pred_score.desc())
        .all()
    )

    print(f"推荐结果数量：{len(results)}")

    if not results:
        print(f"用户 {user_id} 没有推荐记录")

    # 聚合相同 track_name，保留最高分的一个
    track_dict = defaultdict(list)
    for r in results:
        track_dict[r.track_name].append((r.track_id, r.pred_score))

    tracks = []
    for title, track_list in track_dict.items():
        best_track = max(track_list, key=lambda x: x[1])
        tracks.append({
            "id": best_track[0],
            "title": title,
            "score": round(best_track[1], 4)
        })

    tracks.sort(key=lambda x: x["score"], reverse=True)
    return {"tracks": tracks}

TOP_TRACKS_LIMIT = 10

def query_top_tracks(db: Session):
    # 子查询：为每个 track_id 分配 row_number，按 playcount 降序排序
    ranked_subquery = (
        db.query(
            TopTrack,
            func.row_number().over(
                partition_by=TopTrack.track_id,
                order_by=desc(TopTrack.playcount)
            ).label("row_num")
        ).subquery()
    )

    # 创建别名用于主查询
    ranked_alias = aliased(TopTrack, ranked_subquery)

    # 主查询：只取 row_num == 1 的记录（每个 track_id 一条），再按播放量降序排列
    query = (
        db.query(ranked_alias)
        .filter(ranked_subquery.c.row_num == 1)
        .order_by(ranked_alias.playcount.desc())
        .limit(TOP_TRACKS_LIMIT)
    )

    return query.all()

@router.get("/rank")
def get_top_tracks(db: Session = Depends(get_db)):
    tracks = query_top_tracks(db)
    print("后端查询结果:", tracks)
    return [
        {
            "id": track.track_id,
            "title": track.title,
            "artist": track.artist_name,
            "playcount": track.playcount
        }
        for track in tracks
    ]

@router.get("/api/playlist")
def get_playlist():
    return [
        {"id": 1, "title": "早安歌单", "num_tracks": 12},
        {"id": 2, "title": "夜晚放松", "num_tracks": 9},
    ]

@router.get("/api/artists")
def get_artists():
    return [
        {"id": 1, "name": "周杰伦", "num_tracks": 45},
        {"id": 2, "name": "Taylor Swift", "num_tracks": 120},
        {"id": 3, "name": "Ed Sheeran", "num_tracks": 90}
    ]
