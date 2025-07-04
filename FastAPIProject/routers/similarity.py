from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import SessionLocal
from models.similarity import FullTrack, SongSimilarity

router = APIRouter()

# 依赖注入获取数据库会话
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/search")
def search_similar_songs(q: str = Query(..., min_length=1), db: Session = Depends(get_db)):
    song_title = q.strip()
    if not song_title:
        return {"results": []}

    # 先尝试精确匹配
    res = db.query(FullTrack).filter(FullTrack.track_name == song_title).first()
    if not res:
        # 模糊匹配
        res = db.query(FullTrack).filter(FullTrack.track_name.like(f"%{song_title}%")).first()
    if not res:
        return {"results": []}

    track_id = res.track_id
    matched_name = res.track_name
    matched_artist = res.artist_name

    # 用ORM或原生SQL查相似歌曲
    similars = db.query(SongSimilarity).filter(SongSimilarity.track_id == track_id)\
               .order_by(SongSimilarity.similar_score.desc()).limit(100).all()

    results = [{
        "track_name": matched_name,
        "artist_name": matched_artist,
        "similar_score": 1.0
    }]

    seen_keys = set()
    score_seen = set()

    for sim in similars:
        if sim.similar_track_id == track_id:
            continue
        key = (sim.similar_track_id, sim.similar_score)
        if key in seen_keys or sim.similar_score in score_seen:
            continue
        seen_keys.add(key)
        score_seen.add(sim.similar_score)

        # 获取歌曲信息
        track = db.query(FullTrack).filter(FullTrack.track_id == sim.similar_track_id).first()
        if not track:
            continue
        results.append({
            "track_name": track.track_name,
            "artist_name": track.artist_name,
            "similar_score": sim.similar_score
        })

        if len(results) >= 10:
            break

    return {"results": results}
