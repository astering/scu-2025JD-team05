from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from database import SessionLocal
from models.music import TopTrack

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/recommend")
def get_recommend():
    return {
        "tracks": [
            {"id": 1, "title": "歌名1", "artist": "歌手1"},
            {"id": 2, "title": "歌名2", "artist": "歌手2"},
        ]
    }

@router.get("/rank")
def get_top_tracks(db: Session = Depends(get_db)):
    tracks = db.query(TopTrack).order_by(TopTrack.playcount.desc()).limit(10).all()
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
