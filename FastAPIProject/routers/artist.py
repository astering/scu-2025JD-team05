from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from sqlalchemy.sql import over
from database import SessionLocal
from models.artist import TopArtist

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/top-artists")
def get_top_artists(limit: int = Query(10, le=100), db: Session = Depends(get_db)):
    ranked = db.query(
        TopArtist.artist_id,
        TopArtist.artist_name,
        TopArtist.total_playcount,
        func.row_number().over(
            partition_by=TopArtist.artist_id,
            order_by=TopArtist.artist_name.asc()
        ).label("rank")
    ).subquery()

    results = (
        db.query(
            ranked.c.artist_id,
            ranked.c.artist_name,
            ranked.c.total_playcount
        )
        .filter(ranked.c.rank == 1)
        .order_by(desc(ranked.c.total_playcount))
        .limit(limit)
        .all()
    )

    data = [
        {
            "id": r.artist_id,
            "name": r.artist_name,
            "playcount": int(r.total_playcount)
        }
        for r in results
    ]

    print("Top Artists Data:", data)
    return data
