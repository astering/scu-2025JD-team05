# models/artist.py
from sqlalchemy import Column, BigInteger, String, Integer
from database import Base

class TopArtist(Base):
    __tablename__ = "top_artist"
    __table_args__ = {"schema": "mir_ads"}

    artist_id = Column(BigInteger, primary_key=True)
    artist_name = Column(String(255))
    total_playcount = Column(BigInteger)
