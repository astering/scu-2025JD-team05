from sqlalchemy import Column, String, Text, Float, Integer
from database import Base

class SongSimilarity(Base):
    __tablename__ = "song_similarity"
    __table_args__ = {'schema': 'mir_ads'}

    track_id = Column(Text, primary_key=True, index=True)
    title = Column(Text)
    artist = Column(Text)
    release = Column(Text)
    song_hotttnesss = Column(Float)
    year = Column(Integer)
    similar_track_id = Column(Text, primary_key=True, index=True)
    similar_score = Column(Float)


class FullTrack(Base):
    __tablename__ = "full_track"
    __table_args__ = {'schema': 'mir_ads'}

    track_id = Column(Text, primary_key=True, index=True)
    track_name = Column(String(255))
    artist_name = Column(String(255))
