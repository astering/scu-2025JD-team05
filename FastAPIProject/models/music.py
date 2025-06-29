from sqlalchemy import Column, BigInteger, String, Text
from database import Base

class TopTrack(Base):
    __tablename__  = "top_track"

    track_id = Column(BigInteger, primary_key=True, index=True)
    duration = Column(BigInteger)
    playcount = Column(BigInteger)
    track_mbid = Column(Text)
    title = Column(Text)
    artist_id = Column(BigInteger)
    artist_name = Column(Text)
    album_id = Column(BigInteger)
