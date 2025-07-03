from sqlalchemy import Column, BigInteger, String, Text, Float
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

class CFRecommendResult(Base):
    __tablename__ = "cf_recommend_result"
    __table_args__ = {'schema': 'mir_ads'}

    user_id = Column(BigInteger, primary_key=True, index=True)       # 设置为主键之一
    track_id = Column(BigInteger, primary_key=True)                  # 设置为主键之一
    user_name = Column(String(255))
    track_name = Column(String(255))
    pred_score = Column(Float)
