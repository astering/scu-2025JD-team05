# models/auth.py
from sqlalchemy import Column, BigInteger, String
from database import Base

class DwUserCleaned(Base):
    __tablename__ = "dw_users_cleaned"
    __table_args__ = {'schema': 'mir_ads'}  # 指定 schema（如果你在数据库中有设置）

    user_id = Column(BigInteger, primary_key=True, index=True)
    lastfm_username = Column(String(255), index=True)
    gender = Column(String(10))
    age = Column(BigInteger)
    country = Column(String(50))
    playcount = Column(BigInteger)
    playlists = Column(BigInteger)
    subscribertype = Column(String(50))
    create_time = Column(BigInteger)
