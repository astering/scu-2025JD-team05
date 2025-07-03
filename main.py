# 请求相似歌曲
# 列出歌曲详情

from fastapi import Depends, FastAPI, HTTPException, Request
# 首先应该安装fastapi，sqlalchemy
from sqlalchemy import Boolean, Column, Integer, String, Double
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from fastapi.responses import JSONResponse

# 与mysql连接
# 格式为 'mysql+pymysql://账号名:密码@ip:port/数据库名'
SQLALCHEMY_DATABASE_URI:str = 'mysql+pymysql://hive:admin@node-master:3306/mir_ads'

# 生成一个SQLAlchemy引擎
engine = create_engine(SQLALCHEMY_DATABASE_URI,pool_pre_ping=True)
# 生成sessionlocal类，这个类的每一个实例都是一个数据库的会话
# 注意命名为SessionLocal，与sqlalchemy的session分隔开
SessionLocal = sessionmaker(autocommit=False,autoflush=False,bind=engine)
session = SessionLocal()

Base = declarative_base()
# Base是用来给模型类继承的，类似django中的models.Model

# 模型类，tablename指表名，如果数据库中没有这个表会自动创建，有表则会沿用
class song_similarity(Base):
    __tablename__ = "song_similarity"
    track_id = Column(String, primary_key=True)
    similar_track_id = Column(String, primary_key=True)
    # Column就类似django里的Table.objects
    # 里面放字段，字段必须在上面先导入
    title = Column(String)
    artist = Column(String)
    release = Column(String)
    song_hotttnesss = Column(Double)
    year = Column(Integer)
    similar_score = Column(Double)

Base.metadata.create_all(bind=engine)
# 此步也必不可少
app = FastAPI()
# 创建fastapi对象

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # 允许所有来源
    allow_credentials=True, # 允许携带凭据（如 Cookies）
    allow_methods=["*"], # 允许所有 HTTP 方法
    allow_headers=["*"], # 允许所有请求头
)

@app.get("/l/{track_id}")
async def search_track(track_id: str):
    session = SessionLocal()
    try:
        result = session.query(song_similarity).filter(song_similarity.track_id == track_id).first()
        if result:
            return {
                "track_id": result.track_id,
                "title": result.title,
                "artist": result.artist,
                "release": result.release,
                "year": result.year,
            }
        else:
            return {
                "track_id": "-",
                "title": "-",
                "artist": "-",
                "release": "-",
                "year": 0,
            }
    finally:
        session.close()

@app.get("/q/{track_id}")
async def search_similar_track(track_id: str):
    session = SessionLocal()
    try:
        print(track_id)
        # 1. 查找所有 track_id 匹配的行，获取 similar_track_id 和 similar_score
        sim_rows = session.query(song_similarity).filter(song_similarity.track_id == track_id).all()
        print(len(sim_rows))
        if not sim_rows:
            return {"msg": "没找到对应记录"}
        # 2. 收集所有 similar_track_id 及其分数
        sim_list = []
        for row in sim_rows:
            sim_list.append({
                "similar_track_id": row.similar_track_id,
                "similar_score": row.similar_score
            })
        # 3. 按分数排序，取前3
        # sim_list = sorted(sim_list, key=lambda x: x["similar_score"], reverse=True)[:3]
        # 能索引到的太少了，全部拿去查询，有一个算一个
        sim_list = sorted(sim_list, key=lambda x: x["similar_score"], reverse=True)
        print("<sim_list>")
        result = {"track_id": []}
        for sim in sim_list:
            # 4. 用 similar_track_id 查找对应行
            target = session.query(song_similarity).filter(song_similarity.track_id == sim["similar_track_id"]).first()
            if target:
                result["track_id"].append(target.track_id)
        print(result)
        return result
    finally:
        session.close()

@app.post("/search")
async def search_tracks(request: Request):
    """
    支持两种请求体：
    1. {"track_id":..., "title":..., "artist":..., "release":...}
    2. {"track_id_list": [...]}
    """
    session = SessionLocal()
    try:
        data = await request.json()
        # 批量查
        if "track_id_list" in data:
            ids = data["track_id_list"]
            if not ids:
                return []
            rows = session.query(song_similarity).filter(song_similarity.track_id.in_(ids)).all()
            # 按track_id去重
            seen = set()
            result = []
            for row in rows:
                if row.track_id not in seen:
                    seen.add(row.track_id)
                    result.append({
                        "track_id": row.track_id,
                        "title": row.title,
                        "artist": row.artist,
                        "release": row.release,
                        "year": row.year
                    })
            return result
        # 单条件查
        query = session.query(song_similarity)
        if data.get("track_id"):
            query = query.filter(song_similarity.track_id == data["track_id"])
        if data.get("title"):
            query = query.filter(song_similarity.title == data["title"])
        if data.get("artist"):
            query = query.filter(song_similarity.artist == data["artist"])
        if data.get("release"):
            query = query.filter(song_similarity.release == data["release"])
        rows = query.all()
        # 按track_id去重
        seen = set()
        result = []
        for row in rows:
            if row.track_id not in seen:
                seen.add(row.track_id)
                result.append({
                    "track_id": row.track_id,
                    "title": row.title,
                    "artist": row.artist,
                    "release": row.release,
                    "year": row.year
                })
        return result
    finally:
        session.close()

@app.get("/similar/{track_id}")
async def get_similar(track_id: str):
    session = SessionLocal()
    try:
        sim_rows = session.query(song_similarity).filter(song_similarity.track_id == track_id).all()
        if not sim_rows:
            return []
        sim_list = sorted(sim_rows, key=lambda x: x.similar_score if x.similar_score is not None else 0, reverse=True)
        result = []
        for sim in sim_list:
            result.append(sim.similar_track_id)
        return result
    finally:
        session.close()

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app,host="127.0.0.1",port=8000)
	# 让fastapi跑起来
