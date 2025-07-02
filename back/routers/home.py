from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()

class Section(BaseModel):
    title: str
    description: str

@router.get("/home_sections", response_model=list[Section])
def get_home_sections():
    return [
        {"title": "推荐歌曲", "description": "这些是根据你的喜好推荐的热门歌曲。"},
        {"title": "热门专辑", "description": "今日最受欢迎的专辑精选。"},
    ]
