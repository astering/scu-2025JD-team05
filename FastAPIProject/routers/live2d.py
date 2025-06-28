from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()

# 模拟的模型配置数据（可扩展）
class ModelInfo(BaseModel):
    name: str
    path: str

@router.get("/models", response_model=list[ModelInfo])
def get_models():
    return [
    ]