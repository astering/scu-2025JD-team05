from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()

@router.get("/assets/api/model_list.json")
def get_model_list():
    # 返回模型文件夹名的列表（可根据你 assets/api 模型目录动态生成）
    return JSONResponse(content=["shizuku", "wanko", "z16"])
