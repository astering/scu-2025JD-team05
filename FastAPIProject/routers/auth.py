# routers/auth.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from database import SessionLocal
from models.auth import DwUserCleaned

router = APIRouter()

# 登录请求与响应模型
class LoginRequest(BaseModel):
    username: str
    password: str

class LoginResponse(BaseModel):
    user_id: int
    username: str
    token: str

# 登录接口
@router.post("/login", response_model=LoginResponse)
def login(request: LoginRequest):
    db: Session = SessionLocal()
    try:
        # 查询数据库中是否存在匹配的 lastfm_username
        user = db.query(DwUserCleaned).filter(DwUserCleaned.lastfm_username == request.username).first()
        if not user:
            raise HTTPException(status_code=401, detail="用户名不存在")

        # 输出匹配到的用户信息
        print(f"登录匹配成功: user_id={user.user_id}, lastfm_username={user.lastfm_username}")

        # 检查密码是否正确（此处为硬编码的示例密码：123456）
        if request.password != "123456":
            raise HTTPException(status_code=401, detail="密码错误")

        # 登录成功，返回 token（此处为示例 token）
        return {
            "user_id": user.user_id,
            "username": user.lastfm_username,
            "token": f"fake-jwt-token-{user.user_id}"
        }

    finally:
        db.close()
