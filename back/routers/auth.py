# backend/auth.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()

# 模拟数据库用户数据
fake_users_db = {
    "admin": {
        "username": "admin",
        "password": "123456",
        "token": "fake-jwt-token-admin"
    },
    "user": {
        "username": "user",
        "password": "password",
        "token": "fake-jwt-token-user"
    }
}

class LoginRequest(BaseModel):
    username: str
    password: str

class LoginResponse(BaseModel):
    username: str
    token: str

@router.post("/login", response_model=LoginResponse)
def login(request: LoginRequest):
    print(f"🌐 后端收到登录请求: 用户名={request.username}, 密码={request.password}")
    user = fake_users_db.get(request.username)
    if not user or user["password"] != request.password:
        raise HTTPException(status_code=401, detail="用户名或密码错误")
    return {"username": user["username"], "token": user["token"]}
