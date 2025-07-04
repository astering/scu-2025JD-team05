from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Request
from pydantic import BaseModel
from starlette.staticfiles import StaticFiles

from routers import music, auth,  home, artist
from chat_helper.ai_chat import ai_chat

app = FastAPI(title="大数据音乐推荐系统后端")

# 静态文件挂载
app.mount("/assets", StaticFiles(directory="assets"), name="assets")

# 跨域设置：允许前端 http://localhost:5173 访问
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 模块路由
app.include_router(auth.router, prefix="/api/auth", tags=["Auth"])
app.include_router(music.router, prefix="/api/music", tags=["Music"])
app.include_router(home.router, prefix="/api/home", tags=["Home"])
app.include_router(artist.router, prefix="/api/artist", tags=["Artist"])

# 聊天小助手的数据格式
class ChatRequest(BaseModel):
    message: str

# 聊天接口
@app.post("/api/chat")
async def chat(chat_request: ChatRequest):
    response = ai_chat(chat_request.message)
    return {"response": response}

# 根路径测试
@app.get("/")
def read_root():
    return {"msg": "后端服务运行中"}



