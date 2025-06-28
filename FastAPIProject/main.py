from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles

from routers import music, auth, live2d, home

app = FastAPI(title="大数据音乐推荐系统后端")
app.mount("/assets", StaticFiles(directory="assets"), name="assets")

# 允许前端跨域访问
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # 可改为前端地址如http://localhost:5173
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 路由注册
app.include_router(auth.router, prefix="/api/auth", tags=["Auth"])
app.include_router(music.router, prefix="/api/music", tags=["Music"])
app.include_router(home.router, prefix="/api/home", tags=["Home"])
app.include_router(live2d.router, prefix="/api/live2d", tags=["Live2D"])


@app.get("/")
def read_root():
    return {"msg": "后端服务运行中"}




