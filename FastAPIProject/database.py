
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# 替换为你实际的 MySQL 连接配置
MYSQL_USER = "hive"
MYSQL_PASSWORD = "admin"
MYSQL_HOST = "192.168.101.235"
MYSQL_DB = "mir_ads"

# 拼接 SQLAlchemy 连接 URI（使用 pymysql 驱动）
SQLALCHEMY_DATABASE_URL = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:3306/{MYSQL_DB}"

# 创建引擎（echo=True 可开启 SQL 日志）
engine = create_engine(SQLALCHEMY_DATABASE_URL, echo=False)

# 创建 session 工厂
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 创建 ORM 模型基类
Base = declarative_base()
