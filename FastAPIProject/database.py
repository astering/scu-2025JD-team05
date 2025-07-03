
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# MySQL连接配置
MYSQL_USER = "hive"
MYSQL_PASSWORD = "admin"
MYSQL_HOST = "192.168.101.235"
MYSQL_DB = "mir_ads"

# 拼接SQLAlchemy连接URI（使用pymysql驱动）
SQLALCHEMY_DATABASE_URL = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:3306/{MYSQL_DB}"

# 创建引擎（echo=True可开启SQL日志）
engine = create_engine(SQLALCHEMY_DATABASE_URL, echo=False)

# 创建session工厂
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 创建ORM模型基类
Base = declarative_base()
