import os
from sqlalchemy import create_engine, Column, Integer, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime

# 💡 Debug提示: 将 echo=False 改为 True 可以查看生成的 SQL 语句
DATABASE_URL = os.getenv('DATABASE_URL', 'mysql+pymysql://xf:xf666@192.168.0.141:3306/spiderweb')

engine = create_engine(
    DATABASE_URL,
    pool_size=20,           # 核心连接数：保持常驻的连接数量
    max_overflow=40,        # 突发连接数：高并发时允许临时创建的连接
    pool_recycle=3600,      # 连接回收：防止 MySQL 8小时断开问题
    pool_timeout=30,
    echo=False              # 生产环境建议关闭 SQL 日志
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class BaseModel(Base):
    """所有模型的基类，包含通用审计字段"""
    __abstract__ = True
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)

def init_db():
    """初始化数据库表结构"""
    Base.metadata.create_all(bind=engine)