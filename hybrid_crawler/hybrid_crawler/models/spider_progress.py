from sqlalchemy import Column, String, Integer, DateTime, JSON, Text, Float
from sqlalchemy.sql import func
from . import BaseModel

class SpiderProgress(BaseModel):
    """
    爬虫实时进度表 (Snapshot)
    用于 Dashboard 实时监控，每个爬虫实例只有一行记录 (Upsert)
    """
    __tablename__ = 'spider_progress'
    
    spider_name = Column(String(64), nullable=False, unique=True, comment="爬虫名称 (唯一键)")
    run_id = Column(String(64), nullable=False, comment="当前运行ID")
    
    # 状态
    status = Column(String(32), default="running", comment="状态: running, paused, error, finished")
    pid = Column(Integer, nullable=True, comment="进程ID")
    
    # 进度数据
    total_tasks = Column(Integer, default=0, comment="总任务数 (如总页数/总关键词数)")
    completed_tasks = Column(Integer, default=0, comment="已完成任务数")
    progress_percent = Column(Float, default=0.0, comment="进度百分比")
    
    # 详情 (用于分层展示)
    current_stage = Column(String(64), comment="当前阶段 (如: 列表采集, 详情采集)")
    current_item = Column(String(255), comment="当前正在处理的项目描述")
    
    # 统计
    items_scraped = Column(Integer, default=0, comment="已采集数据量")
    requests_made = Column(Integer, default=0, comment="已发起请求")
    errors_count = Column(Integer, default=0, comment="错误数")
    
    # 最后更新时间
    last_updated = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # 扩展信息 (JSON)
    extra_info = Column(JSON, nullable=True, comment="其他扩展信息")
