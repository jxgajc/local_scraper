from sqlalchemy import Column, String, Integer, DateTime, Boolean, JSON, Text
from sqlalchemy.sql import func
from . import BaseModel

class CrawlStatus(BaseModel):
    """
    爬虫采集状态表，用于记录每个爬虫的采集过程和参数
    用于数据完整性验证和遗漏分析
    """
    __tablename__ = 'crawl_status'
    
    spider_name = Column(String(64), nullable=False, index=True, comment="爬虫名称")
    crawl_id = Column(String(64), nullable=False, index=True, comment="本次采集唯一标识")
    stage = Column(String(32), nullable=False, comment="采集阶段: start_requests, list_page, detail_page, etc.")
    
    # 页码相关信息
    page_no = Column(Integer, default=1, comment="当前页码")
    total_pages = Column(Integer, default=0, comment="总页数")
    page_size = Column(Integer, default=0, comment="每页大小")
    
    # 数据统计
    items_found = Column(Integer, default=0, comment="本页/本阶段发现的数据项数量")
    items_stored = Column(Integer, default=0, comment="本页/本阶段成功存储的数据项数量")
    
    # 采集参数
    params = Column(JSON, nullable=True, comment="采集请求参数")
    api_url = Column(String(768), nullable=True, comment="请求的API地址")
    
    # 状态信息
    success = Column(Boolean, default=True, comment="是否成功")
    error_message = Column(Text, nullable=True, comment="错误信息")
    
    # 时间信息
    start_time = Column(DateTime(timezone=True), server_default=func.now(), comment="开始时间")
    end_time = Column(DateTime(timezone=True), onupdate=func.now(), comment="结束时间")
    
    # 关联信息
    parent_crawl_id = Column(String(64), nullable=True, index=True, comment="父级采集ID，用于关联嵌套请求")
    reference_id = Column(String(64), nullable=True, index=True, comment="引用ID，如药品ID、医院ID等")
