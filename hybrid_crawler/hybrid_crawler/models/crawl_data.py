from sqlalchemy import Column, String, Text, Integer, JSON
from . import BaseModel

class CrawlData(BaseModel):
    __tablename__ = 'crawl_data'
    
    url = Column(String(768), nullable=False, index=True, comment="URL")
    url_hash = Column(String(64), index=True, comment="URL哈希")
    title = Column(String(512), nullable=True)
    content = Column(Text, nullable=True)
    meta_info = Column(JSON, nullable=True, comment="存储额外的JSON元数据")
    status_code = Column(Integer, default=200)
    source = Column(String(64), index=True, comment="数据来源标识")