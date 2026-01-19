from sqlalchemy import Column, String, Text, Integer, JSON, DateTime
from . import BaseModel
from .mixins import BizFingerprintMixin
import scrapy
import hashlib
import json
from datetime import datetime


class LiaoningDrugItem(BizFingerprintMixin, scrapy.Item):
    """
    辽宁药店数据Item
    """
    # 药店信息字段
    id = scrapy.Field()
    md5_id = scrapy.Field()
    SubmiTime =scrapy.Field()
    ProductName = scrapy.Field()
    MedicineModelName = scrapy.Field()
    POS = scrapy.Field()
    Outlookc = scrapy.Field()
    HospitalName = scrapy.Field()
    Pack = scrapy.Field()
    GoodsName = scrapy.Field()

    # 采集时间和页码
    collect_time = scrapy.Field()
    page_num = scrapy.Field()
    
    # 其他必要字段
    url = scrapy.Field()
    url_hash = scrapy.Field()
    
    
    def generate_md5_id(self):
        """
        根据业务字段生成MD5唯一标识
        """
        # 调用 Mixin 中的通用方法
        self.generate_biz_id()
        
        # 设置采集时间 (保持原有逻辑)
        self['collect_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return self['md5_id']

    def get_model_class(self):
        return LiaoningDrug


class LiaoningDrug(BaseModel):
    __tablename__ = 'drug_hospital_liaoning_test'
    
    # 自增主键
    id = Column(Integer, primary_key=True, autoincrement=True, comment="自增主键")
    
    # MD5唯一ID（对所有采集信息进行MD5计算）
    md5_id = Column(String(32), comment="MD5唯一标识")
    
    
    # 药品信息字段
    SubmiTime = Column(String(128), comment="提交时间")
    ProductName = Column(String(255), nullable=True, comment="药品名称")
    MedicineModelName = Column(String(128), nullable=True, comment="药品剂型名称")
    POS = Column(Integer, nullable=True, comment="POS")
    Outlookc = Column(String(512), nullable=True, comment="规格")
    HospitalName = Column(String(512), nullable=True, comment="医院名称")
    Pack = Column(String(512), nullable=True, comment="包装规格")
    GoodsName = Column(String(255), nullable=True, comment="商品名称")
    # 采集时间和页码
    collect_time = Column(DateTime, comment="采集时间")
    page_num = Column(Integer, comment="采集页码")
    
    # 其他必要字段
    url = Column(String(1024), comment="来源URL")
    url_hash = Column(String(64), index=True, comment="URL哈希")
