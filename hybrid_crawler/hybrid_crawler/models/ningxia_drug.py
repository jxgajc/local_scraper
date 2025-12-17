from sqlalchemy import Column, String, Integer, JSON, DateTime, Float, Text, BigInteger
from . import BaseModel
import scrapy
import hashlib
import json
from datetime import datetime

class NingxiaDrugItem(scrapy.Item):
    """
    ningxia医保药品数据Item
    字段已根据提供的JSON数据进行对齐
    """
    # --- 药品订单信息 (对应新的JSON结构) ---
    orderDetailId = scrapy.Field()     # 订单明细ID
    orderId = scrapy.Field()           # 订单ID
    procurecatalogId = scrapy.Field()  # 采购目录ID
    goodsId = scrapy.Field()           # 商品ID
    goodsName = scrapy.Field()         # 商品名称
    medicinemodel = scrapy.Field()     # 药品剂型
    outlook = scrapy.Field()           # 规格
    factor = scrapy.Field()            # 因子
    minUnit = scrapy.Field()           # 最小单位
    unit = scrapy.Field()              # 单位
    productName = scrapy.Field()       # 产品名称
    companyIdTb = scrapy.Field()       # 生产企业ID
    companyNameTb = scrapy.Field()     # 生产企业名称
    hospitalId = scrapy.Field()        # 医院ID
    hospitalName = scrapy.Field()      # 医院名称
    orderName = scrapy.Field()         # 订单名称
    submitTime = scrapy.Field()        # 提交时间
    companyIdPs = scrapy.Field()       # 配送企业ID
    companyNamePs = scrapy.Field()     # 配送企业名称
    orderdetailState = scrapy.Field()  # 订单明细状态
    areaId = scrapy.Field()            # 区域ID
    
    # --- 系统通用字段 ---
    md5_id = scrapy.Field()
    collect_time = scrapy.Field()
    page_num = scrapy.Field()
    url = scrapy.Field()
    url_hash = scrapy.Field()
    
    def generate_md5_id(self):
        """
        根据核心业务字段生成MD5唯一标识
        使用订单明细的核心字段确保唯一性
        """
        # 参与签名的字段（使用订单明细的核心字段确保唯一性）
        sign_fields = ['orderDetailId', 'orderId', 'procurecatalogId', 'goodsId', 'productName', 'companyNameTb', 'hospitalId']
        
        field_values = {}
        for field in sign_fields:
            field_values[field] = str(self.get(field, ''))
        
        # 排序并拼接
        sorted_json = json.dumps(field_values, sort_keys=True, ensure_ascii=False)
        
        md5_hash = hashlib.md5(sorted_json.encode('utf-8')).hexdigest()
        
        self['md5_id'] = md5_hash
        self['collect_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return md5_hash


class NingxiaDrug(BaseModel):
    """
    SQLAlchemy 模型定义
    """
    __tablename__ = 'ningxia_drug'
    
    # 自增主键
    id = Column(Integer, primary_key=True, autoincrement=True, comment="自增主键")
    
    # 业务唯一ID
    md5_id = Column(String(32), unique=True, index=True, comment="MD5唯一标识")
    
    # --- 药品订单信息列 ---
    orderDetailId = Column(String(64), comment="订单明细ID")
    orderId = Column(String(64), index=True, comment="订单ID")
    procurecatalogId = Column(Integer, comment="采购目录ID")
    goodsId = Column(Integer, comment="商品ID")
    goodsName = Column(String(256), comment="商品名称")
    medicinemodel = Column(String(128), comment="药品剂型")
    outlook = Column(String(256), comment="规格")
    factor = Column(Float, comment="因子")
    minUnit = Column(String(32), comment="最小单位")
    unit = Column(String(32), comment="单位")
    productName = Column(String(256), comment="产品名称")
    companyIdTb = Column(String(64), comment="生产企业ID")
    companyNameTb = Column(String(256), comment="生产企业名称")
    hospitalId = Column(String(64), index=True, comment="医院ID")
    hospitalName = Column(String(256), comment="医院名称")
    orderName = Column(String(512), comment="订单名称")
    submitTime = Column(BigInteger, comment="提交时间戳")
    companyIdPs = Column(String(64), comment="配送企业ID")
    companyNamePs = Column(String(256), comment="配送企业名称")
    orderdetailState = Column(Integer, comment="订单明细状态")
    areaId = Column(String(32), comment="区域ID")
    
    # --- 采集系统字段 ---
    collect_time = Column(DateTime, default=datetime.now, comment="采集时间")
    page_num = Column(Integer, comment="采集页码")
    url = Column(String(1024), comment="来源URL")
    url_hash = Column(String(64), index=True, comment="URL哈希")