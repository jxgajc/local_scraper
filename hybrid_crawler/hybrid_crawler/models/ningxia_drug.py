from sqlalchemy import Column, String, Integer, JSON, DateTime, Float, Text, BigInteger
from . import BaseModel
import scrapy
import hashlib
import json
from datetime import datetime

class NingxiaDrugItem(scrapy.Item):
    """
    宁夏医保药品及医院数据Item
    对应 ningxia_drug_store.py 的两层采集结构
    """
    # --- 核心关联键 ---
    procurecatalogId = scrapy.Field()  # 采购目录ID (核心关联ID)
    
    # --- 药品基础信息 (来自第一层: getRecentPurchaseDetailData) ---
    orderDetailId = scrapy.Field()     # 订单明细ID
    orderId = scrapy.Field()           # 订单ID
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
    orderName = scrapy.Field()         # 订单名称
    submitTime = scrapy.Field()        # 提交时间
    companyIdPs = scrapy.Field()       # 配送企业ID
    companyNamePs = scrapy.Field()     # 配送企业名称
    orderdetailState = scrapy.Field()  # 订单明细状态
    areaId = scrapy.Field()            # 区域ID
    
    # --- 医院信息 (来自第二层: getDrugDetailDate) ---
    hospitalId = scrapy.Field()        # 医院ID (可能为空)
    hospitalName = scrapy.Field()      # 医院名称 (核心字段)
    areaName = scrapy.Field()          # 地区名称 (新增字段)
    
    # --- 系统通用字段 ---
    md5_id = scrapy.Field()
    collect_time = scrapy.Field()
    page_num = scrapy.Field()
    url = scrapy.Field()
    url_hash = scrapy.Field()
    
    def generate_md5_id(self):
        """
        生成唯一标识
        逻辑变更为: 采购目录ID (药品) + 医院名称
        确保同一药品在不同医院的分布是唯一的
        """
        # 核心去重字段
        procure_id = str(self.get('procurecatalogId', ''))
        hospital_name = str(self.get('hospitalName', ''))
        product_name = str(self.get('productName', ''))
        
        # 如果没有 hospitalName (仅有药品列表情况)，则退化为使用 procurecatalogId + orderId
        if not hospital_name:
            unique_str = f"{procure_id}_{self.get('orderDetailId', '')}"
        else:
            unique_str = f"{procure_id}_{hospital_name}_{product_name}"
            
        # 计算MD5哈希值
        md5_hash = hashlib.md5(unique_str.encode('utf-8')).hexdigest()
        
        self['md5_id'] = md5_hash
        self['collect_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return md5_hash


class NingxiaDrug(BaseModel):
    """
    SQLAlchemy 模型定义
    """
    __tablename__ = 'drug_hospital_ningxia'
    
    # 自增主键
    id = Column(Integer, primary_key=True, autoincrement=True, comment="自增主键")
    
    # 业务唯一ID
    md5_id = Column(String(32), index=True, comment="MD5唯一标识")#, unique=True
    
    # --- 核心ID ---
    procurecatalogId = Column(String(64), index=True, comment="采购目录ID")
    
    # --- 药品信息 ---
    goodsId = Column(Integer, comment="商品ID")
    goodsName = Column(String(256), comment="商品名称")
    medicinemodel = Column(String(128), comment="药品剂型")
    outlook = Column(String(256), comment="规格")
    factor = Column(Float, nullable=True, comment="因子")
    minUnit = Column(String(32), comment="最小单位")
    unit = Column(String(32), comment="单位")
    productName = Column(String(256), comment="产品名称")
    companyNameTb = Column(String(256), comment="生产企业名称")
    companyNamePs = Column(String(256), comment="配送企业名称")
    
    # --- 订单/原始信息 (可为空) ---
    orderDetailId = Column(String(64), nullable=True, comment="订单明细ID")
    orderId = Column(String(64), nullable=True, comment="订单ID")
    orderName = Column(String(512), nullable=True, comment="订单名称")
    submitTime = Column(BigInteger, nullable=True, comment="提交时间戳")
    orderdetailState = Column(Integer, nullable=True, comment="订单明细状态")
    
    # --- 医院/区域信息 ---
    hospitalId = Column(String(64), nullable=True, index=True, comment="医院ID")
    hospitalName = Column(String(256), nullable=True, comment="医院名称")
    areaId = Column(String(32), nullable=True, comment="区域ID")
    areaName = Column(String(128), nullable=True, comment="区域名称") # 新增
    
    # --- 采集系统字段 ---
    collect_time = Column(DateTime, default=datetime.now, comment="采集时间")
    page_num = Column(Integer, comment="采集页码")
    url = Column(String(1024), comment="来源URL")
    url_hash = Column(String(64), index=True, comment="URL哈希")