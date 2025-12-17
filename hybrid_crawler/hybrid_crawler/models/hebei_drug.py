from sqlalchemy import Column, String, Integer, JSON, DateTime, Float, Text
from . import BaseModel
import scrapy
import hashlib
import json
from datetime import datetime

class HebeiDrugItem(scrapy.Item):
    """
    hebei医保药品数据Item
    字段已根据提供的JSON数据进行对齐
    """
    # --- 药品基础信息 (对应 JSON 片段 1) ---
    prodId = scrapy.Field()          # 产品ID (如: 1997931108578770945)
    prodCode = scrapy.Field()        # 产品编码 (如: XL01XGK141B001010101444)
    prodName = scrapy.Field()        # 药品名称 (如: 注射用卡非佐米)
    dosform = scrapy.Field()         # 剂型 (如: 注射剂)
    prodSpec = scrapy.Field()        # 规格 (如: 60mg)
    prodPac = scrapy.Field()         # 包装 (如: 60mg×1瓶/盒)
    prodentpCode = scrapy.Field()    # 生产企业代码
    prodentpName = scrapy.Field()    # 生产企业名称 (如: 江苏豪森药业集团有限公司)
    pubonlnPric = scrapy.Field()     # 挂网价格 (如: 1429.59)
    isMedicare = scrapy.Field()      # 是否医保 (如: 是)
    
    # --- 医院采购信息 (对应 JSON 片段 2) ---
    # 这里将存储包含 hospital_name, shpCnt, time 等信息的列表
    hospital_purchases = scrapy.Field() 
    
    # --- 系统通用字段 ---
    md5_id = scrapy.Field()
    collect_time = scrapy.Field()
    page_num = scrapy.Field()
    url = scrapy.Field()
    url_hash = scrapy.Field()
    
    def generate_md5_id(self):
        """
        根据核心业务字段生成MD5唯一标识
        注意：通常我们只用"药品基础信息"生成指纹，因为采购信息可能会动态增加
        """
        # 参与签名的字段（排除采集时间、采购列表等动态字段，确保同一药品的ID稳定）
        sign_fields = ['prodId', 'prodCode', 'prodName', 'prodSpec', 'prodentpName']
        
        field_values = {}
        for field in sign_fields:
            field_values[field] = str(self.get(field, ''))
        
        # 排序并拼接
        sorted_json = json.dumps(field_values, sort_keys=True, ensure_ascii=False)
        
        md5_hash = hashlib.md5(sorted_json.encode('utf-8')).hexdigest()
        
        self['md5_id'] = md5_hash
        self['collect_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return md5_hash


class HebeiDrug(BaseModel):
    """
    SQLAlchemy 模型定义
    """
    __tablename__ = 'hebei_drug'
    
    # 自增主键
    id = Column(Integer, primary_key=True, autoincrement=True, comment="自增主键")
    
    # 业务唯一ID
    md5_id = Column(String(32), unique=True, index=True, comment="MD5唯一标识")
    
    # --- 药品基础信息列 ---
    prodId = Column(String(64), comment="产品系统ID")
    prodCode = Column(String(128), index=True, comment="产品流水号/编码")
    prodName = Column(String(256), comment="药品名称")
    dosform = Column(String(128), comment="剂型")
    prodSpec = Column(String(256), comment="规格")
    prodPac = Column(String(256), comment="包装")
    
    # 企业信息
    prodentpCode = Column(String(128), comment="生产企业代码")
    prodentpName = Column(String(256), comment="生产企业名称")
    
    # 价格与医保属性
    # 使用 Float 或 DECIMAL 存储价格，保留2位小数
    pubonlnPric = Column(Float(precision=2), comment="挂网价格") 
    isMedicare = Column(String(32), comment="是否医保")
    
    # --- 医院采购列表 ---
    # 存储结构示例:
    # [
    #   {
    #     "isPublicHospitals": "是",
    #     "prodEntpName": "怀来县精神病专科医院",  <-- 注意：在JSON片段2中，这个key代表医院
    #     "prodEntpAdmdvs": "河北省>张家口市>怀来县",
    #     "shpCnt": 300,
    #     "shpTimeFormat": "2023-03-14"
    #   }
    # ]
    hospital_purchases = Column(JSON, comment="医院采购明细列表")
    
    # --- 采集系统字段 ---
    collect_time = Column(DateTime, default=datetime.now, comment="采集时间")
    page_num = Column(Integer, comment="采集页码")
    url = Column(String(1024), comment="来源URL")
    url_hash = Column(String(64), index=True, comment="URL哈希")