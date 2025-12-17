import scrapy
import hashlib
import json
from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, Text
from . import BaseModel
from datetime import datetime


class ShandongDrugItem(scrapy.Item):
    """
    山东省药品数据 Item
    结构已扁平化：每一条Item代表一个医院对某一种药品的采购记录
    """
    # --- 药品基础字段 ---
    prodCode = scrapy.Field()       # 产品代码
    prodName = scrapy.Field()       # 产品名称
    prodentpName = scrapy.Field()   # 申报企业/配送企业
    spec = scrapy.Field()           # 规格
    pac = scrapy.Field()            # 包装
    price = scrapy.Field()          # 挂网价格
    aprvno = scrapy.Field()         # 批准文号
    manufacture_name = scrapy.Field() # 上市许可持有人
    public_time = scrapy.Field()    # 挂网时间
    
    # --- 医院采购信息 (扁平化字段) ---
    hospitalName = scrapy.Field()           # 医院名称
    hospitalId = scrapy.Field()             # 医院ID
    cityName = scrapy.Field()               # 城市名称
    cotyName = scrapy.Field()               # 区县名称
    admdvsName = scrapy.Field()             # 行政区划名称
    drugPurchasePropertyStr = scrapy.Field() # 药品采购属性 (是/否)
    userName = scrapy.Field()               # 医院账号/编码
    admdvs = scrapy.Field()                 # 行政区划代码

    # --- 系统字段 ---
    source_data = scrapy.Field()    # 原始数据备份 (药品基础信息)
    md5_id = scrapy.Field()         # 唯一标识
    collect_time = scrapy.Field()   # 采集时间

    def generate_md5_id(self):
        """生成MD5唯一标识"""
        p_code = str(self.get('prodCode', ''))
        aprv = str(self.get('aprvno', ''))
        prc = str(self.get('price', ''))
        hosp_id = str(self.get('hospitalId', ''))
        user_name = str(self.get('userName', ''))
        
        # 唯一键组合：药品代码 + 批号 + 价格 + 医院ID + 医院账号
        # 这样可以区分同一个药品在不同医院的记录
        sign_str = f"{p_code}_{aprv}_{prc}_{hosp_id}_{user_name}"
        
        self['md5_id'] = hashlib.md5(sign_str.encode('utf-8')).hexdigest()
        self['collect_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


class ShandongDrug(BaseModel):
    """
    山东省药品挂网及医院采购数据模型
    对应 ShandongDrugItem 的扁平化结构
    """
    __tablename__ = 'shandong_drug'

    # --- 基础主键 ---
    id = Column(Integer, primary_key=True, autoincrement=True, comment="自增主键")
    
    # --- 业务唯一标识 ---
    # 对应 Item.generate_md5_id() 生成的 MD5
    md5_id = Column(String(32), index=True, comment="MD5唯一标识")#, unique=True

    # --- 药品基础字段 ---
    prodCode = Column(String(64), index=True, comment="产品代码")
    prodName = Column(String(256), index=True, comment="产品名称")
    prodentpName = Column(String(256), comment="申报企业/配送企业")
    spec = Column(String(256), comment="规格")
    pac = Column(String(256), comment="包装")
    price = Column(String(32), comment="挂网价格")  # 存储字符串形式的价格，如 "12.98"
    aprvno = Column(String(128), comment="批准文号")
    manufacture_name = Column(String(256), comment="上市许可持有人")
    public_time = Column(String(32), comment="挂网时间/操作时间")

    # --- 医院采购信息 ---
    hospitalName = Column(String(256), comment="医院名称")
    hospitalId = Column(String(64), index=True, comment="医院ID")
    cityName = Column(String(64), comment="城市名称")     # 例如：济南市
    cotyName = Column(String(64), comment="区县名称")     # 例如：历下区
    admdvsName = Column(String(128), comment="行政区划名称") # 例如：济南市-历下区
    drugPurchasePropertyStr = Column(String(16), comment="药品采购属性(是/否)")
    userName = Column(String(64), comment="医院账号/编码")
    admdvs = Column(String(64), comment="行政区划代码")

    # --- 系统通用字段 ---
    source_data = Column(Text, comment="原始数据备份(JSON字符串)")
    collect_time = Column(DateTime, default=datetime.now, comment="采集时间")