import scrapy
import hashlib
from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, Text, Boolean
from . import BaseModel  # 假设您有这个基础类，如果没有可以使用 sqlalchemy.ext.declarative.declarative_base()

class TianjinDrugItem(scrapy.Item):
    """
    天津市药品数据 Item
    对应接口: guideGetMedList (药品列表) + guideGetHosp (医院列表)
    """
    # --- 核心标识 ---
    med_id = scrapy.Field()             # medid (药品ID)
    
    # --- 药品基础信息 ---
    gen_name = scrapy.Field()           # genname (通用名)
    prod_name = scrapy.Field()          # prodname (商品名/别名)
    dosform = scrapy.Field()            # dosform (剂型)
    spec = scrapy.Field()               # spec (规格)
    pac = scrapy.Field()                # pac (包装规格)
    
    # --- 企业信息 ---
    prod_entp = scrapy.Field()          # prodentp (生产企业)
    
    # --- 价格与单位 ---
    conv_rat = scrapy.Field()           # convrat (转换比)
    min_sal_unt = scrapy.Field()        # minSalunt (最小销售单位)
    
    # --- 注册信息 ---
    aprv_no = scrapy.Field()            # aprvno (批准文号)
    
    # --- 医院采购信息 (扁平化) ---
    has_hospital_record = scrapy.Field() # 是否有医院记录
    hs_name = scrapy.Field()            # hsname (医院名称)
    hs_lav = scrapy.Field()             # hslav (医院等级)
    got_time = scrapy.Field()           # gottime (采购/获取时间)
    
    # --- 系统字段 ---
    source_data = scrapy.Field()        # 原始完整JSON
    md5_id = scrapy.Field()             # 唯一标识
    collect_time = scrapy.Field()       # 采集时间

    def generate_md5_id(self):
        """
        生成规则: 药品ID + 医院名称 (如果存在)
        """
        sign_str = f"{self.get('med_id')}|{self.get('hs_name')}"
        self['md5_id'] = hashlib.md5(sign_str.encode('utf-8')).hexdigest()
        self['collect_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

class TianjinDrug(BaseModel):
    """
    天津市药品挂网及采购数据表
    """
    __tablename__ = 'drug_hospital_tianjin_test'

    id = Column(Integer, primary_key=True, autoincrement=True)
    md5_id = Column(String(32), index=True)
    
    # 药品信息
    med_id = Column(String(64), index=True, comment="药品ID")
    gen_name = Column(String(256), index=True, comment="通用名")
    prod_name = Column(String(256), comment="商品名")
    dosform = Column(String(128), comment="剂型")
    spec = Column(String(512), comment="规格")
    pac = Column(String(512), comment="包装")
    prod_entp = Column(String(512), comment="生产企业")
    aprv_no = Column(String(128), comment="批准文号")
    
    conv_rat = Column(String(32), comment="转换比")
    min_sal_unt = Column(String(32), comment="最小销售单位")
    
    # 医院信息
    has_hospital_record = Column(Boolean, default=False, comment="是否有采购医院")
    hs_name = Column(String(256), comment="医院名称")
    hs_lav = Column(String(64), comment="医院等级")
    got_time = Column(String(32), comment="获取时间")
    
    source_data = Column(Text, comment="原始JSON")
    collect_time = Column(DateTime, default=datetime.now)