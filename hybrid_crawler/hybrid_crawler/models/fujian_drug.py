import scrapy
import hashlib
from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, Text, Boolean
from . import BaseModel

class FujianDrugItem(scrapy.Item):
    """
    福建省药品挂网及医院采购数据 Item
    对应接口: item-cfg-info/list (药品列表) + queryHospital (医院列表)
    """
    # --- 核心标识 ---
    ext_code = scrapy.Field()           # extCode (产品ID, 关联键)
    drug_list_code = scrapy.Field()     # druglistCode (目录编码)
    
    # --- 药品基础信息 ---
    drug_name = scrapy.Field()          # drugName (药品名称)
    drug_list_name = scrapy.Field()     # druglistName (通用名/目录名)
    dosform = scrapy.Field()            # dosformName (剂型)
    spec = scrapy.Field()               # specName (规格)
    pac = scrapy.Field()                # pac (包装)
    rute_name = scrapy.Field()          # ruteName (给药途径)
    
    # --- 企业信息 ---
    prod_entp = scrapy.Field()          # prodentpName (生产企业)
    
    # --- 医院采购信息 (扁平化) ---
    has_hospital_record = scrapy.Field() # 是否有医院记录 (Boolean)
    hospital_name = scrapy.Field()       # hospitalName (医院名称)
    medins_code = scrapy.Field()         # medinsCode (医院编码)
    area_name = scrapy.Field()           # areaName (地区)
    area_code = scrapy.Field()           # areaCode (地区编码)
    
    # --- 系统字段 ---
    source_data = scrapy.Field()        # 原始完整JSON
    md5_id = scrapy.Field()             # 唯一标识
    collect_time = scrapy.Field()       # 采集时间

    def generate_md5_id(self):
        """
        生成规则: 产品ID + 医院编码
        """
        # 如果没有医院记录，使用ext_code + None 防止重复
        key_part_2 = self.get('medins_code', 'NO_HOSPITAL')
        sign_str = f"{self.get('ext_code')}|{key_part_2}"
        
        self['md5_id'] = hashlib.md5(sign_str.encode('utf-8')).hexdigest()
        self['collect_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def get_model_class(self):
        return FujianDrug


class FujianDrug(BaseModel):
    """
    福建省药品挂网及采购数据表
    """
    __tablename__ = 'drug_hospital_fujian_test'

    id = Column(Integer, primary_key=True, autoincrement=True)
    md5_id = Column(String(32), index=True)#, unique=True
    
    # 药品信息
    ext_code = Column(String(64), index=True, comment="产品ID")
    drug_list_code = Column(String(64), comment="目录编码")
    drug_name = Column(String(256), index=True, comment="药品名称")
    drug_list_name = Column(String(256), comment="通用名")
    dosform = Column(String(128), comment="剂型")
    spec = Column(String(512), comment="规格")
    pac = Column(String(512), comment="包装")
    rute_name = Column(String(64), comment="给药途径")
    prod_entp = Column(String(256), comment="生产企业")
    
    # 医院信息
    has_hospital_record = Column(Boolean, default=False, comment="是否有采购医院")
    hospital_name = Column(String(256), comment="医院名称")
    medins_code = Column(String(64), index=True, comment="医院编码")
    area_name = Column(String(64), comment="地区")
    area_code = Column(String(32), comment="地区编码")
    
    source_data = Column(Text, comment="原始JSON")
    collect_time = Column(DateTime, default=datetime.now)