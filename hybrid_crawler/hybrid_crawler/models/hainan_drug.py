import scrapy
import hashlib
from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, Text, Float, Boolean
from . import BaseModel
from .mixins import BizFingerprintMixin

class HainanDrugItem(BizFingerprintMixin, scrapy.Item):
    """
    海南省药品及药店库存数据 Item
    对应接口: getDrugStore (药品列表) + getDrugStoreDetl (药店详情)
    """
    # --- 核心标识 ---
    drug_code = scrapy.Field()          # prodCode (药品编码)
    
    # --- 药品基础信息 ---
    prod_name = scrapy.Field()          # prodName (通用名)
    dosform = scrapy.Field()            # dosform (剂型)
    spec = scrapy.Field()               # prodSpec (规格)
    pac = scrapy.Field()                # prodPac (包装)
    conv_rat = scrapy.Field()           # convrat (转换比)
    
    # --- 企业信息 ---
    prod_entp = scrapy.Field()          # prodentpName (生产企业)
    dcla_entp = scrapy.Field()          # dclaEntpName (申报企业)
    aprv_no = scrapy.Field()            # aprvno (批准文号)
    
    # --- 药店/医院库存信息 (扁平化) ---
    has_shop_record = scrapy.Field()    # 是否有药店记录 (Boolean)
    shop_name = scrapy.Field()          # medinsName (机构名称)
    shop_code = scrapy.Field()          # medinsCode (机构编码)
    shop_type_memo = scrapy.Field()     # memo (机构类型备注)
    
    # --- 价格与库存 ---
    price = scrapy.Field()              # pric (价格)
    inventory = scrapy.Field()          # invCnt (库存数量)
    update_time = scrapy.Field()        # invChgTime (库存更新时间)
    
    hilist_name = scrapy.Field()        # fixmedinsHilistName (医保目录名称)
    
    # --- 系统字段 ---
    source_data = scrapy.Field()        # 原始完整JSON
    md5_id = scrapy.Field()             # 唯一标识
    collect_time = scrapy.Field()       # 采集时间

    def generate_md5_id(self):
        """
        生成规则: 使用统一业务指纹
        """
        mapping = {
            'HospitalName': 'shop_name',
            'ProductName': 'prod_name',
            'MedicineModelName': 'dosform',
            'Outlookc': 'spec',
            'Pack': 'pac',
            'Manufacturer': 'prod_entp'
        }
        self.generate_biz_id(field_mapping=mapping)
        self['collect_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def get_model_class(self):
        return HainanDrug


class HainanDrug(BaseModel):
    """
    海南省药品库存查询表
    """
    __tablename__ = 'drug_shop_hainan_test'

    id = Column(Integer, primary_key=True, autoincrement=True)
    md5_id = Column(String(32), index=True)
    
    # 药品信息
    drug_code = Column(String(64), index=True, comment="药品编码")
    prod_name = Column(String(256), index=True, comment="通用名")
    dosform = Column(String(128), comment="剂型")
    spec = Column(String(256), comment="规格")
    pac = Column(String(256), comment="包装")
    conv_rat = Column(String(32), comment="转换比")
    
    prod_entp = Column(String(256), comment="生产企业")
    dcla_entp = Column(String(256), comment="申报企业")
    aprv_no = Column(String(128), comment="批准文号")
    
    # 药店/医院信息
    has_shop_record = Column(Boolean, default=False, comment="是否有药店记录")
    shop_name = Column(String(256), comment="机构名称")
    shop_code = Column(String(64), index=True, comment="机构编码")
    shop_type_memo = Column(String(256), comment="机构类型备注")
    
    price = Column(String(32), comment="价格")
    inventory = Column(String(32), comment="库存数量")
    update_time = Column(String(32), comment="更新时间")
    hilist_name = Column(String(128), comment="医保名称")
    
    source_data = Column(Text, comment="原始JSON")
    collect_time = Column(DateTime, default=datetime.now)