import scrapy
import hashlib
import json
from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, Text, Float
from . import BaseModel
from .mixins import BizFingerprintMixin

class GuangdongDrugItem(BizFingerprintMixin, scrapy.Item):
    """
    广东省药品数据 Item (信息无损版)
    对应接口: queryPubonlnPage (药品基础) + getPurcHospitalInfoListNew (医院采购)
    """
    # --- 核心标识 ---
    drug_id = scrapy.Field()            # drugId (平台内部ID)
    drug_code = scrapy.Field()          # drugCode (药品流水号/编码)
    
    # --- 药品基础信息 ---
    gw_active = scrapy.Field()          # gwActive (活跃区状态)
    gen_name = scrapy.Field()           # genname (通用名)
    trade_name = scrapy.Field()         # tradeName (商品名)
    dosform_name = scrapy.Field()       # dosformName (剂型)
    spec_name = scrapy.Field()          # specName (规格)
    pac_matl = scrapy.Field()           # pacmatl (包装材质)
    
    # --- 企业信息 ---
    listing_holder = scrapy.Field()     # listingLicenseHolder (上市许可持有人)
    prod_entp_name = scrapy.Field()     # prodentpName (生产企业)
    dcla_entp_name = scrapy.Field()     # dclaEntpName (申报企业)
    dcla_entp_uscc = scrapy.Field()     # dclaEntpUscc (申报企业信用代码)
    
    # --- 价格与包装 ---
    price = scrapy.Field()              # minPacPubonlnPric (最小包装挂网价)
    min_unit = scrapy.Field()           # minuntName (最小制剂单位)
    min_pac_name = scrapy.Field()       # minpacName (最小包装单位)
    conv_rat = scrapy.Field()           # convrat (转换比)
    
    # --- 注册与批号 ---
    aprv_no = scrapy.Field()            # aprvno (批准文号)
    reg_dosform_name = scrapy.Field()   # regDosformName (注册剂型)
    reg_spec_name = scrapy.Field()      # regSpecName (注册规格)
    
    # --- 医保与政策属性 ---
    quality_lv = scrapy.Field()         # qualityLv (质量层次)
    jyl_category = scrapy.Field()       # jylCategory (甲乙类)
    jyl_no = scrapy.Field()             # jylNo (交易类编号)
    policy_att = scrapy.Field()         # policyAtt (政策属性, 如: 非集采药品)
    drug_select_type = scrapy.Field()   # drugSelectType (选药类型)
    formation_mode = scrapy.Field()     # formationMode (形成方式代码)
    
    # --- 状态标记 ---
    pubonln_time = scrapy.Field()       # pubonlnTime (挂网时间)
    erm_flag = scrapy.Field()           # ermFlag
    zc_spt_id = scrapy.Field()          # zcSptId
    exist_price_flag = scrapy.Field()   # existPubonlnPric
    stop_flag = scrapy.Field()          # stopPubonln
    
    # --- 医院采购信息 (扁平化合并) ---
    has_hospital_record = scrapy.Field() # 是否有医院记录 (Boolean)
    medins_code = scrapy.Field()        # medinsCode (医院编码)
    medins_name = scrapy.Field()        # medinsName (医院名称)
    hosp_type = scrapy.Field()          # type (医院类型, 如: 民营)
    admdvs_name = scrapy.Field()        # admdvsName (行政区划全称)
    city_name = scrapy.Field()          # (解析自 admdvsName) 城市
    area_name = scrapy.Field()          # (解析自 admdvsName) 区县
    source_id = scrapy.Field()          # sourceId

    # --- 系统字段 ---
    url = scrapy.Field()
    url_hash = scrapy.Field()
    source_data = scrapy.Field()        # 原始完整JSON备份
    md5_id = scrapy.Field()             # 唯一标识
    collect_time = scrapy.Field()       # 采集时间

    def generate_md5_id(self):
        """
        生成规则: 使用统一业务指纹
        """
        mapping = {
            'HospitalName': 'medins_name',
            'ProductName': 'gen_name',
            'MedicineModelName': 'dosform_name',
            'Outlookc': 'spec_name',
            'Pack': 'min_pac_name',
            'Manufacturer': 'prod_entp_name'
        }
        self.generate_biz_id(field_mapping=mapping)
        self['collect_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def get_model_class(self):
        return GuangdongDrug


class GuangdongDrug(BaseModel):
    """
    广东省药品挂网数据表
    """
    __tablename__ = 'drug_hospital_guangdong_test'

    id = Column(Integer, primary_key=True, autoincrement=True)
    md5_id = Column(String(32), index=True)#, unique=True
    
    # 核心字段
    drug_id = Column(Integer, comment="平台DrugID")
    drug_code = Column(String(64), index=True, comment="药品编码")
    gw_active = Column(String(32), comment="活跃区")
    
    # 基础信息
    gen_name = Column(String(256), index=True, comment="通用名")
    trade_name = Column(String(256), comment="商品名")
    dosform_name = Column(String(128), comment="剂型")
    spec_name = Column(String(512), comment="规格")
    pac_matl = Column(String(256), comment="包装材质")
    
    # 企业
    listing_holder = Column(String(256), comment="上市许可持有人")
    prod_entp_name = Column(String(256), comment="生产企业")
    dcla_entp_name = Column(String(256), comment="申报企业")
    dcla_entp_uscc = Column(String(64), comment="申报企业信用代码")
    
    # 价格
    price = Column(String(32), comment="挂网价格")
    min_unit = Column(String(32), comment="最小制剂单位")
    min_pac_name = Column(String(32), comment="最小包装单位")
    conv_rat = Column(String(32), comment="转换比")
    
    # 注册
    aprv_no = Column(String(128), comment="批准文号")
    reg_dosform_name = Column(String(128), comment="注册剂型")
    reg_spec_name = Column(String(512), comment="注册规格")
    
    # 属性
    quality_lv = Column(String(64), comment="质量层次")
    jyl_category = Column(String(32), comment="甲乙类")
    policy_att = Column(String(128), comment="政策属性")
    drug_select_type = Column(String(64), comment="选药类型")
    
    # 时间状态
    pubonln_time = Column(String(32), comment="挂网时间")
    
    # 医院信息
    has_hospital_record = Column(Integer, default=0, comment="是否有采购医院")
    medins_code = Column(String(64), index=True, comment="医院编码")
    medins_name = Column(String(256), comment="医院名称")
    hosp_type = Column(String(64), comment="医院类型")
    admdvs_name = Column(String(256), comment="行政区划全称")
    city_name = Column(String(64), comment="城市")
    area_name = Column(String(64), comment="区县")
    
    source_data = Column(Text, comment="原始JSON")
    collect_time = Column(DateTime, default=datetime.now)
    url = Column(String(256), comment="url")
    url_hash = Column(String(64),index=True, comment="url信息")