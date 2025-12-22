from sqlalchemy import Column, String, Text, Integer, JSON, DateTime
from . import BaseModel
import scrapy
import hashlib
import json
from datetime import datetime

class NhsaDrugItem(scrapy.Item):
    # 用户提供的JSON结构字段
    businessLicense = scrapy.Field()
    productcode = scrapy.Field()
    materialname = scrapy.Field()
    usageDosage = scrapy.Field()
    drugValidityDate = scrapy.Field()
    goodsname = scrapy.Field()
    baseId = scrapy.Field()
    goodsstandardcode = scrapy.Field()
    nameOrLhoder = scrapy.Field()
    productmedicinemodel = scrapy.Field()
    approvalcode = scrapy.Field()
    isChildDrugs = scrapy.Field()
    registeredmedicinemodel = scrapy.Field()
    productname = scrapy.Field()
    factor = scrapy.Field()
    productinsurancetype = scrapy.Field()
    goodscode = scrapy.Field()
    isOtc = scrapy.Field()
    registeredoutlook = scrapy.Field()
    companynamesc = scrapy.Field()
    subpackager = scrapy.Field()
    version = scrapy.Field()
    realitymedicinemodel = scrapy.Field()
    minunit = scrapy.Field()
    productremark = scrapy.Field()
    marketState = scrapy.Field()
    unit = scrapy.Field()
    registeredproductname = scrapy.Field()
    indication = scrapy.Field()
    realityoutlook = scrapy.Field()
    
    # 添加MD5唯一ID、采集时间和页码
    md5_id = scrapy.Field()
    collect_time = scrapy.Field()
    page_num = scrapy.Field()  # 采集页码
    
    # 其他必要字段
    url = scrapy.Field()
    url_hash = scrapy.Field()
    
    def generate_md5_id(self):
        """
        根据所有字段生成MD5唯一标识
        """
        # 获取所有字段值
        field_values = {}
        for field in self.fields:
            if field != 'md5_id' and field != 'collect_time':  # 排除MD5 ID和采集时间本身
                field_values[field] = self.get(field, '')
        
        # 将字段值转换为JSON字符串，排序键以确保一致性
        sorted_json = json.dumps(field_values, sort_keys=True, ensure_ascii=False)
        
        # 计算MD5哈希值
        md5_hash = hashlib.md5(sorted_json.encode('utf-8')).hexdigest()
        
        # 设置MD5 ID字段
        self['md5_id'] = md5_hash
        
        # 设置采集时间
        self['collect_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return md5_hash


class NhsaDrug(BaseModel):
    __tablename__ = 'nhsa_drug_test'
    
    # 自增主键
    id = Column(Integer, primary_key=True, autoincrement=True, comment="自增主键")
    
    # MD5唯一ID（对所有采集信息进行MD5计算）
    md5_id = Column(String(32), comment="MD5唯一标识")
    
    # 用户提供的JSON结构字段
    businessLicense = Column(String(256), comment="营业执照")
    productcode = Column(String(128), comment="产品代码")
    materialname = Column(String(128), comment="材质名称")
    usageDosage = Column(Text, comment="用法用量")
    drugValidityDate = Column(Text, comment="药品有效期")
    goodsname = Column(String(256), comment="商品名称")
    baseId = Column(String(128), comment="基础ID")
    goodsstandardcode = Column(String(128), comment="商品标准代码")
    nameOrLhoder = Column(String(512), comment="名称或持有人")
    productmedicinemodel = Column(String(128), comment="产品药品型号")
    approvalcode = Column(String(128), comment="批准文号")
    isChildDrugs = Column(Integer, comment="是否儿童用药")
    registeredmedicinemodel = Column(String(128), comment="注册药品型号")
    productname = Column(String(256), comment="产品名称")
    factor = Column(Integer, comment="因子")
    productinsurancetype = Column(String(128), comment="产品保险类型")
    goodscode = Column(String(128), index=True, comment="商品代码")
    isOtc = Column(Integer, comment="是否OTC")
    registeredoutlook = Column(String(1024), comment="注册外观")
    companynamesc = Column(String(512), comment="企业名称(中文)")
    subpackager = Column(String(512), comment="分包商")
    version = Column(String(32), comment="版本")
    realitymedicinemodel = Column(String(128), comment="实际药品型号")
    minunit = Column(String(32), comment="最小单位")
    productremark = Column(Text, comment="产品备注")
    marketState = Column(String(32), comment="市场状态")
    unit = Column(String(32), comment="单位")
    registeredproductname = Column(String(512), comment="注册产品名称")
    indication = Column(Text, comment="适应症")
    realityoutlook = Column(String(128), comment="实际外观")
    
    # 采集时间和页码
    collect_time = Column(DateTime, comment="采集时间")
    page_num = Column(Integer, comment="采集页码")
    
    # 其他必要字段
    url = Column(String(1024), comment="来源URL")
    url_hash = Column(String(64), index=True, comment="URL哈希")
