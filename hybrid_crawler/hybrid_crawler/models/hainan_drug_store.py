from sqlalchemy import Column, String, Text, Integer, JSON
from . import BaseModel

class HainanDrugStore(BaseModel):
    __tablename__ = 'hainan_drug_store'
    
    # 以下字段将根据第一次请求的返回结果动态调整
    # 初始定义一些可能的字段，后续会根据实际返回结果进行调整
    store_name = Column(String(255), nullable=True, comment="药店名称")
    address = Column(String(512), nullable=True, comment="地址")
    contact = Column(String(128), nullable=True, comment="联系方式")
    business_license = Column(String(255), nullable=True, comment="营业执照")
    legal_person = Column(String(128), nullable=True, comment="法定代表人")
    store_type = Column(String(128), nullable=True, comment="药店类型")
    status = Column(String(64), nullable=True, comment="状态")
    area = Column(String(128), nullable=True, comment="所属区域")
    meta_info = Column(JSON, nullable=True, comment="存储额外的JSON元数据")